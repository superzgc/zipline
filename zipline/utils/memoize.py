"""
Tools for memoization of function results.
"""
from collections import OrderedDict
from functools import wraps
from weakref import WeakKeyDictionary, ref

from six.moves._thread import allocate_lock as Lock


class lazyval(object):
    """Decorator that marks that an attribute of an instance should not be
    computed until needed, and that the value should be memoized.

    Example
    -------

    >>> from zipline.utils.memoize import lazyval
    >>> class C(object):
    ...     def __init__(self):
    ...         self.count = 0
    ...     @lazyval
    ...     def val(self):
    ...         self.count += 1
    ...         return "val"
    ...
    >>> c = C()
    >>> c.count
    0
    >>> c.val, c.count
    ('val', 1)
    >>> c.val, c.count
    ('val', 1)
    >>> c.val = 'not_val'
    Traceback (most recent call last):
    ...
    AttributeError: Can't set read-only attribute.
    >>> c.val
    'val'
    """
    def __init__(self, get):
        self._get = get
        self._cache = WeakKeyDictionary()

    def __get__(self, instance, owner):
        if instance is None:
            return self
        try:
            return self._cache[instance]
        except KeyError:
            self._cache[instance] = val = self._get(instance)
            return val

    def __set__(self, instance, value):
        raise AttributeError("Can't set read-only attribute.")

    def __delitem__(self, instance):
        del self._cache[instance]


class classlazyval(lazyval):
    """ Decorator that marks that an attribute of a class should not be
    computed until needed, and that the value should be memoized.

    Example
    -------

    >>> from zipline.utils.memoize import classlazyval
    >>> class C(object):
    ...     count = 0
    ...     @classlazyval
    ...     def val(cls):
    ...         cls.count += 1
    ...         return "val"
    ...
    >>> C.count
    0
    >>> C.val, C.count
    ('val', 1)
    >>> C.val, C.count
    ('val', 1)
    """
    # We don't reassign the name on the class to implement the caching because
    # then we would need to use a metaclass to track the name of the
    # descriptor.
    def __get__(self, instance, owner):
        return super(classlazyval, self).__get__(owner, owner)


def weak_lru_cache(maxsize=100):
    """Least-recently-used cache decorator.

    If *maxsize* is set to None, the LRU features are disabled and the cache
    can grow without bound.

    Arguments to the cached function must be hashable.

    View the cache statistics named tuple (hits, misses, maxsize, currsize)
    with f.cache_info().  Clear the cache and statistics with f.cache_clear().
    Access the underlying function with f.__wrapped__.

    See:  http://en.wikipedia.org/wiki/Cache_algorithms#Least_Recently_Used

    """
    # Users should only access the lru_cache through its public API:
    #       cache_info, cache_clear, and f.__wrapped__
    # The internals of the lru_cache are encapsulated for thread safety and
    # to allow the implementation to change (including a possible C version).

    def decorating_function(
            user_function, tuple=tuple, sorted=sorted, len=len,
            KeyError=KeyError):

        hits, misses = [0], [0]
        kwd_mark = (object(),)    # separates positional and keyword args
        lock = Lock()             # needed because OrderedDict isn't threadsafe

        if maxsize is None:
            # simple cache without ordering or size limit
            cache = _WeakKeyDict()

            @wraps(user_function)
            def wrapper(*args, **kwds):
                key = args
                if kwds:
                    key += kwd_mark + tuple(sorted(kwds.items()))
                try:
                    result = cache[key]
                    hits[0] += 1
                    return result
                except KeyError:
                    pass
                result = user_function(*args, **kwds)
                cache[key] = result
                misses[0] += 1
                return result
        else:
            cache = _WeakOrderedDict()    # ordered least recent to most recent
            cache_popitem = cache.popitem
            cache_renew = cache.move_to_end

            @wraps(user_function)
            def wrapper(*args, **kwds):
                key = args
                if kwds:
                    key += kwd_mark + tuple(sorted(kwds.items()))
                with lock:
                    try:
                        result = cache[key]
                        cache_renew(key)    # record recent use of this key
                        hits[0] += 1
                        return result
                    except KeyError:
                        pass
                result = user_function(*args, **kwds)
                with lock:
                    cache[key] = result     # record recent use of this key
                    misses[0] += 1
                    if len(cache) > maxsize:
                        # purge least recently used cache entry
                        cache_popitem(False)
                return result

        def cache_info():
            """Report cache statistics"""
            with lock:
                return (hits[0], misses[0], maxsize, len(cache))

        def cache_clear():
            """Clear the cache and cache statistics"""
            with lock:
                cache.clear()
                hits[0] = misses[0] = 0

        wrapper.cache_info = cache_info
        wrapper.cache_clear = cache_clear
        return wrapper

    return decorating_function


class _WeakKey(list):
    def __init__(self, items, dict_remove=None):
        def remove(k, selfref=ref(self), dict_remove=dict_remove):
            self = selfref()
            if self is not None and dict_remove is not None:
                dict_remove(self)

        super(_WeakKey, self).__init__(tuple(self._try_ref(item, remove)
                                             for item in items))

    @staticmethod
    def _try_ref(item, callback):
        try:
            return ref(item, callback)
        except TypeError:
            return item

    def __hash__(self):
        try:
            return self.__hash
        except AttributeError:
            h = self.__hash = hash(tuple(self))
            return h


class _WeakKeyDict(WeakKeyDictionary, object):
    def __delitem__(self, key):
        del self.data[_WeakKey(key)]

    def __getitem__(self, key):
        return self.data[_WeakKey(key)]

    def __repr__(self):
        return "<_WeakKeyDict at %s>" % id(self)

    def __setitem__(self, key, value):
        self.data[_WeakKey(key, self._remove)] = value

    def __contains__(self, key):
        try:
            wr = _WeakKey(key)
        except TypeError:
            return False
        return wr in self.data

    def pop(self, key, *args):
        return self.data.pop(_WeakKey(key), *args)


class _WeakOrderedDict(_WeakKeyDict, object):
    def __init__(self):
        super(_WeakOrderedDict, self).__init__()
        self.data = OrderedDict()

    def popitem(self, last=True):
        while True:
            key, value = self.data.popitem(last)
            return key, value

    def move_to_end(self, key, last=True):
        """Move an existing element to the end (or beginning if last==False).

        Raises KeyError if the element does not exist.
        When last=True, acts like a fast version of self[key]=self.pop(key).
        """
        self[key] = self.pop(key)


remember_last = weak_lru_cache(1)
