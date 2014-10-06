__all__ = ('g',)

import threading
from contextlib import contextmanager


class Context(object):
    _local = threading.local()

    def __getattr__(self, name):
        return getattr(self._local, name)

    @property
    def path(self):
        return getattr(self._local, 'path', None)
    @path.setter
    def path(self, value):
        self._local.path = value

    @contextmanager
    def set_context(self, **kwargs):
        saved = dict(
            (k, getattr(self._local, k, ()))
            for k in kwargs)
        for k, v in kwargs.items():
            setattr(self._local, k, v)
        yield self
        for k, v in saved.items():
            if v is ():
                delattr(self._local, k)
            else:
                setattr(self._local, k, v)

g = Context()
