__all__ = ('g',)

import threading
from contextlib import contextmanager

class Context(object):
    _local = threading.local()

    @property
    def actor(self):
        return self._local.actor

    @property
    def message(self):
        return self._local.message

    @contextmanager
    def set_context(self, actor, message):
        self._local.actor = actor
        self._local.message = message
        yield self
        self._local.actor = None
        self._local.message = None

g = Context()


    
