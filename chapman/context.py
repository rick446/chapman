__all__ = ('g',)

import threading
from contextlib import contextmanager


class Context(object):
    _local = threading.local()

    @property
    def task(self):
        return self._local.task

    @property
    def message(self):
        return self._local.message

    @contextmanager
    def set_context(self, task, message):
        self._local.task = task
        self._local.message = message
        yield self
        self._local.task = None
        self._local.message = None

g = Context()
