import sys

from chapman import exc
from chapman import model as M

from .t_base import Task, Result

class Function(Task):
    target=None
    raise_errors=False
    options=None

    @classmethod
    def s(cls, **options):
        if cls.options:
            all_options = dict(cls.options)
        else:
            all_options = {}
        all_options.update(options)
        return super(Function, cls).s(**all_options)

    def __repr__(self):
        return '<%s %s>' % (
            self.__class__.__name__,
            self._state._id)

    def run(self, msg):
        try:
            if self._state.options.immutable:
                raw = self.target()
            else:
                raw = self.target(*msg.args, **msg.kwargs)
            result = Result.success(self._state._id, raw)
            self.complete(result)
        except exc.Suspend, s:
            self._state.m.set(dict(status=s.status))
        except Exception:
            if self.raise_errors:
                raise
            result = Result.failure(
                self._state._id, 'Error in %r' % self, *sys.exc_info())
            self.complete(result)
            
    @classmethod
    def decorate(cls, name=None, **options):
        '''Decorator to turn a function into an actor'''
        def decorator(func):
            if name is None:
                n = '%s.%s' % (
                    func.__module__, func.__name__)
            else:
                n = name
            return FunctionTaskWrapper(n, func, options)
        return decorator

class FunctionTaskWrapper(object):

    def __init__(self, name, target, options):
        class_name = 'FunctionTask<%s>' % name
        bases = (Function,)
        dct = dict(
            target=staticmethod(target),
            name=name,
            options=options)
        if 'raise_errors' in options:
            dct['raise_errors'] = options.pop('raise_errors')
        self._cls = type(class_name, bases, dct)

    def __getattr__(self, name):
        return getattr(self._cls, name)

    def __repr__(self): # pragma no cover
        return '<Wrapper %s>' % (self._cls.__name__)

    def __call__(self, *args, **kwargs):
        return self._cls.target(*args, **kwargs)
