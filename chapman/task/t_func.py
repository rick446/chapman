import sys
import logging

from chapman import exc
from chapman import model as M

from .t_base import Task, Result

log = logging.getLogger(__name__)


class Function(Task):
    target=None
    raise_errors=False
    options=None

    @classmethod
    def n(cls, *args, **kwargs):
        return cls.new(args, kwargs)

    @classmethod
    def ni(cls, *args, **kwargs):
        return cls.new(args, kwargs, immutable=True)

    @classmethod
    def new(cls, args=(), kwargs=None, **options):
        if kwargs is None:
            kwargs = {}
        if cls.options:
            all_options = dict(cls.options)
        else:
            all_options = {}
        all_options.update(options)
        data = dict(args=M.dumps(args),
                    kwargs=M.dumps(kwargs))
        return super(Function, cls).new(data, **all_options)

    @classmethod
    def spawn(cls, *args, **kwargs):
        self = cls.new(args, kwargs)
        self.start()
        return self

    def __repr__(self):
        return '<%s %s>' % (
            self.__class__.__name__,
            self._state._id)

    def run(self, msg):
        try:
            args, kwargs = self._merge_args(msg)
            raw = self.target(*args, **kwargs)
            result = Result.success(self._state._id, raw)
            self.complete(result)
        except exc.Suspend, s:
            self._state.m.set(dict(status=s.status))
        except Exception:
            if self.raise_errors or self._state is None:
                raise
            status = 'Error in %r' % self
            exc_info = sys.exc_info()
            result = Result.failure(self._state._id, status, *exc_info)
            try:
                self.complete(result)
            except Exception as err:
                log.error('Exception completing task: %s', err)
                result.data.args[1] = repr(exc_info[1])
                self.complete(result)

    def _merge_args(self, msg):
        '''Compute the *args and **kwargs based on the args and kwargs stored in
        the TaskState, possibly prepended (in the case of *args) or overridden
        (in the case of **kwargs) by msg
        '''
        args = M.loads(self._state.data.args)
        kwargs = M.loads(self._state.data.kwargs)
        if not self._state.options.immutable:
            args = msg.args + args
            kwargs.update(msg.kwargs)
        return args, kwargs

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
