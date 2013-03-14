import logging

from .actor import Actor
from .decorators import slot

__all__ = ('FunctionActor',)

log = logging.getLogger()

class FunctionActor(Actor):
    target=None

    @slot()
    def run(self, *args, **kwargs):
        data = self._state.get_data
        call_args = data('cargs') + list(args)
        call_kwargs = dict(data('ckwargs'))
        call_kwargs.update(kwargs)
        log.info('run %r (*%r, **%r)', self, call_args, call_kwargs)
        return self.target(*call_args, **call_kwargs)

    @classmethod
    def decorate(cls, actor_name, **options):
        '''Decorator to turn a function into an actor'''
        def decorator(func):
            if actor_name is None:
                name = '%s.%s' % (
                    func.__module__, func.__name__)
            else:
                name = actor_name
            return type(
                '%s(%s)' % (cls.__name__, func.__name__),
                (cls,),
                { 'target': staticmethod(func),
                  'name': name,
                  '_options': dict(options) })
        return decorator

    def curry(self, *args, **kwargs):
        if self._state.options.immutable: return self
        data = self._state.get_data
        cargs = data('cargs') + list(args)
        ckwargs = dict(data('ckwargs'))
        ckwargs.update(kwargs)
        self.update_data(cargs=cargs, ckwargs=ckwargs)
        return self

