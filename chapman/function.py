from .actor import Actor
from .decorators import slot

__all__ = ('FunctionActor',)

class FunctionActor(Actor):
    target=None

    @slot()
    def run(self, *args, **kwargs):
        data = self._state.data
        call_args = data['cargs'] + list(args)
        call_kwargs = dict(data['ckwargs'])
        call_kwargs.update(kwargs)
        return self.target(*call_args, **call_kwargs)

    @classmethod
    def decorate(cls, actor_name, ignore_result=False):
        '''Decorator to turn a function into an actor'''
        def decorator(func):
            return type(
                '%s(%s)' % (cls.__name__, func.__name__),
                (cls,),
                { 'target': staticmethod(func),
                  'name': actor_name,
                  'ignore_result': ignore_result })
        return decorator

    def curry(self, *args, **kwargs):
        if self._state.immutable: return self
        data = self._state.data
        cargs = data['cargs'] + list(args)
        ckwargs = dict(data['ckwargs'])
        ckwargs.update(kwargs)
        self.update_data(cargs=cargs, ckwargs=ckwargs)
        return self

