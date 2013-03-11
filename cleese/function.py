from .actor import Actor
from .decorators import slot

__all__ = ('FunctionActor',)

class FunctionActor(Actor):
    target=None

    @slot()
    def run(self, *args, **kwargs):
        return self.target(*args, **kwargs)

    @classmethod
    def spawn(cls, *args, **kwargs):
        obj = cls.create()
        cls.send(obj.id, 'run', args, kwargs)
        obj.refresh()
        return obj

    @classmethod
    def decorate(cls, actor_name):
        '''Decorator to turn a function into an actor'''
        def decorator(func):
            class _(cls):
                target=func
                name=actor_name
            _.__name__ = '%s(%s)' % (
                cls.__name__, func.__name__)
            return _
        return decorator

