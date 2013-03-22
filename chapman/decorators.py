class slot(object):
    '''Decorator that marks a method as a slot'''
    def __init__(self, name=None):
        self.name = name
    def __call__(self, func):
        if self.name is None:
            name = func.__name__
        else:
            name = self.name
        func._chapman_slot = name
        return func

def actor(name=None, **options):
    from chapman.function import FunctionActor
    def decorator(func):
        return FunctionActor.decorate(name, **options)(func)
    return decorator

def task(name=None):
    from chapman.task import Function
    return Function.decorate(name)
    
