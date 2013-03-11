class slot(object):
    '''Decorator that marks a method as a slot'''
    def __init__(self, name=None):
        self.name = name
    def __call__(self, func):
        if self.name is None:
            name = func.__name__
        else:
            name = self.name
        func._cleese_slot = name
        return func
