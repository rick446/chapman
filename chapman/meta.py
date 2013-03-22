class RegistryMetaclass(type):

    def __new__(meta, name, bases, dct):
        dct.setdefault('name', name)
        result = type.__new__(meta, name, bases, dct)
        result._registry[result.name] = result
        return result

    def by_name(cls, name):
        return cls._registry[name]

