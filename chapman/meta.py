class RegistryMetaclass(type):

    def __new__(meta, name, bases, dct):
        dct.setdefault('name', name)
        result = type.__new__(meta, name, bases, dct)
        result._registry[result.name] = result
        return result

    def by_name(cls, name):
        return cls._registry[name]

class SlotsMetaclass(RegistryMetaclass):
    def __new__(meta, name, bases, dct):
        slots = {}
        for k,v in dct.items():
            s = getattr(v, '_chapman_slot', None)
            if s is None: continue
            slots[k] = s
        cls = RegistryMetaclass.__new__(meta, name, bases, dct)
        myslots = {}
        for b in reversed(bases):
            myslots.update(getattr(b, '_slots', {}))
        myslots.update(slots)
        cls._slots = myslots
        return cls

    
