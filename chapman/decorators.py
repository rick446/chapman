def task(name=None, **options):
    from chapman.task import Function
    return Function.decorate(name, **options)
