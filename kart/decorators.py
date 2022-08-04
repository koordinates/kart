class allow_classmethod(object):
    """Allows an instance method to also be called as a classmethod"""

    def __init__(self, f):
        self.f = f

    def __get__(self, obj, cls=None):
        def newfunc(*args, **kwargs):
            return self.f(obj or cls, *args, **kwargs)

        return newfunc
