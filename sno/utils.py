import functools


def ungenerator(cast_function):
    """
    Decorator.
    Turns a generator into something else. Typically a list or a dict.
    Usage:
        @ungenerator(dict):
        def mygenerator():
            yield 'x', 'y'

        >>> mygenerator()
        {'x': 'y'}
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            generator = func(*args, **kwargs)
            return cast_function(generator)

        return wrapper

    return decorator
