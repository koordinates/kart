import shutil

import reflink as rl


def reflink(from_, to):
    """Same as reflink.reflink, but can be called with pathlib.Path objects."""
    rl.reflink(str(from_), str(to))


def try_reflink(*args):
    """
    Usage:
    >>> try_reflink(from_, to) - tries to use reflink to copy, but falls back to shutil.copy
    >>> copy_ = try_reflink() - returns a callable object that tries to use reflink to copy until
        that fails, and from then on it uses shutil.copy
    """

    reflink_copier = ReflinkCopier()
    if args:
        return reflink_copier(*args)
    else:
        return reflink_copier


class ReflinkCopier:
    """
    A callable object that, when called with (from_, to), calls reflink(from_, to) -
    until that fails, and from then on it uses shutil.copy(from_, to)
    """

    def __init__(self):
        self._copy_fn = reflink

    def __call__(self, from_, to):
        try:
            return self._copy_fn(from_, to)
        except (rl.ReflinkImpossibleError, NotImplementedError):
            self._copy_fn = shutil.copy
        return self._copy_fn(from_, to)
