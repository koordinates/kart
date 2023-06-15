import shutil

import reflink as rl


def reflink(from_, to):
    """Same as reflink.reflink, but can be called with pathlib.Path objects."""
    rl.reflink(str(from_), str(to))


def try_reflink(from_, to):
    """Same as reflink_util.reflink, but falls back to shutil.copy if reflinking fails."""

    assert not to.exists()

    try:
        return reflink(from_, to)
    except (rl.ReflinkImpossibleError, NotImplementedError):
        pass
    return shutil.copy(from_, to)
