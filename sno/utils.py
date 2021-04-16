import functools
import itertools
import os
import platform
from pathlib import Path


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


def chunk(iterable, size):
    """Generator. Yield successive chunks from iterable of length <size>."""
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, size))
        if not chunk:
            return
        yield chunk


def get_num_available_cores():
    """
    Returns the number of available CPU cores (best effort)
      * uses cgroup quotas on Linux if available
      * uses processor affinity on Windows/Linux if available
      * otherwise, uses total number of CPU cores

    The result is a float which may or may not be a round number, and may be less than 1.
    """
    if platform.system() == "Linux":
        quota_f = Path("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
        try:
            quota = quota_f.read_text()
            period = Path("/sys/fs/cgroup/cpu/cpu.cfs_period_us").read_text()
        except FileNotFoundError:
            pass
        else:
            if quota == -1:
                # no quota set
                pass
            else:
                # note: this is a float, and may not be a round number
                # (it's possible to allocate half-cores)
                return float(quota / period)
    try:
        return float(len(os.sched_getaffinity(0)))
    except AttributeError:
        # sched_getaffinity isn't available on some platforms (macOS mostly I think)
        # Fallback to total machine CPUs
        return float(os.cpu_count())
