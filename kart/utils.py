import functools
import itertools
import os
from pathlib import Path
import platform
from typing import BinaryIO, Iterator


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


def chunk(iterable, size, strict=False):
    """Generator. Yield successive chunks from iterable of length <size>."""
    # TODO: replace this chunk() function with itertools.batched() (Python 3.12+)
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, size))
        if not chunk:
            return
        if strict and len(chunk) != size:
            raise ValueError("chunk(): incomplete batch")
        yield chunk


def iter_records_from_file(
    file_: BinaryIO, separator: bytes, chunk_size: int = 4096
) -> Iterator[bytes]:
    """
    Split a (binary) file into records, using a separator. Yields records as bytes.

    Oddly, there's no easy stdlib way to do this.
    See fairly-stale discussion at https://bugs.python.org/issue1152248
    """
    partial_record = None
    while True:
        chunk = file_.read(chunk_size)
        if not chunk:
            break
        pieces = chunk.split(separator)
        if partial_record is None:
            partial_record = pieces[0]
        else:
            partial_record += pieces[0]
        if len(pieces) > 1:
            yield partial_record
            for i in range(1, len(pieces) - 1):
                yield pieces[i]
            partial_record = pieces[-1]
    if partial_record is not None:
        yield partial_record


def get_num_available_cores():
    """
    Returns the number of available CPU cores (best effort)
      * uses cgroup quotas on Linux if available
      * uses processor affinity on Windows/Linux if available
      * otherwise, uses total number of CPU cores

    The result is a float which may or may not be a round number, and may be less than 1.
    """
    if platform.system() == "Linux":
        try:
            quota = float(Path("/sys/fs/cgroup/cpu/cpu.cfs_quota_us").read_text())
            period = float(Path("/sys/fs/cgroup/cpu/cpu.cfs_period_us").read_text())
        except FileNotFoundError:
            pass
        else:
            if quota == -1:
                # no quota set
                pass
            else:
                # note: this is a float, and may not be a round number
                # (it's possible to allocate half-cores)
                return quota / period
    try:
        return float(len(os.sched_getaffinity(0)))
    except AttributeError:
        # sched_getaffinity isn't available on some platforms (macOS mostly I think)
        # Fallback to total machine CPUs
        return float(os.cpu_count())


class classproperty:
    def __init__(self, getter):
        self.fget = getter

    def __get__(self, cls, owner):
        return self.fget(owner)
