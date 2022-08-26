import os
import sys
import subprocess as s
from subprocess import *  # noqa


# kart.subprocess works exactly like subprocess unless the _KART_TEST environment variable is set.
# In that case, kart.subprocess.run is actually run_for_test which makes sure the subprocess output
# passes through sys.stdout and sys.stderr and is captured by CliRunner.


def run(*args, **kwargs):
    if "_KART_TEST" in os.environ:
        return run_for_test(*args, **kwargs)
    else:
        return s.run(*args, **kwargs)


def run_for_test(*args, **kwargs):
    # Don't do this if we're doing anything complicated:
    if "stdout" in kwargs or "stderr" in kwargs or "capture_output" in kwargs:
        return s.run(*args, **kwargs)

    # Helps CliRunner capture subprocess output during testing. This is pretty hacky.
    # TODO - find out if there's a better way to let CliRunner capture subprocess output.
    p = s.run(*args, **kwargs, capture_output=True, encoding="utf-8")
    sys.stdout.write(p.stdout)
    sys.stdout.flush()
    sys.stderr.write(p.stderr)
    sys.stderr.flush()
    return p
