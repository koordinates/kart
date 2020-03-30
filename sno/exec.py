import os
import subprocess
import sys

from . import is_windows


def execvp(cmd, args):
    if "_SNO_NO_EXEC" in os.environ:
        # used in testing. This is pretty hackzy
        p = subprocess.run([cmd] + args[1:], capture_output=True, encoding="utf-8")
        sys.stdout.write(p.stdout)
        sys.stderr.write(p.stderr)
        sys.exit(p.returncode)
    elif is_windows:
        p = subprocess.run([cmd] + args[1:])
        sys.exit(p.returncode)
    else:  # Posix
        os.execvp(cmd, args)
