import os
import subprocess
import sys

from . import is_windows


def execvpe(cmd, args, env):
    if "_SNO_NO_EXEC" in os.environ:
        # used in testing. This is pretty hackzy
        p = subprocess.run(
            [cmd] + args[1:], capture_output=True, encoding="utf-8", env=env
        )
        sys.stdout.write(p.stdout)
        sys.stderr.write(p.stderr)
        sys.exit(p.returncode)
    elif is_windows:
        p = subprocess.run([cmd] + args[1:], env=env)
        sys.exit(p.returncode)
    else:  # Posix
        os.execvpe(cmd, args, env)


def execvp(cmd, args):
    return execvpe(cmd, args, os.environ)
