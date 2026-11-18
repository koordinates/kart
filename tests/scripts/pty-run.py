"""
Runs a command with stdin/stdout/stderr attached to a pty, and echoes whatever the
command writes to our own stdout.

Used by the e2e tests to exercise the code paths Kart only takes when it's attached to
a terminal - notably the pager.
"""

import os
import pty
import sys


def main(argv: list[str]) -> int:
    # Don't forward our own stdin: the e2e tests are non-interactive, and reading from a
    # closed/redirected stdin here would just hang.
    status = pty.spawn(argv, lambda fd: os.read(fd, 4096), lambda fd: b"")
    return os.waitstatus_to_exitcode(status)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
