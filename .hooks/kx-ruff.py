#!/usr/bin/env python3
import subprocess
import sys

filez = sys.argv[1:]

print("Running ruff linter")
# TODO: should we enable `--unsafe-fixes` here?
# (Most of the 'unsafe' fixes appear to be "it might move a comment slightly" level of unsafe)
subprocess.check_call(["ruff", "check", "--fix", *filez])

print("Running ruff formatter")
subprocess.check_call(["ruff", "format", *filez])

# pre-commit doesn't add changed files to the index. Normally changed files fail the hook.
# however, just calling git add sneakily works around that.
subprocess.check_call(["git", "add"] + filez)
