#!/usr/bin/env python3
import sys
import kart


def main(ctx, args):
    r = args[0] if args else "0"
    if r.isdigit():
        print(f"ext-run-test: returning {r}")
        sys.exit(int(r))
    else:
        print(f"ext-run-test: raising RuntimeError({r})")
        raise RuntimeError(r)
