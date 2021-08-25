#!/usr/bin/env python3

import shutil
import sys
from pathlib import Path
from subprocess import check_call

from auditwheel.lddtree import lddtree
from auditwheel.policy.external_references import lddtree_external_references


POLICY = "manylinux_2_17_x86_64"  # ie: manylinux2014_x86_64


def main():
    if len(sys.argv) < 2:
        print(f"USAGE: {sys.argv[0]} PATH [RPATH]")
        sys.exit(2)

    is_err = False

    root = Path(sys.argv[1])
    if root.is_dir():
        libs = list(root.glob("*.so*"))
    else:
        libs, root = [root], root.parent

    if len(sys.argv) > 2:
        rpath = "$ORIGIN/{sys.argv[2]}"
    else:
        rpath = "$ORIGIN"

    for lib in libs:
        if lib.is_symlink():
            continue

        print(f"{lib}:")
        libtree = lddtree(str(lib))

        ext_refs = lddtree_external_references(libtree, root).get(POLICY, None)

        if ext_refs:
            for name, path in ext_refs["libs"].items():
                print(f" -> {name} (@ {path})")

                if path is None:
                    print(f"    ERROR: Couldn't find a real library path for: {name}")
                    is_err = True
                    continue

                local_dep = root / name
                if not local_dep.exists():
                    print(f"    Copying {path} -> {local_dep}")
                    shutil.copy2(path, local_dep, follow_symlinks=True)

            if not is_err:
                print(f" re-setting RPATH for {lib} -> {rpath} ...")
                check_call(["patchelf", "--remove-rpath", str(lib)])
                check_call(
                    ["patchelf", "--force-rpath", "--set-rpath", rpath, str(lib)]
                )

    if is_err:
        print("Errors encountered")
        sys.exit(1)


if __name__ == "__main__":
    main()
