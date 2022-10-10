#!/usr/bin/env python3

import argparse
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
from enum import Enum
from pathlib import Path

# Checks / fixes every lib in a vendor-PLATFORM.tar.gz archive according to the following rules.
# This includes the libraries that are currently embedded inside wheels.

# Glossary:
# A system dep is a dependency that we expect to be installed on the target system such as "libstdc++.so"
# Other dependencies are vendor deps - these ones we must bundle in the vendor archive.
#
# - fix_unsatisfied_deps:
#   All vendor deps must be contained in the vendor-archive. Vendor deps that are outside the archive but which can be
#   found on the filesystem will be copied into the right place.
#   All system deps must be explicitly allowed on the SYSTEM_DEPS_ALLOW_LIST.
#
# - fix_dep_linkage:
#   All vendor deps must be specified in the following manner.
#     * On Darwin: @rpath/<name-of-library>
#     * On Linux: simply <name-of-library>
#   Deps to libraries not contained in the archive are left unchanged.
#
# - fix_names:
#   All libraries must have install-names that are simply their own filename - not any other kind of path - or no install name at all.
#
# - fix_rpaths:
#   (Note that @loader_path on Darwin and $ORIGIN on Linux expand to the directory of the binary or shared object doing
#   the loading - they are both referred to as LOADER_PATH for convenience.)
#   All libraries must have an RPATH of LOADER_PATH to ensure they can find deps in the same folder.
#   All libraries that are not / will not be in env/lib must also have an RPATH of LOADER_PATH/<path-to-env-lib>,
#   using the library's eventual install location for libraries that are currently inside wheels.
#


USAGE = """

Usage: fix_vendor_libs INPUT_PATH [OUTPUT_PATH]")
    INPUT_PATH is the path to a vendor archive (eg vendor-Darwin.tar.gz),
        or a path to the uncompressed contents of a vendor archive.
    OUTPUT_PATH the path to which the fixed vendor archive is written.
        If not supplied, fix_vendor_libs runs in a dry-run mode where it fixes
        the archive in a temp directory, but doesn't output it anywhere.
"""

SITE_PACKAGES_PREFIX = "env/lib/python3.x/site-packages/"

# The sole directories allowed at the top level of vendor-Darwin.tar.gz
TOP_LEVEL_DIRECTORIES = ["env", "wheelhouse"]
# The sole files allowed at the top level of vendor-Darwin.tar.gz
TOP_LEVEL_FILES = ["_kart_env.py"]

PLATFORM = platform.system()

if PLATFORM == "Darwin":
    VENDOR_ARCHIVE_NAME = "vendor-Darwin.tar.gz"
    LOADER_PATH = "@loader_path"
    RPATH_PREFIX = "@rpath/"
    LIB_EXTENSIONS = [".dylib", ".so"]
    SYSTEM_PREFIXES = ["/usr/lib/"]
elif PLATFORM == "Linux":
    VENDOR_ARCHIVE_NAME = "vendor-Linux.tar.gz"
    LOADER_PATH = "$ORIGIN"
    RPATH_PREFIX = ""
    LIB_EXTENSIONS = [".so", ".so.*"]
    SYSTEM_PREFIXES = []

VENDOR_ARCHIVE_CONTENTS = f"{VENDOR_ARCHIVE_NAME}-contents"


if PLATFORM == "Darwin":
    SYSTEM_DEPS_ALLOW_LIST = [
        "/usr/lib/libSystem.B.dylib",
        "/usr/lib/libc++.1.dylib",
        "/usr/lib/libcharset.1.dylib",
        "/usr/lib/libiconv.2.dylib",
        "/usr/lib/libncurses.5.4.dylib",
        "/usr/lib/libpanel.5.4.dylib",
        "/usr/lib/libresolv.9.dylib",
        "/usr/lib/libsasl2.2.dylib",
        "/usr/lib/libz.1.dylib",
    ]
elif PLATFORM == "Linux":
    SYSTEM_DEPS_ALLOW_LIST = [
        "ld-linux-x86-64.so.2",
        "libc.so.6",
        "libcrypto.so.10",
        "libdl.so.2",
        "libexpat.so.1",
        "libgcc_s.so.1",
        "libm.so.6",
        "libodbc.so.2",
        "libpcre.so.1",
        "libpcreposix.so.0",
        "libpthread.so.0",
        "libpython3.7m.so.1.0",
        "libresolv.so.2",
        "librt.so.1",
        "libssl.so.10",
        "libstdc++.so.6",
        "libz.so.1",
    ]


SYSTEM_DEPS_ALLOW_SET = set(SYSTEM_DEPS_ALLOW_LIST)

verbosity = 0


class PlatformSpecific:
    """Marker for functions that vary by platform."""


def log_subprocess(event, event_args):
    if event == "subprocess.Popen":
        executable, args, cwd, env = event_args
        print("> invoke:", args)


def info(message):
    print(message)


def checkmark(message):
    print(f"✅  {message}")


def warn(message, make_fatal=False, detail=None):
    if make_fatal:
        fatal(message, detail)
    message = "\n".join([message, detail]) if detail else message
    print(f"⚠️  {message}", file=sys.stderr)


def fatal(message, detail=None):
    message = "\n".join([message, detail]) if detail else message
    print(f"❌  {message}", file=sys.stderr)
    sys.exit(1)


def json_dumps(json_obj, root_path):
    def default(unhandled):
        if isinstance(unhandled, Path):
            return os.path.relpath(unhandled, root_path)
        raise TypeError

    return json.dumps(json_obj, indent=2, default=default)


def unpack_all(input_path, root_path):
    contents_path = root_path / VENDOR_ARCHIVE_CONTENTS
    contents_path.mkdir()
    if input_path.is_file():
        info(f"Extracting {input_path} to {root_path} ...")
        subprocess.check_call(["tar", "-xzf", input_path, "--directory", contents_path])
    else:
        info(f"Copying from {input_path} to {root_path} ...")
        for d in TOP_LEVEL_DIRECTORIES:
            assert (input_path / d).is_dir()
            shutil.copytree(input_path / d, contents_path / d, symlinks=True)

        for f in TOP_LEVEL_FILES:
            assert (input_path / f).is_file()
            shutil.copy(input_path / f, contents_path / f, follow_symlinks=True)

    for d in TOP_LEVEL_DIRECTORIES:
        assert (contents_path / d).is_dir()

    for f in TOP_LEVEL_FILES:
        assert (contents_path / f).is_file()

    for path_to_wheel in wheel_paths(contents_path):
        unpack_wheel(path_to_wheel, root_path)


def pack_all(root_path, output_path):
    for path_to_wheel in wheel_paths(root_path):
        pack_wheel(path_to_wheel, root_path)

    info(f"Writing {output_path} ...")
    contents_path = root_path / VENDOR_ARCHIVE_CONTENTS
    assert contents_path.is_dir()
    subprocess.check_call(
        [
            "tar",
            "-czf",
            output_path,
            "--directory",
            contents_path,
            *[f.name for f in contents_path.glob("*")],
        ]
    )


def wheel_paths(root_path):
    yield from root_path.glob("**/*.whl")


def unpack_wheel(path_to_wheel, root_path):
    wheel_name = path_to_wheel.name

    info(f"Unpacking {wheel_name} ...")
    cmdline = [
        sys.executable,
        "-m",
        "wheel",
        "unpack",
        "--dest",
        root_path,
        path_to_wheel,
    ]
    if verbosity >= 2:
        print(cmdline)
    subprocess.check_output(cmdline)

    parts = wheel_name.split("-")
    wheel_id = f"{parts[0]}-{parts[1]}"

    wheel_contents_path = root_path / wheel_id
    if not wheel_contents_path.is_dir():
        fatal(f"Unpacking {wheel_name} didn't work as expected")

    wheel_contents_path.rename(root_path / f"{wheel_name}-contents")


def pack_wheel(path_to_wheel, root_path):
    wheel_name = path_to_wheel.name
    dest_dir = path_to_wheel.parents[0]
    wheel_contents_path = root_path / f"{wheel_name}-contents"
    assert wheel_contents_path.is_dir()

    info(f"Re-packing {wheel_name} ...")
    cmdline = [
        sys.executable,
        "-m",
        "wheel",
        "pack",
        "--dest-dir",
        dest_dir,
        wheel_contents_path,
    ]
    if verbosity >= 2:
        print(cmdline)
    subprocess.check_output(cmdline)


def read_cmd_lines(cmd):
    return subprocess.check_output(cmd, text=True).strip().splitlines()


def read_elf_cmd_lines(path_to_lib, pattern_to_read):
    result = []
    lines = read_cmd_lines(["readelf", "-d", path_to_lib])
    for line in lines:
        if pattern_to_read in line:
            result.append(line.strip().split()[4].strip("[]"))
    return result


def lib_paths(root_path, is_symlink=False):
    for ext in LIB_EXTENSIONS:
        for path_to_lib in root_path.glob(f"**/*{ext}"):
            if path_to_lib.is_symlink() == is_symlink:
                yield path_to_lib


is_binary = PlatformSpecific()


def is_binary_Darwin(path_to_bin):
    output = subprocess.check_output(["otool", "-D", path_to_bin], text=True)
    return "is not an object file" not in output


def is_binary_Linux(path_to_bin):
    try:
        get_install_name_Linux(path_to_bin)
        return True
    except subprocess.CalledProcessError:
        return False


def bin_paths(root_path, is_symlink=False):
    env_bin_path = root_path / VENDOR_ARCHIVE_CONTENTS / "env" / "bin"
    git_core_path = root_path / VENDOR_ARCHIVE_CONTENTS / "env" / "libexec" / "git-core"

    assert env_bin_path.is_dir()
    assert git_core_path.is_dir()

    for bin_dir in (env_bin_path, git_core_path):
        for path_to_bin in bin_dir.glob("*"):
            if path_to_bin.is_file() and path_to_bin.is_symlink() == is_symlink:
                if is_binary(path_to_bin):
                    yield path_to_bin


def lib_and_bin_paths(root_path, is_symlink=False):
    yield from bin_paths(root_path, is_symlink=is_symlink)
    yield from lib_paths(root_path, is_symlink=is_symlink)


def remove_lib_ext(lib_name):
    for ext in LIB_EXTENSIONS:
        if lib_name.endswith(ext):
            return lib_name[: -len(ext)]
    return lib_name


def split_lib_ext(lib_name):
    for ext in LIB_EXTENSIONS:
        if lib_name.endswith(ext):
            return lib_name[: -len(ext)], ext
    return lib_name, ""


DOT_PLUS_DIGITS = r"\.[0-9]+"
VERSION_PATTERN = re.compile("(" + DOT_PLUS_DIGITS + ")*$")


def split_lib_version_suffix(lib_name):
    match = VERSION_PATTERN.search(lib_name)
    if match:
        return lib_name[: match.span()[0]], match.group(0)
    return lib_name, ""


UNMODIFIED = 0
MODIFIED = 1


get_install_name = PlatformSpecific()


def get_install_name_Darwin(path_to_lib):
    lines = read_cmd_lines(["otool", "-D", path_to_lib])
    result = lines[1].strip() if len(lines) == 2 else None
    return result if result else None


def get_install_name_Linux(path_to_lib):
    lines = read_cmd_lines(["patchelf", "--print-soname", path_to_lib])
    result = lines[0].strip() if lines else None
    return result if result else None


set_install_name = PlatformSpecific()


def set_install_name_Darwin(path_to_lib, install_name):
    subprocess.check_call(["install_name_tool", "-id", install_name, path_to_lib])


def set_install_name_Linux(path_to_lib, install_name):
    subprocess.check_call(["patchelf", "--set-soname", install_name, path_to_lib])


def fix_names(root_path, make_fatal=False, verbose=False):
    problems = []
    for path_to_lib in lib_paths(root_path):
        if verbose:
            info(path_to_lib)
        install_name = get_install_name(path_to_lib)
        proposed_name = path_to_lib.name
        if install_name and install_name != proposed_name:
            problems.append(
                {
                    "lib": path_to_lib,
                    "install_name": install_name,
                    "proposed_name": proposed_name,
                }
            )

    if not problems:
        checkmark("Checking names: all libs are well named.")
        return UNMODIFIED

    detail = json_dumps(problems, root_path) if verbose else None
    warn(
        f"Checking names: found {len(problems)} libs with name issues.",
        make_fatal=make_fatal,
        detail=detail,
    )

    for problem in problems:
        path_to_lib = problem["lib"]
        install_name = problem["install_name"]
        proposed_name = problem["proposed_name"]

        if path_to_lib.name != proposed_name:
            rename_path = path_to_lib.parents[0] / proposed_name
            path_to_lib.rename(rename_path)
            path_to_lib = rename_path

        if install_name != proposed_name:
            set_install_name(path_to_lib, path_to_lib.name)

    return MODIFIED


get_rpaths = PlatformSpecific()


def get_rpaths_Darwin(path_to_lib):
    rpaths = []
    lines = read_cmd_lines(["otool", "-l", path_to_lib])
    for i, line in enumerate(lines):
        if "RPATH" in line:
            rpaths.append(lines[i + 2].split()[1])
    return rpaths


def get_rpaths_Linux(path_to_lib):
    lines = read_cmd_lines(["patchelf", "--print-rpath", path_to_lib])
    if not lines or not lines[0]:
        return []
    return lines[0].split(":")


set_sole_rpaths = PlatformSpecific()


def set_sole_rpaths_Darwin(path_to_lib, rpaths):
    remove_all_rpaths_Darwin(path_to_lib)
    for rpath in rpaths:
        subprocess.check_call(["install_name_tool", "-add_rpath", rpath, path_to_lib])


def set_sole_rpaths_Linux(path_to_lib, rpaths):
    rpaths = ":".join(rpaths)
    subprocess.check_call(["patchelf", "--set-rpath", rpaths, path_to_lib])


remove_all_rpaths = PlatformSpecific()


def remove_all_rpaths_Darwin(path_to_lib):
    for rpath in get_rpaths_Darwin(path_to_lib):
        subprocess.check_call(
            ["install_name_tool", "-delete_rpath", rpath, path_to_lib]
        )


def remove_all_rpaths_Linux(path_to_lib):
    subprocess.check_call(["patchelf", "--remove-rpath", path_to_lib])


def get_eventual_path(path_to_lib):
    path_to_lib = str(path_to_lib)
    path_within_contents = path_to_lib.split("-contents/", maxsplit=1)[1]
    if ".whl-contents/" in path_to_lib:
        return SITE_PACKAGES_PREFIX + path_within_contents
    return path_within_contents


def propose_rpaths(eventual_lib_path):
    path_to_env_lib = os.path.relpath("env/lib/", Path(eventual_lib_path).parents[0])
    if path_to_env_lib == ".":
        return [LOADER_PATH]
    path_to_env_lib = path_to_env_lib.rstrip("/") + "/"
    return [LOADER_PATH, f"{LOADER_PATH}/{path_to_env_lib}"]


def fix_rpaths(root_path, make_fatal=False, verbose=False):
    problems = []
    for path_to_lib in lib_and_bin_paths(root_path):
        if verbose:
            info(path_to_lib)
        actual_rpaths = get_rpaths(path_to_lib)
        eventual_path = get_eventual_path(path_to_lib)
        proposed_rpaths = propose_rpaths(eventual_path)
        if set(actual_rpaths) != set(proposed_rpaths):
            problems.append(
                {
                    "lib": path_to_lib,
                    "eventual_path": eventual_path,
                    "actual_rpaths": actual_rpaths,
                    "proposed_rpaths": proposed_rpaths,
                }
            )

    if not problems:
        checkmark("Checking rpaths: all libs have good rpaths.")
        return UNMODIFIED

    detail = json_dumps(problems, root_path) if verbose else None
    warn(
        f"Checking rpaths: found {len(problems)} libs with rpath issues.",
        make_fatal=make_fatal,
        detail=detail,
    )

    for problem in problems:
        set_sole_rpaths(problem["lib"], problem["proposed_rpaths"])

    return MODIFIED


fix_codesigning = PlatformSpecific()


def fix_codesigning_Darwin(root_path, make_fatal=False, verbose=False):
    problems = []
    for path_to_lib in lib_and_bin_paths(root_path):
        if verbose:
            info(path_to_lib)
        try:
            subprocess.check_output(
                ["codesign", "-vvvv", path_to_lib], stderr=subprocess.STDOUT, text=True
            )
            continue
        except subprocess.CalledProcessError as e:
            if "code object is not signed at all" not in e.stdout:
                problems.append(
                    {
                        "lib": path_to_lib,
                        "error": e.stdout,
                    }
                )

    if not problems:
        checkmark("Checking code signing: no invalid signatures.")
        return UNMODIFIED

    detail = json_dumps(problems, root_path) if verbose else None
    warn(
        f"Checking code signing: found {len(problems)} libs with signature issues.",
        make_fatal=make_fatal,
        detail=detail,
    )

    for problem in problems:
        subprocess.check_call(["codesign", "--remove-signature", path_to_lib])

    return MODIFIED


def fix_codesigning_Linux(root_path, make_fatal=False, verbose=False):
    return UNMODIFIED


get_deps = PlatformSpecific()


def get_deps_Darwin(path_to_lib):
    deps = []
    lines = read_cmd_lines(["otool", "-L", path_to_lib])
    for line in lines[1:]:
        dep = line.strip().split()[0]
        if any(dep.endswith(ext) for ext in LIB_EXTENSIONS):
            deps.append(dep)
    return deps


def get_deps_Linux(path_to_lib):
    return read_elf_cmd_lines(path_to_lib, "(NEEDED)")


def sorted_good_and_bad_deps(good_deps, bad_deps):
    output = []
    for dep in sorted(good_deps | bad_deps, key=lambda dep: Path(dep).name):
        prefix = "🆗  " if dep in good_deps else "❌  "
        output.append(prefix + dep)
    return "\n".join(output)


change_dep = PlatformSpecific()


def change_dep_Darwin(path_to_lib, old_dep, new_dep):
    subprocess.check_call(
        ["install_name_tool", "-change", old_dep, new_dep, path_to_lib]
    )


def change_dep_Linux(path_to_lib, old_dep, new_dep):
    subprocess.check_call(
        ["patchelf", "--replace-needed", old_dep, new_dep, path_to_lib]
    )


def get_pattern_for_dep(dep):
    base_name, ext = split_lib_ext(Path(dep).name)
    base_name, version_suffix = split_lib_version_suffix(base_name)
    return base_name + ext, base_name + ".*" + ext


def lib_names_match(dep1, dep2):
    base1, ext1 = split_lib_ext(Path(dep1).name)
    base2, ext2 = split_lib_ext(Path(dep1).name)
    return ext1 == ext2 and base1.startswith(base2) or base2.startswith(base1)


class FindDepResult(Enum):
    VENDOR_DEP_FOUND = "vendor dep found"
    VENDOR_DEP_NOT_FOUND = "vendor dep not found"
    ALLOWED_SYSTEM_DEP = "allowed system dep"
    UNEXPECTED_SYSTEM_DEP = "unexpected system dep"


VENDOR_DEP_FOUND = FindDepResult.VENDOR_DEP_FOUND
VENDOR_DEP_NOT_FOUND = FindDepResult.VENDOR_DEP_NOT_FOUND
ALLOWED_SYSTEM_DEP = FindDepResult.ALLOWED_SYSTEM_DEP
UNEXPECTED_SYSTEM_DEP = FindDepResult.UNEXPECTED_SYSTEM_DEP


def resolve_lib_in_folder(folder, lib_name):
    if lib_name is None:
        return None
    unresolved = folder / Path(lib_name).name
    if not unresolved.is_file():
        return None
    if unresolved.is_symlink():
        symlinked_to_name = unresolved.resolve()
        return resolve_lib_in_folder(folder, symlinked_to_name)
    else:
        return unresolved


def find_dep(dep_str, search_paths):
    if dep_str in SYSTEM_DEPS_ALLOW_SET:
        return ALLOWED_SYSTEM_DEP, None

    for system_prefix in SYSTEM_PREFIXES:
        if dep_str.startswith(system_prefix):
            return UNEXPECTED_SYSTEM_DEP, None

    dep_path = Path(dep_str)
    if dep_path.is_absolute() and dep_path.is_file():
        return VENDOR_DEP_FOUND, dep_path.resolve()

    dep_name = dep_path.name
    for search_path in search_paths:
        dep_path = resolve_lib_in_folder(search_path, dep_name)
        if dep_path:
            return VENDOR_DEP_FOUND, dep_path

    dep_name_base, dep_name_pattern = get_pattern_for_dep(dep_name)
    for search_path in search_paths:
        dep_path = resolve_lib_in_folder(search_path, dep_name_base)
        if not dep_path:
            dep_path = resolve_lib_in_folder(
                search_path, next(iter(search_path.glob(dep_name_pattern)), None)
            )
        if dep_path and lib_names_match(dep_str, dep_path):
            return VENDOR_DEP_FOUND, dep_path

    return VENDOR_DEP_NOT_FOUND, None


def fix_unsatisfied_deps(root_path, make_fatal=False, verbose=False):
    env_lib_path = root_path / VENDOR_ARCHIVE_CONTENTS / "env" / "lib"

    bin_paths_list = list(bin_paths(root_path))
    lib_paths_list = list(lib_paths(root_path))

    deps_by_result = {key: set() for key in FindDepResult}
    vendor_deps_found_outside = []

    def bin_and_lib_paths():
        yield from bin_paths_list
        yield from lib_paths_list

    for path_to_lib in bin_and_lib_paths():
        if verbose:
            info(path_to_lib)
        install_name = get_install_name(path_to_lib)
        search_paths = [
            env_lib_path,
            path_to_lib.parents[0],
            *[Path(rpath) for rpath in get_rpaths(path_to_lib)],
        ]

        for dep in get_deps(path_to_lib):
            if dep == install_name:
                continue
            result, found_path = find_dep(dep, search_paths)
            if verbose:
                info(f"  {dep} -> {result!s} @ {found_path or ''}")

            if found_path and found_path not in lib_paths_list:
                lib_paths_list.append(found_path)
                vendor_deps_found_outside.append(found_path)

    if deps_by_result[UNEXPECTED_SYSTEM_DEP]:
        detail = sorted_good_and_bad_deps(
            deps_by_result[ALLOWED_SYSTEM_DEP],
            deps_by_result[UNEXPECTED_SYSTEM_DEP],
        )
        count = len(deps_by_result[ALLOWED_SYSTEM_DEP])
        fatal(
            f"Checking deps: Found {count} system deps that have not been explicitly allowed.",
            detail=detail,
        )

    if deps_by_result[VENDOR_DEP_NOT_FOUND]:
        detail = sorted_good_and_bad_deps(
            set(Path(lib).name for lib in lib_paths_list),
            deps_by_result[VENDOR_DEP_NOT_FOUND],
        )
        count = len(deps_by_result[VENDOR_DEP_NOT_FOUND])
        fatal(
            f"Checking deps: Found {count} vendor deps where the library to satisfy the dep could not be found.",
            detail=detail,
        )

    if not vendor_deps_found_outside:
        checkmark(
            "Checking deps: all vendor deps are satisfied with libraries inside the vendor archive."
        )
        return UNMODIFIED

    count = len(vendor_deps_found_outside)
    detail = "\n".join(str(p) for p in vendor_deps_found_outside) if verbose else None
    warn(
        f"Checking deps: found {count} deps satisfied with a library outside the vendor archive.",
        make_fatal=make_fatal,
        detail=detail,
    )
    for src_path in vendor_deps_found_outside:
        dest_path = env_lib_path / src_path.name
        if not dest_path.exists():
            shutil.copy(src_path, dest_path)

    return MODIFIED


def fix_dep_linkage(root_path, make_fatal=False, verbose=False):
    env_lib_path = root_path / VENDOR_ARCHIVE_CONTENTS / "env" / "lib"

    problems = []
    for path_to_lib in lib_and_bin_paths(root_path):
        if verbose:
            info(path_to_lib)
        install_name = get_install_name(path_to_lib)
        search_paths = [
            env_lib_path,
            path_to_lib.parents[0],
        ]
        deps_to_change = []

        for dep in get_deps(path_to_lib):
            if dep == install_name:
                continue
            result, found_path = find_dep(dep, search_paths)
            if result != VENDOR_DEP_FOUND:
                continue

            proposed_dep = RPATH_PREFIX + found_path.name
            if dep != proposed_dep:
                deps_to_change.append([dep, proposed_dep])

        if deps_to_change:
            problems.append(
                {
                    "lib": path_to_lib,
                    "deps_to_change": deps_to_change,
                }
            )

    if not problems:
        checkmark("Checking dep linkage: all vendor deps are properly linked.")
        return UNMODIFIED

    detail = json_dumps(problems, root_path) if verbose else None
    warn(
        f"Checking dep linkage: found {len(problems)} libs with linkage issues.",
        make_fatal=make_fatal,
        detail=detail,
    )

    for problem in problems:
        path_to_lib = problem["lib"]
        for dep, proposed_dep in problem["deps_to_change"]:
            change_dep(path_to_lib, dep, proposed_dep)

    return MODIFIED


def fix_everything(input_path, output_path, verbose=0):
    if not input_path.resolve().exists():
        fatal(f"Path does not exist {input_path}")

    if output_path:
        if output_path.is_dir():
            output_path = output_path / VENDOR_ARCHIVE_NAME
    else:
        print("(Running in dry-run mode since no OUTPUT_PATH was supplied.)")

    with tempfile.TemporaryDirectory() as root_path:
        root_path = Path(root_path)
        unpack_all(input_path, root_path)

        kwargs = {"make_fatal": False, "verbose": bool(verbose)}

        status = UNMODIFIED
        status |= fix_unsatisfied_deps(root_path, **kwargs)
        status |= fix_dep_linkage(root_path, **kwargs)
        status |= fix_names(root_path, **kwargs)
        status |= fix_rpaths(root_path, **kwargs)
        # last, so it checks/fixes the modified files
        status |= fix_codesigning(root_path, **kwargs)

        if status == MODIFIED:
            checkmark("Finished fixing.\n")
            info("Checking everything was fixed ...")
            kwargs = {"make_fatal": True, "verbose": True}

            fix_unsatisfied_deps(root_path, **kwargs)
            fix_dep_linkage(root_path, **kwargs)
            fix_names(root_path, **kwargs)
            fix_rpaths(root_path, **kwargs)
            fix_codesigning(root_path, **kwargs)

        else:
            checkmark("Nothing to change.\n")

        if output_path:
            pack_all(root_path, output_path)
            checkmark(f"Wrote fixed archive to {output_path}")
        elif status == MODIFIED:
            warn("Archive was fixed, but not writing anywhere due to dry-run mode.")
            sys.exit(3)


# Make foo_{PLATFORM} functions work:
for symbol in list(globals().keys()):
    if isinstance(globals()[symbol], PlatformSpecific):
        globals()[symbol] = globals()[f"{symbol}_{PLATFORM}"]


def main():
    parser = argparse.ArgumentParser(usage=USAGE)
    parser.add_argument("input_path", type=Path, help="Path to vendor archive/dir")
    parser.add_argument(
        "output_path",
        type=Path,
        nargs="?",
        help="Path to output. If not specified, perform a dry-run",
    )
    parser.add_argument(
        "-v", "--verbose", default=0, action="count", help="Increase verbosity"
    )
    args = parser.parse_args()

    if args.verbose >= 2:
        sys.addaudithook(log_subprocess)

    fix_everything(args.input_path, args.output_path, verbose=args.verbose)


if __name__ == "__main__":
    main()
