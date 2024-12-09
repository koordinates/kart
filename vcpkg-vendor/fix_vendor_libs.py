#!/usr/bin/env python3

import argparse
import json
import logging
import os
import platform
import re
import shutil
import subprocess
import sys
import sysconfig
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

Usage: fix_vendor_libs [OPTIONS] INPUT_PATH [OUTPUT_PATH]

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
NOFIX_PATHS = []

PLATFORM = platform.system()

if PLATFORM == "Windows":
    VENDOR_ARCHIVE_NAME = "vendor-Windows.zip"
    RPATH_PREFIX = ""
    LIB_EXTENSIONS = [".lib", ".dll", ".pyd"]
    SYSTEM_PREFIXES = []
    EXE_PATHS = ["env/scripts", "env/tools/gdal"]
    EXE_EXTENSION = ".exe"
    TOP_LEVEL_DIRECTORIES += ["git"]
    NOFIX_PATHS = ["git"]
elif PLATFORM == "Darwin":
    VENDOR_ARCHIVE_NAME = "vendor-Darwin.tar.gz"
    LOADER_PATH = "@loader_path"
    RPATH_PREFIX = "@rpath/"
    LIB_EXTENSIONS = [".dylib", ".so"]
    SYSTEM_PREFIXES = ["/usr/lib/"]
    EXE_PATHS = ["env/bin", "env/libexec/git-core", "env/tools/gdal"]
    EXE_EXTENSION = ""
elif PLATFORM == "Linux":
    VENDOR_ARCHIVE_NAME = "vendor-Linux.tar.gz"
    LOADER_PATH = "$ORIGIN"
    RPATH_PREFIX = ""
    LIB_EXTENSIONS = [".so", ".so.*"]
    SYSTEM_PREFIXES = []
    EXE_PATHS = ["env/bin", "env/libexec/git-core", "env/tools/gdal"]
    EXE_EXTENSION = ""

if PLATFORM == "Windows":
    OK, LOOK, WARN, ERR = ":ok:", ":look:", ":warn:", ":err:"
else:
    OK, LOOK, WARN, ERR = "✅", "👀", "⚠️", "❌"


VENDOR_ARCHIVE_CONTENTS = f"{VENDOR_ARCHIVE_NAME}-contents"

EXTRA_SEARCH_PATHS = []

if PLATFORM == "Windows":
    SYSTEM_DEPS_ALLOW_LIST = [
        "advapi32.dll",
        "api-ms-win-core-path-l1-1-0.dll",
        "api-ms-win-core-synch-l1-2-0.dll",
        "api-ms-win-crt-conio-l1-1-0.dll",
        "api-ms-win-crt-convert-l1-1-0.dll",
        "api-ms-win-crt-environment-l1-1-0.dll",
        "api-ms-win-crt-filesystem-l1-1-0.dll",
        "api-ms-win-crt-heap-l1-1-0.dll",
        "api-ms-win-crt-locale-l1-1-0.dll",
        "api-ms-win-crt-math-l1-1-0.dll",
        "api-ms-win-crt-process-l1-1-0.dll",
        "api-ms-win-crt-runtime-l1-1-0.dll",
        "api-ms-win-crt-stdio-l1-1-0.dll",
        "api-ms-win-crt-string-l1-1-0.dll",
        "api-ms-win-crt-time-l1-1-0.dll",
        "api-ms-win-crt-utility-l1-1-0.dll",
        "bcrypt.dll",
        "bcryptprimitives.dll",
        "crypt32.dll",
        "kernel32.dll",
        "kernel32.dll",
        "msvcp140.dll",
        "ntdll.dll",
        "ole32.dll",
        "oleaut32.dll",
        "rpcrt4.dll",
        "secur32.dll",
        "shell32.dll",
        "shlwapi.dll",
        "ucrtbased.dll",
        "user32.dll",
        "vcruntime140.dll",
        "vcruntime140_1.dll",
        "vcruntime140d.dll",
        "version.dll",
        "winhttp.dll",
        "ws2_32.dll",
        "odbc32.dll",
        "wldap32.dll",
        "python3.dll",
        # python39.dll or similar:
        f"python{sysconfig.get_config_var('py_version_nodot')}.dll",
    ]
elif PLATFORM == "Darwin":
    SYSTEM_DEPS_ALLOW_LIST = [
        "/usr/lib/libSystem.B.dylib",
        "/usr/lib/libc++.1.dylib",
        "/usr/lib/libcharset.1.dylib",
        "/usr/lib/libiconv.2.dylib",
        "/usr/lib/libncurses.5.4.dylib",
        "/usr/lib/libobjc.A.dylib",
        "/usr/lib/libpanel.5.4.dylib",
        "/usr/lib/libresolv.9.dylib",
        "/usr/lib/libsasl2.2.dylib",
        "/usr/lib/libz.1.dylib",
    ]
elif PLATFORM == "Linux":
    SYSTEM_DEPS_ALLOW_LIST = [
        "ld-linux-x86-64.so.2",
        "ld-linux-aarch64.so.1",
        "libc.so.6",
        "libdl.so.2",
        "libgcc_s.so.1",
        "libm.so.6",
        "libpthread.so.0",
        "libresolv.so.2",
        "librt.so.1",
        "libstdc++.so.6",
        # special: ODBC drivers link to it, so don't bundle our own
        "libodbc.so.2",
    ]

IGNORE_LIST = ["git-lfs"]

SYSTEM_DEPS_ALLOW_SET = set(SYSTEM_DEPS_ALLOW_LIST)


class PlatformSpecific:
    """Marker for functions that vary by platform."""


L = logging.getLogger("fix_vendor_libs")


def log_subprocess(event, event_args):
    if event == "subprocess.Popen":
        executable, args, cwd, env = event_args
        L.debug("invoking: %s", " ".join(map(str, args)))


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
        print(f"Extracting {input_path} to {root_path} ...")
        subprocess.check_call(["tar", "-xzf", input_path, "--directory", contents_path])
    else:
        print(f"Copying from {input_path} to {root_path} ...")
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

    print(f"Writing {output_path} ...")
    contents_path = root_path / VENDOR_ARCHIVE_CONTENTS
    assert contents_path.is_dir()
    name = output_path.name
    if name.endswith(".zip"):
        # CMake can make zips, means we don't need to depend on 7z/etc
        cmake_cmd = os.environ.get("CMAKE_COMMAND", "cmake")
        subprocess.check_call(
            [
                cmake_cmd,
                "-E",
                "tar",
                "cf",
                output_path.absolute(),
                "--format=zip",
                *[f.name for f in contents_path.glob("*")],
            ],
            cwd=contents_path,
        )
    elif name.endswith(".tar.gz") or name.endswith(".tgz"):
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
    else:
        raise RuntimeError(f"Bad output path: {output_path}")


def wheel_paths(root_path):
    yield from root_path.glob("**/*.whl")


def unpack_wheel(path_to_wheel, root_path):
    wheel_name = path_to_wheel.name

    print(f"Unpacking {wheel_name} ...")
    subprocess.check_output(
        [
            sys.executable,
            "-m",
            "wheel",
            "unpack",
            "--dest",
            root_path,
            path_to_wheel,
        ]
    )

    parts = wheel_name.split("-")
    wheel_id = f"{parts[0]}-{parts[1]}"

    wheel_contents_path = root_path / wheel_id
    if not wheel_contents_path.is_dir():
        L.error(f"{ERR} Unpacking {wheel_name} didn't work as expected")
        sys.exit(1)

    wheel_contents_path.rename(root_path / f"{wheel_name}-contents")


def pack_wheel(path_to_wheel, root_path):
    wheel_name = path_to_wheel.name
    dest_dir = path_to_wheel.parents[0]
    wheel_contents_path = root_path / f"{wheel_name}-contents"
    assert wheel_contents_path.is_dir()

    print(f"Re-packing {wheel_name} ...")
    subprocess.check_output(
        [
            sys.executable,
            "-m",
            "wheel",
            "pack",
            "--dest-dir",
            dest_dir,
            wheel_contents_path,
        ]
    )


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
            if any(
                path_to_lib.is_relative_to(root_path / VENDOR_ARCHIVE_CONTENTS / p)
                for p in NOFIX_PATHS
            ):
                continue

            if path_to_lib.is_symlink() == is_symlink:
                yield path_to_lib


is_binary = PlatformSpecific()


def is_binary_Windows(path_to_bin):
    name = path_to_bin.name
    return name.endswith(".dll") or name.endswith(".lib") or name.endswith(".exe")


def is_binary_Darwin(path_to_bin):
    output = subprocess.check_output(["otool", "-D", path_to_bin], text=True)
    return "is not an object file" not in output


def is_binary_Linux(path_to_bin):
    try:
        get_install_name_Linux(path_to_bin)
        return True
    except subprocess.CalledProcessError:
        return False


def exe_paths(root_path, is_symlink=False):
    exe_dirs = [root_path / VENDOR_ARCHIVE_CONTENTS / p for p in EXE_PATHS]

    for exe_dir in exe_dirs:
        assert exe_dir.is_dir()

    for exe_dir in exe_dirs:
        for path_to_exe in exe_dir.glob(f"*{EXE_EXTENSION}"):
            if path_to_exe.is_file() and path_to_exe.is_symlink() == is_symlink:
                if is_binary(path_to_exe):
                    yield path_to_exe


def lib_and_exe_paths(root_path, is_symlink=False):
    yield from exe_paths(root_path, is_symlink=is_symlink)
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


def get_install_name_Windows(path_to_lib):
    return path_to_lib.name


def get_install_name_Darwin(path_to_lib):
    lines = read_cmd_lines(["otool", "-D", path_to_lib])
    result = lines[1].strip() if len(lines) == 2 else None
    return result if result else None


def get_install_name_Linux(path_to_lib):
    lines = read_cmd_lines(["patchelf", "--print-soname", path_to_lib])
    result = lines[0].strip() if lines else None
    return result if result else None


set_install_name = PlatformSpecific()


def set_install_name_Windows(path_to_lib, install_name):
    raise NotImplementedError()


def set_install_name_Darwin(path_to_lib, install_name):
    subprocess.check_call(["install_name_tool", "-id", install_name, path_to_lib])


def set_install_name_Linux(path_to_lib, install_name):
    subprocess.check_call(["patchelf", "--set-soname", install_name, path_to_lib])


def fix_names(root_path, make_fatal=False, verbose=False):
    if PLATFORM == "Windows":
        return UNMODIFIED

    problems = []
    for path_to_lib in lib_paths(root_path):
        L.info(f"{LOOK} {path_to_lib}")

        if path_to_lib.name in IGNORE_LIST:
            L.debug("  ignoring")
            continue

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
        print(f"{OK} Checking names: all libs are well named.")
        return UNMODIFIED

    detail = json_dumps(problems, root_path) if verbose else None
    L.warning(
        f"{WARN} Checking names: found %s libs with name issues: %s",
        len(problems),
        detail,
    )
    if make_fatal:
        sys.exit(1)

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


def get_rpaths_Windows(path_to_lib):
    return []


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


def set_sole_rpaths_Windows(path_to_lib, rpaths):
    raise NotImplementedError()


def set_sole_rpaths_Darwin(path_to_lib, rpaths):
    remove_all_rpaths_Darwin(path_to_lib)
    for rpath in rpaths:
        subprocess.check_call(["install_name_tool", "-add_rpath", rpath, path_to_lib])


def set_sole_rpaths_Linux(path_to_lib, rpaths):
    rpaths = ":".join(rpaths)
    subprocess.check_call(["patchelf", "--set-rpath", rpaths, path_to_lib])


remove_all_rpaths = PlatformSpecific()


def remove_all_rpaths_Windows(path_to_lib):
    raise NotImplementedError()


def remove_all_rpaths_Darwin(path_to_lib):
    for rpath in get_rpaths_Darwin(path_to_lib):
        subprocess.check_call(
            ["install_name_tool", "-delete_rpath", rpath, path_to_lib]
        )


def remove_all_rpaths_Linux(path_to_lib):
    subprocess.check_call(["patchelf", "--remove-rpath", path_to_lib])


def get_eventual_path(path_to_lib):
    path_to_lib = str(path_to_lib)
    path_within_contents = path_to_lib.split(f"-contents{os.sep}", maxsplit=1)[1]
    if ".whl-contents/" in path_to_lib:
        return SITE_PACKAGES_PREFIX + path_within_contents
    return path_within_contents


def propose_rpaths(eventual_lib_path):
    path_to_env_lib = os.path.relpath("env/lib/", Path(eventual_lib_path).parents[0])

    if path_to_env_lib == ".":
        return [LOADER_PATH]

    path_to_env_lib = path_to_env_lib.rstrip("/") + "/"
    rpaths = [LOADER_PATH, f"{LOADER_PATH}/{path_to_env_lib}"]

    if "libexec/git-core/" in eventual_lib_path:
        path_to_env = os.path.relpath("env/", Path(eventual_lib_path).parents[0])
        path_to_env = path_to_env.rstrip("/") + "/"
        rpaths.append(f"{LOADER_PATH}/{path_to_env}")

    return rpaths


def fix_rpaths(root_path, make_fatal=False, verbose=False):
    if PLATFORM == "Windows":
        return UNMODIFIED

    problems = []
    for path_to_lib in lib_and_exe_paths(root_path):
        L.info(f"{LOOK} {path_to_lib}")

        if path_to_lib.name in IGNORE_LIST:
            L.debug("  ignoring")
            continue

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
        print(f"{OK} Checking rpaths: all libs have good rpaths.")
        return UNMODIFIED

    detail = json_dumps(problems, root_path) if verbose else None
    L.warning(
        f"{WARN} Checking rpaths: found %s libs with rpath issues.\n%s",
        len(problems),
        detail,
    )
    if make_fatal:
        sys.exit(1)

    for problem in problems:
        set_sole_rpaths(problem["lib"], problem["proposed_rpaths"])

    return MODIFIED


def fix_codesigning(root_path, make_fatal=False, verbose=False):
    if PLATFORM != "Darwin":
        return UNMODIFIED

    problems = []
    for path_to_lib in lib_and_exe_paths(root_path):
        L.info(f"{LOOK} {path_to_lib}")
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
        print(f"{OK} Checking code signing: no invalid signatures.")
        return UNMODIFIED

    detail = json_dumps(problems, root_path) if verbose else None
    L.warning(
        f"{WARN}  Checking code signing: found %s libs with signature issues: %s",
        len(problems),
        detail,
    )
    if make_fatal:
        sys.exit(1)

    for problem in problems:
        subprocess.check_call(["codesign", "--remove-signature", path_to_lib])

    return MODIFIED


get_deps = PlatformSpecific()


def get_deps_Windows(path_to_lib):
    deps = []
    lines = read_cmd_lines(["dumpbin", "/dependents", path_to_lib])
    for line in lines:
        dep = line.strip()
        if any(dep.endswith(ext) for ext in LIB_EXTENSIONS):
            deps.append(dep)
    return deps


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
        prefix = f"{OK} " if dep in good_deps else f"{ERR} "
        output.append(prefix + dep)
    return "\n".join(output)


change_dep = PlatformSpecific()


def change_dep_Windows(path_to_lib, old_dep, new_dep):
    raise NotImplementedError()


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

    if PLATFORM == "Windows" and dep_str.lower() in SYSTEM_DEPS_ALLOW_SET:
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

    exe_paths_list = list(exe_paths(root_path))
    lib_paths_list = list(lib_paths(root_path))

    deps_by_result = {key: set() for key in FindDepResult}
    vendor_deps_found_outside = []

    def exe_and_lib_paths():
        yield from exe_paths_list
        yield from lib_paths_list

    for path_to_lib in exe_and_lib_paths():
        L.info(f"{LOOK} {path_to_lib}")
        install_name = get_install_name(path_to_lib)
        search_paths = [
            env_lib_path,
            path_to_lib.parents[0],
            *[Path(rpath) for rpath in get_rpaths(path_to_lib)],
            *EXTRA_SEARCH_PATHS,
        ]

        if verbose:
            L.debug("  searching: %s)", search_paths)

        for dep in get_deps(path_to_lib):
            if dep == install_name:
                continue
            result, found_path = find_dep(dep, search_paths)
            if verbose:
                L.debug(f"  {dep} -> {result!s} @ {found_path or ''}")

            deps_by_result[result].add(dep)
            if found_path and found_path not in lib_paths_list:
                lib_paths_list.append(found_path)
                vendor_deps_found_outside.append(found_path)

    if deps_by_result[UNEXPECTED_SYSTEM_DEP]:
        detail = sorted_good_and_bad_deps(
            deps_by_result[ALLOWED_SYSTEM_DEP],
            deps_by_result[UNEXPECTED_SYSTEM_DEP],
        )
        count = len(deps_by_result[ALLOWED_SYSTEM_DEP])
        L.error(
            f"{ERR} Checking deps: Found %s system deps that have not been explicitly allowed.\n%s",
            count,
            detail,
        )
        sys.exit(1)

    if deps_by_result[VENDOR_DEP_NOT_FOUND]:
        detail = sorted_good_and_bad_deps(
            set(Path(lib).name for lib in lib_paths_list),
            deps_by_result[VENDOR_DEP_NOT_FOUND],
        )
        count = len(deps_by_result[VENDOR_DEP_NOT_FOUND])
        L.error(
            f"{ERR} Checking deps: Found %s vendor deps where the library to satisfy the dep could not be found.\n%s",
            count,
            detail,
        )
        sys.exit(1)

    if not vendor_deps_found_outside:
        print(
            f"{OK} Checking deps: all vendor deps are satisfied with libraries inside the vendor archive."
        )
        return UNMODIFIED

    count = len(vendor_deps_found_outside)
    detail = "\n".join(str(p) for p in vendor_deps_found_outside) if verbose else None
    L.warning(
        f"{WARN}  Checking deps: found %s deps satisfied with a library outside the vendor archive.\n%s",
        count,
        detail,
    )
    if make_fatal:
        sys.exit(1)

    for src_path in vendor_deps_found_outside:
        dest_path = env_lib_path / src_path.name
        if not dest_path.exists():
            shutil.copy(src_path, dest_path)

    return MODIFIED


def fix_dep_linkage(root_path, make_fatal=False, verbose=False):
    if PLATFORM == "Windows":
        return UNMODIFIED

    env_lib_path = root_path / VENDOR_ARCHIVE_CONTENTS / "env" / "lib"

    problems = []
    for path_to_lib in lib_and_exe_paths(root_path):
        L.info(f"{LOOK} {path_to_lib}")
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
        print(f"{OK} Checking dep linkage: all vendor deps are properly linked.")
        return UNMODIFIED

    detail = json_dumps(problems, root_path) if verbose else None
    L.warning(
        f"{WARN}  Checking dep linkage: found %s libs with linkage issues.\n%s",
        len(problems),
        detail,
    )
    if make_fatal:
        sys.exit(1)

    for problem in problems:
        path_to_lib = problem["lib"]

        if path_to_lib.name in IGNORE_LIST:
            L.error(
                "  %s in IGNORE_LIST, but we need to change a dep: %s",
                path_to_lib,
                problem["deps_to_change"],
            )
            sys.exit(1)

        for dep, proposed_dep in problem["deps_to_change"]:
            change_dep(path_to_lib, dep, proposed_dep)

    return MODIFIED


def fix_everything(input_path, output_path, verbose=0):
    if not input_path.resolve().exists():
        L.error("Path does not exist: %s", input_path)
        sys.exit(2)

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
            print(f"{OK} Finished fixing.\n")
            print("\nChecking everything was fixed ...")
            kwargs = {"make_fatal": True, "verbose": True}

            fix_unsatisfied_deps(root_path, **kwargs)
            fix_dep_linkage(root_path, **kwargs)
            fix_names(root_path, **kwargs)
            fix_rpaths(root_path, **kwargs)
            fix_codesigning(root_path, **kwargs)

        else:
            print(f"{OK} Nothing to change.\n")

        if output_path:
            pack_all(root_path, output_path)
            print(f"{OK} Wrote fixed archive to {output_path}")
        elif status == MODIFIED:
            L.warning(
                f"{WARN}  Archive was fixed, but not writing anywhere due to dry-run mode."
            )
            sys.exit(3)


# Make foo_{PLATFORM} functions work:
for symbol in list(globals().keys()):
    if isinstance(globals()[symbol], PlatformSpecific):
        globals()[symbol] = globals()[f"{symbol}_{PLATFORM}"]


def main():
    parser = argparse.ArgumentParser(usage=USAGE)
    parser.add_argument("-v", "--verbose", type=int, default=0, help="Verbosity (0-3)")
    parser.add_argument(
        "-s",
        "--search-path",
        type=Path,
        action="append",
        help="Additional library search paths",
    )
    parser.add_argument("input_path", type=Path, help="Path to vendor archive/dir")
    parser.add_argument(
        "output_path",
        type=Path,
        nargs="?",
        help="Path to output. If not specified, perform a dry-run",
    )
    args = parser.parse_args()

    logging.basicConfig(
        format="%(funcName)-20s [%(levelname)s]:\t%(message)s",
        level=min(10, 30 - (args.verbose * 10)),
    )
    if args.verbose >= 2:
        sys.addaudithook(log_subprocess)

    if args.search_path:
        EXTRA_SEARCH_PATHS.extend([p.resolve() for p in args.search_path])

    fix_everything(args.input_path, args.output_path, verbose=args.verbose)


if __name__ == "__main__":
    main()
