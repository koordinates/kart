__all__ = (
    "is_frozen",
    "is_linux",
    "is_darwin",
    "is_windows",
    "spatialite_path",
    "prefix",
    "git_bin_path",
    "package_data_path",
)

import importlib
import logging
import os
import platform
import sys

L = logging.getLogger("kart.__init__")

try:
    import _kart_env

    L.debug("Found _kart_env configuration module")
except ImportError:
    L.debug("No _kart_env configuration module found")
    _kart_env = None

is_frozen = getattr(sys, "frozen", None) and hasattr(sys, "_MEIPASS")
is_darwin = platform.system() == "Darwin"
is_linux = platform.system() == "Linux"
is_windows = platform.system() == "Windows"

if is_darwin:
    libsuffix = "dylib"
elif is_windows:
    libsuffix = "dll"
else:
    libsuffix = "so"

# sys.prefix is correctly set by virtualenv (development) & PyInstaller (release)
prefix = os.path.abspath(sys.prefix)

if is_frozen:
    package_data_path = os.path.join(prefix, "share", "kart")
else:
    package_data_path = os.path.split(__file__)[0]


def _env_path(path):
    p = os.path.normpath(path)
    return p if os.path.isabs(p) else os.path.join(prefix, p)


if _kart_env:
    spatialite_path = os.path.splitext(_env_path(_kart_env.SPATIALITE_EXTENSION))[0]
else:
    spatialite_path = os.path.join(
        prefix, "" if is_frozen else "lib", f"mod_spatialite"
    )
if is_windows:
    # sqlite doesn't appear to like backslashes
    spatialite_path = spatialite_path.replace("\\", "/")

# $PATH is used for DLL lookups on Windows
path_extras = [prefix]
if not is_frozen:
    path_extras += [os.path.join(prefix, "scripts"), os.path.join(prefix, "lib")]

if is_linux:
    import certifi

    os.environ["SSL_CERT_FILE"] = certifi.where()
    # affects PDAL's libcurl, and possibly other libcurls
    os.environ["CURL_CA_INFO"] = certifi.where()

# Git
# https://git-scm.com/book/en/v2/Git-Internals-Environment-Variables
os.environ["GIT_CONFIG_NOSYSTEM"] = "1"
os.environ["XDG_CONFIG_HOME"] = prefix
if _kart_env:
    git_bin_path = os.path.split(_env_path(_kart_env.GIT_EXECUTABLE))[0]
elif is_windows:
    git_bin_path = os.path.join(prefix, "git", "cmd")
else:
    git_bin_path = os.path.join(prefix, "bin")
path_extras.append(os.path.normpath(git_bin_path))

# TODO - consider adding more of this to _kart_env.
if is_windows:
    os.environ["GIT_EXEC_PATH"] = os.path.join(
        prefix, "git", "mingw64", "libexec", "git-core"
    )
    os.environ["GIT_TEMPLATE_DIR"] = os.path.join(
        prefix, "git", "mingw64", "share", "git-core", "templates"
    )
    path_extras.append(os.path.join(prefix, "git", "usr", "bin"))
else:
    os.environ["GIT_EXEC_PATH"] = os.path.join(prefix, "libexec", "git-core")
    os.environ["GIT_TEMPLATE_DIR"] = os.path.join(
        prefix, "share", "git-core", "templates"
    )

# See locked_git_index in.repo.py:
os.environ["GIT_INDEX_FILE"] = os.path.join(".kart", "unlocked_index")

# GDAL Data
if _kart_env:
    os.environ["GDAL_DATA"] = _env_path(_kart_env.GDAL_DATA)
    os.environ["PROJ_LIB"] = _env_path(_kart_env.PROJ_LIB)
else:
    data_prefix = os.path.join(prefix, "share")
    os.environ["GDAL_DATA"] = os.path.join(data_prefix, "gdal")
    os.environ["PROJ_LIB"] = os.path.join(data_prefix, "proj")

if not is_windows:
    # TODO: something's wrong with proj networking in our windows library
    os.environ.setdefault("PROJ_NETWORK", "ON")

# GPKG optimisation:
if "OGR_SQLITE_PRAGMA" not in os.environ:
    os.environ["OGR_SQLITE_PRAGMA"] = "page_size=65536"

# Write our various additions to $PATH
os.environ["PATH"] = (
    os.pathsep.join(path_extras) + os.pathsep + os.environ.get("PATH", "")
)
if is_windows:
    os.add_dll_directory(prefix if is_frozen else os.path.join(prefix, "lib"))
    # FIXME: git2.dll is in the package directory, but isn't setup for ctypes to use
    _pygit2_spec = importlib.util.find_spec("pygit2")
    os.add_dll_directory(_pygit2_spec.submodule_search_locations[0])

# Make sure our SQLite3 build is loaded before Python stdlib one
import pysqlite3  # noqa

# GDAL Error Handling
from osgeo import gdal, ogr, osr  # noqa

gdal.UseExceptions()
ogr.UseExceptions()
osr.UseExceptions()


# Libgit2 options
import pygit2  # noqa

pygit2.option(pygit2.GIT_OPT_ENABLE_STRICT_HASH_VERIFICATION, 0)

# By default, libgit2 caches tree object reads (up to 4K trees).
# However, since Kart stores features in a 256x256 tree structure,
# large repos will have (65536+256) trees.
# Increasing this limit above that number increases import performance dramatically.
# (2 here is the value of `GIT_OBJECT_TREE` constant, pygit2 doesn't expose it)
pygit2.option(pygit2.GIT_OPT_SET_CACHE_OBJECT_LIMIT, 2, 100000)

# Libgit2 TLS CA Certificates
# We build libgit2 to prefer the OS certificate store on Windows/macOS, but Linux doesn't have one.
if is_linux:
    import certifi

    # note: cli_util.py also sets this in git's `http.sslCAInfo` config var
    pygit2.settings.ssl_cert_file = certifi.where()


# If Kart is aborted, also abort all child processes.


def _configure_process_cleanup_windows():
    import ctypes

    # On Windows, the calling process is responsible for giving Kart its own process group ID -
    # which is recommended - but we can't do anything about if they haven't done it properly.
    # But, we do need to handle CTRL-C events - the call below "restores normal processing of CTRL-C events"
    # (ie,  don't ignore them) and this is also inherited by child processes.
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    if not kernel32.SetConsoleCtrlHandler(None, False):
        L.warning("Error calling SetConsoleCtrlHandler: %s", ctypes.get_last_error())


def _configure_process_cleanup_nonwindows():
    # On non-Windows we can use os.setsid() which Windows lacks,
    # to attempt to give Kart it's own process group ID (PGID)
    if "_KART_PGID_SET" not in os.environ and os.getpid() != os.getpgrp():
        try:
            os.setsid()
            # No need to do this again for any Kart subprocess of this Kart process.
            os.environ["_KART_PGID_SET"] = "1"
        except OSError as e:
            L.warning("Error setting Kart PGID - os.setsid() failed. %s", e)

    # If Kart now has its own PGID, which its children share - we want to SIGTERM that when Kart exits.
    if os.getpid() == os.getpgrp():
        import signal

        _kart_process_group_killed = False

        def _cleanup_process_group(signum, stack_frame):
            nonlocal _kart_process_group_killed
            if _kart_process_group_killed:
                return
            _kart_process_group_killed = True
            try:
                os.killpg(0, signum)
            except Exception:
                pass
            sys.exit(128 + signum)

        signal.signal(signal.SIGTERM, _cleanup_process_group)
        signal.signal(signal.SIGINT, _cleanup_process_group)


if "NO_CONFIGURE_PROCESS_CLEANUP" not in os.environ:
    if is_windows:
        _configure_process_cleanup_windows()
    else:
        _configure_process_cleanup_nonwindows()
