__all__ = (
    "is_frozen",
    "is_linux",
    "is_darwin",
    "is_windows",
    "spatialite_path",
    "prefix",
)

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

if _kart_env:
    spatialite_path = os.path.splitext(_kart_env.SPATIALITE_EXTENSION)[0]
else:
    spatialite_path = os.path.join(
        prefix, "" if (is_frozen or is_windows) else "lib", f"mod_spatialite"
    )
if is_windows:
    # sqlite doesn't appear to like backslashes
    spatialite_path = spatialite_path.replace("\\", "/")

# $PATH is used for DLL lookups on Windows
path_extras = [prefix]

# Git
# https://git-scm.com/book/en/v2/Git-Internals-Environment-Variables
os.environ["GIT_CONFIG_NOSYSTEM"] = "1"
os.environ["XDG_CONFIG_HOME"] = prefix
if _kart_env:
    path_extras.append(os.path.split(_kart_env.GIT_EXECUTABLE)[0])
elif is_windows:
    path_extras.append(os.path.join(prefix, "git", "cmd"))
else:
    path_extras.append(os.path.join(prefix, "bin"))

os.environ["GIT_EXEC_PATH"] = os.path.join(prefix, "libexec", "git-core")
os.environ["GIT_TEMPLATE_DIR"] = os.path.join(prefix, "share", "git-core", "templates")
# See locked_git_index in.repo.py:
os.environ["GIT_INDEX_FILE"] = os.path.join(".kart", "unlocked_index")

# GDAL Data
if _kart_env:
    os.environ["GDAL_DATA"] = _kart_env.GDAL_DATA
    os.environ["PROJ_LIB"] = _kart_env.PROJ_LIB
else:
    if is_windows:
        if prefix.endswith("venv"):
            data_prefix = os.path.join(prefix, "Lib", "site-packages", "osgeo", "data")
        else:
            data_prefix = os.path.join(prefix, "osgeo", "data")
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

# GDAL Error Handling
from osgeo import gdal, ogr, osr

gdal.UseExceptions()
ogr.UseExceptions()
osr.UseExceptions()

if is_linux:
    import certifi

    os.environ["SSL_CERT_FILE"] = certifi.where()

# Libgit2 options
import pygit2

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
        L.warn("Error calling SetConsoleCtrlHandler: ", ctypes.get_last_error())


def _configure_process_cleanup_nonwindows():
    # On non-Windows we can use os.setsid() which Windows lacks,
    # to attempt to give Kart it's own process group ID (PGID)
    if "_KART_PGID_SET" not in os.environ and os.getpid() != os.getpgrp():
        try:
            os.setsid()
            # No need to do this again for any Kart subprocess of this Kart process.
            os.environ["_KART_PGID_SET"] = "1"
        except OSError as e:
            L.warn("Error setting Kart PGID - os.setsid() failed.", e)

    # If Kart now has its own PGID, which its children share - we want to SIGTERM that when Kart exits.
    if os.getpid() == os.getpgrp():
        import signal

        _kart_process_group_killed = False

        def _cleanup_process_group(signum, stack_frame):
            nonlocal _kart_process_group_killed
            if _kart_process_group_killed:
                return
            _kart_process_group_killed = True
            os.killpg(os.getpid(), signum)
            sys.exit(128 + signum)

        signal.signal(signal.SIGTERM, _cleanup_process_group)
        signal.signal(signal.SIGINT, _cleanup_process_group)


if "NO_CONFIGURE_PROCESS_CLEANUP" not in os.environ:
    if is_windows:
        _configure_process_cleanup_windows()
    else:
        _configure_process_cleanup_nonwindows()
