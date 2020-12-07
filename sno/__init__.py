__all__ = (
    "is_frozen",
    "is_linux",
    "is_darwin",
    "is_windows",
    "spatialite_path",
    "prefix",
)

import os
import platform
import sys

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

# Rtree / Libspatialindex
if not is_windows:
    os.environ["SPATIALINDEX_C_LIBRARY"] = os.path.join(
        prefix, "" if is_frozen else "lib", f"libspatialindex_c.{libsuffix}"
    )

spatialite_path = os.path.join(
    prefix, "" if (is_frozen or is_windows) else "lib", f"mod_spatialite"
)
if is_windows:
    # sqlite doesn't appear to like backslashes
    spatialite_path = spatialite_path.replace("\\", "/")

os.environ["PATH"] = (
    prefix + os.pathsep + os.path.join(prefix, "bin") + os.pathsep + os.environ["PATH"]
)

# Git
# https://git-scm.com/book/en/v2/Git-Internals-Environment-Variables
os.environ["GIT_CONFIG_NOSYSTEM"] = "1"
os.environ["XDG_CONFIG_HOME"] = prefix
if is_windows:
    os.environ["PATH"] = (
        os.path.join(prefix, "git", "cmd") + os.pathsep + os.environ["PATH"]
    )
else:
    os.environ["GIT_EXEC_PATH"] = os.path.join(prefix, "libexec", "git-core")
    os.environ["GIT_TEMPLATE_DIR"] = os.path.join(
        prefix, "share", "git-core", "templates"
    )
# See LockedGitIndex in.repo.py:
os.environ["GIT_INDEX_FILE"] = os.path.join(".sno", "unlocked_index")

# GDAL Data
if not is_windows:
    os.environ["GDAL_DATA"] = os.path.join(prefix, "share", "gdal")
    os.environ["PROJ_LIB"] = os.path.join(prefix, "share", "proj")

# GDAL Error Handling
from osgeo import gdal, ogr, osr

gdal.UseExceptions()
ogr.UseExceptions()
osr.UseExceptions()

# Libgit2 TLS CA Certificates
# We build libgit2 to prefer the OS certificate store on Windows/macOS, but Linux doesn't have one.
if is_linux:
    import certifi
    import pygit2

    pygit2.settings.ssl_cert_file = certifi.where()
