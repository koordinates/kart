__all__ = ("is_frozen", "is_linux", "is_darwin", "spatialite_path", "prefix")

import os
import platform
import sys

is_frozen = getattr(sys, "frozen", None) and hasattr(sys, "_MEIPASS")
is_darwin = platform.system() == "Darwin"
is_linux = platform.system() == "Linux"
libsuffix = "dylib" if is_darwin else "so"

# sys.prefix is correctly set by virtualenv (development) & PyInstaller (release)
prefix = os.path.abspath(sys.prefix)

# Rtree / Libspatialindex
os.environ["SPATIALINDEX_C_LIBRARY"] = os.path.join(
    prefix, "" if is_frozen else "lib", f"libspatialindex_c.{libsuffix}"
)

spatialite_path = os.path.join(prefix, "" if is_frozen else "lib", f"mod_spatialite")

# Git
# https://git-scm.com/book/en/v2/Git-Internals-Environment-Variables
os.environ["GIT_EXEC_PATH"] = os.path.join(prefix, "libexec", "git-core")
os.environ["GIT_TEMPLATE_DIR"] = os.path.join(prefix, "share", "git-core", "templates")
os.environ["PREFIX"] = prefix
os.environ["PATH"] = (
    prefix + os.pathsep + os.path.join(prefix, "bin") + os.pathsep + os.environ["PATH"]
)

# GDAL Data
os.environ["GDAL_DATA"] = os.path.join(prefix, "share", "gdal")
os.environ["PROJ_DATA"] = os.path.join(prefix, "share", "proj")

# GDAL Error Handling
from osgeo import gdal, ogr

gdal.UseExceptions()
ogr.UseExceptions()

# Libgit2 TLS CA Certificates
import certifi
import pygit2

try:
    # this doesn't work on all platforms/security backends (eg. Security.Framework on macOS)
    pygit2.settings.ssl_cert_file = certifi.where()
except pygit2.GitError:
    pass
