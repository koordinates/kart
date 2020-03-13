__all__ = ('is_frozen', 'is_linux', 'is_darwin', 'is_windows', 'spatialite_path', 'prefix')

import os
import platform
import sys

is_frozen = getattr(sys, 'frozen', None) and hasattr(sys, '_MEIPASS')
is_darwin = (platform.system() == "Darwin")
is_linux = (platform.system() == "Linux")
is_windows = (platform.system() == "Windows")

if is_darwin:
    libsuffix = 'dylib'
elif is_windows:
    libsuffix = 'dll'
else:
    libsuffix = 'so'

# sys.prefix is correctly set by virtualenv (development) & PyInstaller (release)
prefix = os.path.abspath(sys.prefix)

# Rtree / Libspatialindex
if not is_windows:
    os.environ['SPATIALINDEX_C_LIBRARY'] = os.path.join(
        prefix,
        "" if is_frozen else "lib",
        f"libspatialindex_c.{libsuffix}"
    )

spatialite_path = os.path.join(
    prefix,
    "" if (is_frozen or is_windows) else "lib",
    f"mod_spatialite"
)
if is_windows:
    # sqlite doesn't appear to like backslashes
    spatialite_path = spatialite_path.replace('\\', '/') 

os.environ['PATH'] = prefix + os.pathsep + os.path.join(prefix, 'bin') + os.pathsep + os.environ['PATH']

# Git
# https://git-scm.com/book/en/v2/Git-Internals-Environment-Variables
if is_windows:
    pass
else:
    os.environ['GIT_EXEC_PATH'] = os.path.join(prefix, 'libexec', 'git-core')
    os.environ['GIT_TEMPLATE_DIR'] = os.path.join(prefix, 'share', 'git-core', 'templates')
    os.environ['PREFIX'] = prefix

# GDAL Data
if not is_windows:
    os.environ['GDAL_DATA'] = os.path.join(prefix, 'share', 'gdal')
    os.environ['PROJ_DATA'] = os.path.join(prefix, 'share', 'proj')

# GDAL Error Handling
from osgeo import gdal, ogr
gdal.UseExceptions()
ogr.UseExceptions()

# Libgit2 TLS CA Certificates
# We build libgit2 to prefer the OS certificate store on Windows/macOS, but Linux doesn't have one.
if is_linux:
    import certifi
    import pygit2
    pygit2.settings.ssl_cert_file = certifi.where()
