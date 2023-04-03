from pathlib import Path
import re

# Allowed pattern for the the basename part of a tile's filename.
# We don't allow tilenames to start with a "." since these are considered to be hidden -
# and GDAL sometimes creates temporary files alongside TIF files that start with a "." and are best ignored.
TILE_BASENAME_PATTERN = r"([^/.][^/]*)"


def remove_any_tile_extension(filename):
    """Removes any kind of tile extension."""

    from kart.point_cloud.tilename_util import (
        remove_tile_extension as remove_pc_extension,
    )
    from kart.raster.tilename_util import (
        remove_tile_extension as remove_raster_extension,
    )

    orig_len = len(filename)
    for func in (remove_pc_extension, remove_raster_extension):
        filename = func(filename)
        if len(filename) != orig_len:
            return filename
    return filename


def case_insensitive(name):
    """
    Given a name eg "baz", returns a case-insensitive version that can be understood by pathlib.Path.glob
    - eg "[Bb][Aa][Zz]"
    """
    return re.sub(
        "[A-Za-z]", lambda m: f"[{m.group().upper()}{m.group().lower()}]", name
    )


def find_similar_files_case_insensitive(path):
    """
    Given the path to a particular file (which need not exist), finds all files in the same
    directory which have the same name (case-insensitive) as that file.
    Eg, given /foo/bar/baz, will find [/foo/bar/baz, /foo/bar/BAZ, /foo/bar/Baz], if those 3
    files exist.
    """
    path = Path(path)
    return list(path.parent.glob(case_insensitive(path.name)))
