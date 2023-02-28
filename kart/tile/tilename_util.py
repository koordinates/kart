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
