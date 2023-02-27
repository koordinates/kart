from kart.point_cloud.tilename_util import remove_tile_extension as remove_pc_extension
from kart.point_cloud.tilename_util import (
    remove_tile_extension as remove_raster_extension,
)


def remove_any_tile_extension(filename):
    """Removes any kind of tile extension."""
    return remove_pc_extension(remove_raster_extension(filename))
