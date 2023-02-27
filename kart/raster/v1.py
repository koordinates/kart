from kart.tile.tile_dataset import TileDataset
from kart.raster.metadata_util import extract_raster_tile_metadata
from kart.raster.tilename_util import (
    remove_tile_extension,
    set_tile_extension,
)


class RasterV1(TileDataset):
    """A V1 raster dataset."""

    VERSION = 1
    DATASET_TYPE = "raster"
    DATASET_DIRNAME = ".raster-dataset.v1"

    @classmethod
    def remove_tile_extension(cls, filename):
        """Given a tile filename, removes the suffix .tif or .tiff"""
        return remove_tile_extension(filename)

    @classmethod
    def set_tile_extension(cls, filename, ext=None, tile_format=None):
        """Changes a tile's file extension to the given extension, or to the extension appropriate for its format."""
        return set_tile_extension(filename, ext=ext, tile_format=tile_format)

    @classmethod
    def extract_tile_metadata_from_filesystem_path(cls, path):
        return extract_raster_tile_metadata(path)
