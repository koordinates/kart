from kart.core import find_blobs_in_tree
from kart.base_dataset import BaseDataset
from kart.diff_structs import DeltaDiff
from kart.key_filters import DatasetKeyFilter, FeatureKeyFilter
from kart.lfs_util import (
    get_hash_from_pointer_file,
    get_local_path_from_lfs_hash,
    pointer_file_to_json,
)
from kart.serialise_util import hexhash


class PointCloudV1(BaseDataset):
    """A V1 point-cloud (LIDAR) dataset."""

    VERSION = 1
    DATASET_TYPE = "point-cloud"
    DATASET_DIRNAME = ".point-cloud-dataset.v1"

    # All relative paths should be relative to self.inner_tree - that is, to the tree named DATASET_DIRNAME.
    TILES_PATH = "tiles/"

    META_ITEMS = (
        BaseDataset.TITLE,
        BaseDataset.DESCRIPTION,
        BaseDataset.METADATA_XML,
        BaseDataset.SCHEMA_JSON,
        BaseDataset.CRS_DEFINITIONS,
    )

    @property
    def tiles_tree(self):
        return self.get_subtree(self.TILES_PATH)

    def tile_pointer_blobs(self):
        """Returns a generator that yields every tile pointer blob in turn."""
        tiles_tree = self.tiles_tree
        if tiles_tree:
            yield from find_blobs_in_tree(tiles_tree)

    def tilenames_with_lfs_hashes(self):
        """Returns a generator that yields every tilename along with its LFS hash."""
        for blob in self.tile_pointer_blobs():
            yield blob.name, get_hash_from_pointer_file(blob)

    def tilenames_with_lfs_paths(self):
        """Returns a generator that yields every tilename along with the path where the tile content is stored locally."""
        for blob_name, lfs_hash in self.tilenames_with_lfs_hashes():
            yield blob_name, get_local_path_from_lfs_hash(self.repo, lfs_hash)

    def decode_path(self, path):
        rel_path = self.ensure_rel_path(path)
        if rel_path.startswith("tiles/"):
            return ("tiles", self.decode_tile_path(rel_path))
        return super().decode_path(rel_path)

    def encode_tile_path(self, tilename, relative=False, *, schema=None):
        """Given a tile's name, returns the path the tile's pointer should be written to."""
        tile_prefix = hexhash(tilename)[0:2]
        rel_path = f"tiles/{tile_prefix}/{tilename}"
        return rel_path if relative else self.ensure_full_path(rel_path)

    @classmethod
    def decode_tile_path(cls, tile_path):
        return tile_path.rsplit("/", maxsplit=1)[-1]

    def diff(self, other, ds_filter=DatasetKeyFilter.MATCH_ALL, reverse=False):
        """
        Generates a Diff from self -> other.
        If reverse is true, generates a diff from other -> self.
        """
        ds_diff = super().diff(other, ds_filter=ds_filter, reverse=reverse)
        tiles_filter = ds_filter.get("tiles", ds_filter.child_type())
        ds_diff["tiles"] = DeltaDiff(
            self.diff_tiles(other, tiles_filter, reverse=reverse)
        )
        return ds_diff

    def diff_tiles(self, other, tiles_filter=FeatureKeyFilter.MATCH_ALL, reverse=False):
        """
        Yields tiles deltas from self -> other, but only for tiles that match the tiles_filter.
        If reverse is true, yields tiles deltas from other -> self.
        """
        yield from self.diff_subtree(
            other,
            "tiles",
            key_filter=tiles_filter,
            key_decoder_method="decode_tile_path",
            value_decoder_method="get_tile_summary_from_pointer_blob",
            reverse=reverse,
        )

    def get_tile_summary_from_pointer_blob(self, tile_pointer_blob):
        # For now just return the blob contents as a string - it's a reasonable summary.
        result = pointer_file_to_json(tile_pointer_blob)
        if "version" in result:
            del result["version"]
        return result
