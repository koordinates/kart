import functools

from kart.core import find_blobs_in_tree
from kart.base_dataset import BaseDataset
from kart.diff_structs import DatasetDiff, DeltaDiff
from kart.key_filters import DatasetKeyFilter, FeatureKeyFilter
from kart.lfs_util import (
    get_hash_and_size_of_file,
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
    TILE_PATH = "tile/"

    META_ITEMS = (
        BaseDataset.TITLE,
        BaseDataset.DESCRIPTION,
        BaseDataset.METADATA_XML,
        BaseDataset.SCHEMA_JSON,
        BaseDataset.CRS_DEFINITIONS,
    )

    @property
    def tile_tree(self):
        return self.get_subtree(self.TILE_PATH)

    def tile_pointer_blobs(self):
        """Returns a generator that yields every tile pointer blob in turn."""
        tile_tree = self.tile_tree
        if tile_tree:
            yield from find_blobs_in_tree(tile_tree)

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
        if rel_path.startswith("tile/"):
            return ("tile", self.decode_tile_path(rel_path))
        return super().decode_path(rel_path)

    def encode_tile_path(self, tilename, relative=False):
        """Given a tile's name, returns the path the tile's pointer should be written to."""
        tilename = tilename.rsplit("/")[-1]
        tile_prefix = hexhash(tilename)[0:2]
        rel_path = f"tile/{tile_prefix}/{tilename}"
        return rel_path if relative else self.ensure_full_path(rel_path)

    @classmethod
    def decode_tile_path(cls, tile_path):
        return tile_path.rsplit("/", maxsplit=1)[-1]

    def get_tile_summary_from_pointer_blob(self, tile_pointer_blob):
        # For now just return the blob contents as a string - it's a reasonable summary.
        result = pointer_file_to_json(
            tile_pointer_blob, {"name": tile_pointer_blob.name}
        )
        if "version" in result:
            del result["version"]
        return result

    def get_tile_summary_from_wc_path(self, wc_path):
        # For now just return the blob contents as a string - it's a reasonable summary.
        oid, size = get_hash_and_size_of_file(wc_path)
        return {"name": wc_path.name, "oid": f"sha256:{oid}", "size": size}

    def diff(self, other, ds_filter=DatasetKeyFilter.MATCH_ALL, reverse=False):
        """
        Generates a Diff from self -> other.
        If reverse is true, generates a diff from other -> self.
        """
        ds_diff = super().diff(other, ds_filter=ds_filter, reverse=reverse)
        tile_filter = ds_filter.get("tile", ds_filter.child_type())
        ds_diff["tile"] = DeltaDiff(self.diff_tile(other, tile_filter, reverse=reverse))
        return ds_diff

    def diff_tile(self, other, tile_filter=FeatureKeyFilter.MATCH_ALL, reverse=False):
        """
        Yields tile deltas from self -> other, but only for tile that match the tile_filter.
        If reverse is true, yields tile deltas from other -> self.
        """
        yield from self.diff_subtree(
            other,
            "tile",
            key_filter=tile_filter,
            key_decoder_method="decode_tile_path",
            value_decoder_method="get_tile_summary_promise_from_path",
            reverse=reverse,
        )

    def get_tile_summary_promise_from_path(self, tile_path):
        tile_pointer_blob = self.get_blob_at(tile_path)
        return functools.partial(
            self.get_tile_summary_from_pointer_blob, tile_pointer_blob
        )

    def diff_to_wc(self, wc_diff_context, ds_filter=DatasetKeyFilter.MATCH_ALL):
        ds_diff = DatasetDiff()
        tile_filter = ds_filter.get("tile", ds_filter.child_type())
        ds_diff["tile"] = DeltaDiff(self.diff_tile_to_wc(wc_diff_context, tile_filter))
        return ds_diff

    def diff_tile_to_wc(self, wc_diff_context, tile_filter):
        deltas = wc_diff_context.workdir_deltas_by_ds_path().get(self.path)
        if not deltas:
            return

        def encode_tile_path_relative(tilename):
            return self.encode_tile_path(tilename, relative=True)

        yield from self.decode_raw_deltas(
            deltas,
            tile_filter,
            old_path_decoder=encode_tile_path_relative,
            old_key_decoder=self.decode_tile_path,
            old_value_decoder=self.get_tile_summary_promise_from_path,
            # We don't need new_path_decoder - leave wc paths as-is
            new_key_decoder=self.decode_tile_path,
            new_value_decoder=self.get_tile_summary_promise_from_wc_path,
        )

    def get_tile_summary_promise_from_wc_path(self, wc_path):
        wc_path = self.repo.workdir_file(wc_path)
        return functools.partial(self.get_tile_summary_from_wc_path, wc_path)
