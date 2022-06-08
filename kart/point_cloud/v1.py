import functools
import re
import shutil

from kart.core import find_blobs_in_tree
from kart.base_dataset import BaseDataset
from kart.diff_structs import DatasetDiff, DeltaDiff
from kart.exceptions import NotYetImplemented
from kart.key_filters import DatasetKeyFilter, FeatureKeyFilter
from kart.lfs_util import (
    get_hash_and_size_of_file,
    get_hash_from_pointer_file,
    get_local_path_from_lfs_hash,
    pointer_file_bytes_to_dict,
    dict_to_pointer_file_bytes,
)
from kart.serialise_util import hexhash
from kart.working_copy import PartType


class PointCloudV1(BaseDataset):
    """A V1 point-cloud (LIDAR) dataset."""

    VERSION = 1
    DATASET_TYPE = "point-cloud"
    DATASET_DIRNAME = ".point-cloud-dataset.v1"

    WORKING_COPY_PART_TYPE = PartType.WORKDIR

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
            return ("tile", self.tilename_from_path(rel_path))
        return super().decode_path(rel_path)

    def tilename_to_blob_path(self, tilename, relative=False):
        """Given a tile's name, returns the path the tile's pointer should be written to."""
        tilename = self.tilename_from_path(
            tilename
        )  # Just in case it's a whole path, not just a name.
        tile_prefix = hexhash(tilename)[0:2]
        rel_path = f"tile/{tile_prefix}/{tilename}"
        return rel_path if relative else self.ensure_full_path(rel_path)

    def tilename_to_working_copy_path(self, tilename):
        # Just in case it's a whole path, not just a name.
        tilename = self.tilename_from_path(tilename)
        return f"{self.path}/{tilename}"

    @classmethod
    def tilename_from_path(cls, tile_path):
        return tile_path.rsplit("/", maxsplit=1)[-1]

    def get_tile_summary_from_pointer_blob(self, tile_pointer_blob):
        result = pointer_file_bytes_to_dict(
            tile_pointer_blob, {"name": tile_pointer_blob.name}
        )
        if "version" in result:
            del result["version"]
        return result

    def get_tile_summary_from_wc_path(self, wc_path):
        from kart.point_cloud.import_ import (
            extract_pc_tile_metadata,
            pc_tile_metadata_to_pointer_metadata,
        )

        metadata = pc_tile_metadata_to_pointer_metadata(
            extract_pc_tile_metadata(wc_path)
        )

        oid, size = get_hash_and_size_of_file(wc_path)
        return {"name": wc_path.name, **metadata, "oid": f"sha256:{oid}", "size": size}

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
            key_decoder_method="tilename_from_path",
            value_decoder_method="get_tile_summary_promise_from_path",
            reverse=reverse,
        )

    def get_tile_summary_promise_from_path(self, tile_path):
        tile_pointer_blob = self.get_blob_at(tile_path)
        return functools.partial(
            self.get_tile_summary_from_pointer_blob, tile_pointer_blob
        )

    def diff_to_working_copy(
        self, wc_diff_context, ds_filter=DatasetKeyFilter.MATCH_ALL
    ):
        """Returns a diff of all changes made to this dataset in the working copy."""
        ds_diff = DatasetDiff()
        tile_filter = ds_filter.get("tile", ds_filter.child_type())
        ds_diff["tile"] = DeltaDiff(
            self.diff_tile_to_working_copy(wc_diff_context, tile_filter)
        )
        return ds_diff

    def diff_tile_to_working_copy(self, wc_diff_context, tile_filter):
        """Yields deltas of all the changes the user has made to tiles in the working copy."""

        # Dataset-paths have a different structure to workdir paths - the workdir index will have only workdir paths,
        # and we need to find the related dataset paths.
        def wc_to_ds_path_transform(wc_path):
            return self.tilename_to_blob_path(wc_path, relative=True)

        wc_tiles_path_pattern = re.escape(f"{self.path}/")
        wc_tile_ext_pattern = re.escape(".laz")
        wc_tiles_pattern = re.compile(
            rf"^{wc_tiles_path_pattern}[^/]+{wc_tile_ext_pattern}$"
        )

        yield from self.generate_wc_diff_from_workdir_index(
            wc_diff_context,
            wc_path_filter_pattern=wc_tiles_pattern,
            key_filter=tile_filter,
            wc_to_ds_path_transform=wc_to_ds_path_transform,
            ds_key_decoder=self.tilename_from_path,
            wc_key_decoder=self.tilename_from_path,
            ds_value_decoder=self.get_tile_summary_promise_from_path,
            wc_value_decoder=self.get_tile_summary_promise_from_wc_path,
        )

    def get_tile_summary_promise_from_wc_path(self, wc_path):
        wc_path = self.repo.workdir_file(wc_path)
        return functools.partial(self.get_tile_summary_from_wc_path, wc_path)

    def apply_diff(
        self, dataset_diff, object_builder, *, resolve_missing_values_from_ds=None
    ):
        """
        Given a diff that only affects this dataset, write it to the given treebuilder.
        Blobs will be created in the repo, and referenced in the resulting tree, but
        no commit is created - this is the responsibility of the caller.
        """
        meta_diff = dataset_diff.get("meta")
        if meta_diff:
            raise NotYetImplemented(
                "Sorry, committing meta diffs for point cloud datasets is not yet supported"
            )

        tile_diff = dataset_diff.get("tile")
        if tile_diff:
            self.apply_tile_diff(
                tile_diff,
                object_builder,
                resolve_missing_values_from_ds=resolve_missing_values_from_ds,
            )

    def apply_tile_diff(
        self, tile_diff, object_builder, *, resolve_missing_values_from_ds=None
    ):
        lfs_objects_path = self.repo.gitdir_path / "lfs" / "objects"
        lfs_tmp_path = lfs_objects_path / "tmp"
        lfs_tmp_path.mkdir(parents=True, exist_ok=True)

        with object_builder.chdir(self.inner_path):
            for delta in tile_diff.values():
                if delta.type in ("insert", "update"):
                    tilename = delta.new_key
                    path_in_wc = self.repo.workdir_file(f"{self.path}/{tilename}")
                    assert path_in_wc.is_file()

                    oid = delta.new_value["oid"]
                    actual_object_path = get_local_path_from_lfs_hash(self.repo, oid)
                    actual_object_path.parents[0].mkdir(parents=True, exist_ok=True)
                    shutil.copy(path_in_wc, actual_object_path)

                    object_builder.insert(
                        self.tilename_to_blob_path(tilename, relative=True),
                        dict_to_pointer_file_bytes(delta.new_value),
                    )

                else:  # delete:
                    tilename = delta.old_key
                    object_builder.remove(
                        self.tilename_to_blob_path(tilename, relative=True)
                    )
