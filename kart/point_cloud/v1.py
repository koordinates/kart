import functools
import shutil
import os

from kart.base_dataset import BaseDataset
from kart.core import find_blobs_in_tree
from kart.decorators import allow_classmethod
from kart.diff_structs import DatasetDiff, DeltaDiff, Delta, KeyValue, WORKING_COPY_EDIT
from kart.key_filters import DatasetKeyFilter, FeatureKeyFilter
from kart.list_of_conflicts import ListOfConflicts, InvalidNewValue
from kart.lfs_util import (
    copy_file_to_local_lfs_cache,
    get_hash_and_size_of_file,
    get_hash_from_pointer_file,
    get_local_path_from_lfs_hash,
    pointer_file_bytes_to_dict,
    dict_to_pointer_file_bytes,
)
from kart import meta_items
from kart.meta_items import MetaItemDefinition, MetaItemFileType
from kart.progress_util import progress_bar
from kart.point_cloud.metadata_util import (
    RewriteMetadata,
    extract_pc_tile_metadata,
    rewrite_and_merge_metadata,
    format_tile_for_pointer_file,
    get_format_summary,
)
from kart.point_cloud.pdal_convert import convert_tile_to_format
from kart.point_cloud.tilename_util import (
    remove_tile_extension,
    set_tile_extension,
    get_tile_path_pattern,
)
from kart.serialise_util import hexhash
from kart.spatial_filter import SpatialFilter
from kart.working_copy import PartType


class PointCloudV1(BaseDataset):
    """A V1 point-cloud (LIDAR) dataset."""

    VERSION = 1
    DATASET_TYPE = "point-cloud"
    DATASET_DIRNAME = ".point-cloud-dataset.v1"

    ITEM_TYPE = "tile"

    WORKING_COPY_PART_TYPE = PartType.WORKDIR

    # All relative paths should be relative to self.inner_tree - that is, to the tree named DATASET_DIRNAME.
    TILE_PATH = "tile/"

    TITLE = meta_items.TITLE
    DESCRIPTION = meta_items.DESCRIPTION
    TAGS_JSON = meta_items.TAGS_JSON

    # Which tile format(s) this dataset requires / allows.
    FORMAT_JSON = MetaItemDefinition("format.json", MetaItemFileType.JSON)

    SCHEMA_JSON = meta_items.SCHEMA_JSON
    CRS_WKT = meta_items.CRS_WKT

    META_ITEMS = (
        TITLE,
        DESCRIPTION,
        TAGS_JSON,
        FORMAT_JSON,
        SCHEMA_JSON,
        CRS_WKT,
    )

    @property
    def tile_tree(self):
        return self.get_subtree(self.TILE_PATH)

    def _tile_pointer_blobs_and_dicts(
        self,
        spatial_filter=SpatialFilter.MATCH_ALL,
        show_progress=False,
        *,
        parse_pointer_dicts=True,
    ):
        """
        Returns a generator that yields every tile pointer blob in turn.
        Also yields the parsed pointer file as a dict, unless parse_pointer_dicts is False (then it yields None)
        """
        tile_tree = self.tile_tree
        if not tile_tree:
            return

        spatial_filter = spatial_filter.transform_for_dataset(self)

        n_read = 0
        n_matched = 0
        n_total = self.tile_count if show_progress else 0
        progress = progress_bar(
            show_progress=show_progress, total=n_total, unit="tile", desc=self.path
        )

        with progress as p:
            for blob in find_blobs_in_tree(tile_tree):
                n_read += 1
                tile_dict = None
                if parse_pointer_dicts:
                    tile_dict = pointer_file_bytes_to_dict(blob)
                if spatial_filter.matches(tile_dict if parse_pointer_dicts else blob):
                    n_matched += 1
                    yield blob, tile_dict

                p.update(1)

        if show_progress and not spatial_filter.match_all:
            p.write(
                f"(of {n_read} features read, wrote {n_matched} matching features to the working copy due to spatial filter)"
            )

    def tile_pointer_blobs(
        self, spatial_filter=SpatialFilter.MATCH_ALL, show_progress=False
    ):
        """
        Returns a generator that yields every tile pointer blob in turn.
        """
        for blob, _ in self._tile_pointer_blobs_and_dicts(
            spatial_filter=spatial_filter,
            show_progress=show_progress,
            parse_pointer_dicts=False,
        ):
            yield blob

    @property
    def tile_count(self):
        """The total number of features in this dataset."""
        return self.count_blobs_in_subtree(self.TILE_PATH)

    def tile_lfs_hashes(
        self, spatial_filter=SpatialFilter.MATCH_ALL, show_progress=False
    ):
        """Returns a generator that yields every LFS hash."""
        for blob in self.tile_pointer_blobs(
            spatial_filter=spatial_filter, show_progress=show_progress
        ):
            yield get_hash_from_pointer_file(blob)

    def tilenames_with_lfs_hashes(
        self,
        spatial_filter=SpatialFilter.MATCH_ALL,
        fix_extensions=True,
        show_progress=False,
    ):
        """
        Returns a generator that yields every tilename along with its LFS hash.
        If fix_extensions is True, then the returned name will be modified to have the correct extension for the
        type of tile the blob is pointing to (eg .laz or .copc.laz), regardless of the blob's extension (if any).
        """
        for blob, pointer_dict in self._tile_pointer_blobs_and_dicts(
            spatial_filter=spatial_filter, show_progress=show_progress
        ):
            if fix_extensions:
                tile_format = pointer_dict["format"]
                oid = pointer_dict["oid"].split(":", maxsplit=1)[1]
                yield set_tile_extension(blob.name, tile_format=tile_format), oid
            else:
                yield blob.name, get_hash_from_pointer_file(blob)

    def tilenames_with_lfs_paths(
        self,
        spatial_filter=SpatialFilter.MATCH_ALL,
        fix_extensions=True,
        show_progress=False,
    ):
        """Returns a generator that yields every tilename along with the path where the tile content is stored locally."""
        for blob_name, lfs_hash in self.tilenames_with_lfs_hashes(
            spatial_filter=spatial_filter,
            fix_extensions=fix_extensions,
            show_progress=show_progress,
        ):
            yield blob_name, get_local_path_from_lfs_hash(self.repo, lfs_hash)

    def decode_path(self, path):
        rel_path = self.ensure_rel_path(path)
        if rel_path.startswith("tile/"):
            return ("tile", self.tilename_from_path(rel_path))
        return super().decode_path(rel_path)

    @allow_classmethod
    def tilename_to_blob_path(self, tilename, relative=False):
        """Given a tile's name, returns the path the tile's pointer should be written to."""
        assert relative or isinstance(self, PointCloudV1)

        # Just in case it's a whole path, not just a name:
        tilename = self.tilename_from_path(tilename)
        tile_prefix = hexhash(tilename)[0:2]
        rel_path = f"tile/{tile_prefix}/{tilename}"
        return rel_path if relative else self.ensure_full_path(rel_path)

    def tilename_to_working_copy_path(self, tilename):
        # Just in case it's a whole path, not just a name.
        tilename = self.tilename_from_path(tilename)
        return f"{self.path}/{tilename}"

    @classmethod
    def tilename_from_path(cls, tile_path):
        return remove_tile_extension(os.path.basename(tile_path))

    @classmethod
    def get_tile_summary_from_pointer_blob(cls, tile_pointer_blob):
        result = pointer_file_bytes_to_dict(
            tile_pointer_blob, {"name": tile_pointer_blob.name}
        )
        result["name"] = set_tile_extension(
            result["name"], tile_format=result["format"]
        )
        if "version" in result:
            del result["version"]
        return result

    def get_tile_summary(
        self, tilename=None, *, path=None, pointer_blob=None, missing_ok=False
    ):
        """
        Gets the tile summary of the tile as committed in this dataset.
        Either tilename or path must be supplied - whichever is not supplied will be inferred from the other.
        If the pointer_blob is already known, this may be supplied too to avoid extra work.
        """
        if tilename is None and path is None:
            raise ValueError("Either <tilename> or <path> must be supplied")

        if not path:
            path = self.tilename_to_blob_path(tilename, relative=True)
        if not pointer_blob:
            pointer_blob = self.get_blob_at(path, missing_ok=missing_ok)
        if not pointer_blob:
            return None
        return self.get_tile_summary_from_pointer_blob(pointer_blob)

    def get_tile_summary_promise(
        self, tilename=None, *, path=None, pointer_blob=None, missing_ok=False
    ):
        """Same as get_tile_summary, but returns a promise. The blob data is not be read until the promise is called."""
        if tilename is None and path is None:
            raise ValueError("Either <tilename> or <path> must be supplied")

        if not path:
            path = self.tilename_to_blob_path(tilename, relative=True)
        if not pointer_blob:
            pointer_blob = self.get_blob_at(path, missing_ok=missing_ok)
        if not pointer_blob:
            return None
        return functools.partial(self.get_tile_summary_from_pointer_blob, pointer_blob)

    def get_tile_summary_promise_from_blob_path(self, path, *, missing_ok=False):
        return self.get_tile_summary_promise(path=path, missing_ok=missing_ok)

    def _workdir_path(self, wc_path):
        if isinstance(wc_path, str):
            return self.repo.workdir_file(wc_path)
        else:
            return wc_path

    def get_tile_summary_from_workdir_path(self, path, *, tile_metadata=None):
        """Generates a tile summary for a path to a tile in the working copy."""
        path = self._workdir_path(path)
        return self.get_tile_summary_from_filesystem_path(path)

    def get_tile_summary_from_filesystem_path(self, path, *, tile_metadata=None):
        """
        Generates a tile summary from a pathlib.Path for a file somewhere on the filesystem.
        If the tile_metadata is already known, this may be supplied too to avoid extra work.
        """
        if not tile_metadata:
            tile_metadata = extract_pc_tile_metadata(path)
        tile_info = format_tile_for_pointer_file(tile_metadata["tile"])
        oid, size = get_hash_and_size_of_file(path)
        return {"name": path.name, **tile_info, "oid": f"sha256:{oid}", "size": size}

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
            value_decoder_method="get_tile_summary_promise_from_blob_path",
            reverse=reverse,
        )

    def diff_to_working_copy(
        self,
        workdir_diff_cache,
        ds_filter=DatasetKeyFilter.MATCH_ALL,
        *,
        convert_to_dataset_format=False,
        skip_pdal=False,
    ):
        """
        Returns a diff of all changes made to this dataset in the working copy.

        convert_to_dataset_format - user wants this converted to dataset's format as it is
            committed, and wants to see diffs of what this would look like.
        skip_pdal - if set, don't run PDAL to check the tile contents. The resulting diffs
            are missing almost all of the info about the new tiles, but this is faster and more
            reliable if this information is not needed.
        """
        tile_filter = ds_filter.get("tile", ds_filter.child_type())

        current_metadata = self.tile_metadata
        dataset_format_to_apply = None
        if convert_to_dataset_format:
            dataset_format_to_apply = get_format_summary(current_metadata["format"])

        tilename_to_metadata = {}

        wc_tiles_path_pattern = get_tile_path_pattern(parent_path=self.path)

        tile_diff = DeltaDiff()

        for tile_path in workdir_diff_cache.dirty_paths_for_dataset(self):
            if not wc_tiles_path_pattern.fullmatch(tile_path):
                continue

            tilename = self.tilename_from_path(tile_path)
            if tilename not in tile_filter:
                continue

            old_tile_summary = self.get_tile_summary_promise(tilename, missing_ok=True)
            old_half_delta = (tilename, old_tile_summary) if old_tile_summary else None

            wc_path = self._workdir_path(tile_path)
            if not wc_path.is_file():
                new_half_delta = None
            elif skip_pdal:
                new_half_delta = tilename, {"name": wc_path.name}
            else:
                tile_metadata = extract_pc_tile_metadata(wc_path)
                tilename_to_metadata[wc_path.name] = tile_metadata
                new_tile_summary = self.get_tile_summary_from_workdir_path(
                    wc_path, tile_metadata=tile_metadata
                )

                if dataset_format_to_apply and not self.is_tile_compatible(
                    dataset_format_to_apply, new_tile_summary
                ):
                    new_tile_summary = self.pre_conversion_tile_summary(
                        dataset_format_to_apply, new_tile_summary
                    )

                new_half_delta = tilename, new_tile_summary

            tile_delta = Delta(old_half_delta, new_half_delta)
            tile_delta.flags = WORKING_COPY_EDIT
            tile_diff[tilename] = tile_delta

        if not tile_diff:
            return DatasetDiff()

        is_clean_slate = self.is_clean_slate(tile_diff)
        metadata_list = list(tilename_to_metadata.values())
        no_new_metadata = not metadata_list

        if not is_clean_slate:
            metadata_list.insert(0, current_metadata)

        rewrite_metadata = 0
        optimization_constraint = current_metadata["format"].get("optimization")
        if convert_to_dataset_format:
            rewrite_metadata = (
                RewriteMetadata.AS_IF_CONVERTED_TO_COPC
                if optimization_constraint == "copc"
                else RewriteMetadata.DROP_FORMAT
            )
        else:
            rewrite_metadata = (
                0
                if optimization_constraint == "copc"
                else RewriteMetadata.DROP_OPTIMIZATION
            )

        if no_new_metadata:
            merged_metadata = current_metadata
        else:
            merged_metadata = rewrite_and_merge_metadata(
                metadata_list, rewrite_metadata
            )
            if rewrite_metadata & RewriteMetadata.DROP_FORMAT:
                merged_metadata["format"] = current_metadata["format"]

        # Make it invalid to try and commit and LAS files:
        merged_format = merged_metadata["format"]
        if (
            not isinstance(merged_format, ListOfConflicts)
            and merged_format.get("compression") == "las"
        ):
            merged_format = InvalidNewValue([merged_format])
            merged_format.error_message = "Committing LAS tiles is not supported, unless you specify the --convert-to-dataset-format flag"
            merged_metadata["format"] = merged_format

        meta_diff = DeltaDiff()
        for key, ext in (("format", "json"), ("schema", "json"), ("crs", "wkt")):
            if current_metadata[key] != merged_metadata[key]:
                item_name = f"{key}.{ext}"
                meta_diff[item_name] = Delta.update(
                    KeyValue.of((item_name, current_metadata[key])),
                    KeyValue.of((item_name, merged_metadata[key])),
                )

        ds_diff = DatasetDiff()
        ds_diff["meta"] = meta_diff
        ds_diff["tile"] = tile_diff

        return ds_diff

    def is_tile_compatible(self, ds_format, tile_summary):
        tile_format = tile_summary["format"]
        if isinstance(ds_format, dict):
            ds_format = get_format_summary(ds_format)
        return tile_format == ds_format or tile_format.startswith(f"{ds_format}/")

    def pre_conversion_tile_summary(self, ds_format, tile_summary):
        """
        Converts a tile-summary - that is, updates the tile-summary to be a mix of the tiles current information
        (prefixed with "source") and its future information - what it will be once converted - where that is known.
        """
        if isinstance(ds_format, dict):
            ds_format = get_format_summary(ds_format)

        envisioned_summary = {
            "name": set_tile_extension(tile_summary["name"], tile_format=ds_format),
            "format": ds_format,
            "oid": None,
            "size": None,
        }
        result = {}
        for key, value in tile_summary.items():
            if envisioned_summary.get(key):
                result[key] = envisioned_summary[key]
            if key in envisioned_summary:
                result["source" + key[0].upper() + key[1:]] = value
            else:
                result[key] = value
        return result

    def is_clean_slate(self, tile_diff):
        num_existing_tiles_kept = self.tile_count
        for tile_delta in tile_diff.values():
            if tile_delta.type != "insert":
                num_existing_tiles_kept -= 1
        return num_existing_tiles_kept == 0

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
            self.apply_meta_diff(
                meta_diff,
                object_builder,
                resolve_missing_values_from_ds=resolve_missing_values_from_ds,
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
                    # TODO - need more work on normalising / matching names with different extensions

                    if delta.new_value.get("sourceFormat"):
                        # Converting and then committing a new tile
                        source_name = delta.new_value.get("sourceName")
                        path_in_wc = self._workdir_path(f"{self.path}/{source_name}")

                        conversion_func = functools.partial(
                            convert_tile_to_format,
                            target_format=delta.new_value["format"],
                        )
                        pointer_dict = copy_file_to_local_lfs_cache(
                            self.repo, path_in_wc, conversion_func
                        )
                        pointer_dict = format_tile_for_pointer_file(
                            delta.new_value, pointer_dict
                        )
                    else:
                        # Committing in a new tile, preserving its format
                        source_name = delta.new_value.get("name")
                        path_in_wc = self._workdir_path(f"{self.path}/{source_name}")
                        oid = delta.new_value["oid"]
                        path_in_lfs_cache = get_local_path_from_lfs_hash(self.repo, oid)
                        path_in_lfs_cache.parents[0].mkdir(parents=True, exist_ok=True)
                        shutil.copy(path_in_wc, path_in_lfs_cache)
                        pointer_dict = format_tile_for_pointer_file(delta.new_value)

                    tilename = delta.new_value["name"]
                    object_builder.insert(
                        self.tilename_to_blob_path(tilename, relative=True),
                        dict_to_pointer_file_bytes(pointer_dict),
                    )
                    # Update the diff to record what was stored - this is used to reset the workdir.
                    delta.new_value.update(
                        oid=pointer_dict["oid"], size=pointer_dict["size"]
                    )

                else:  # delete:
                    tilename = delta.old_key
                    object_builder.remove(
                        self.tilename_to_blob_path(tilename, relative=True)
                    )

    @property
    def tile_metadata(self):
        return {
            "format": self.get_meta_item("format.json"),
            "schema": self.get_meta_item("schema.json"),
            "crs": self.get_meta_item("crs.wkt"),
        }
