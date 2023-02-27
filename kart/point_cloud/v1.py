import functools

from kart.tile.tile_dataset import TileDataset
from kart.diff_structs import DatasetDiff, DeltaDiff, Delta, KeyValue, WORKING_COPY_EDIT
from kart.key_filters import DatasetKeyFilter
from kart.list_of_conflicts import ListOfConflicts, InvalidNewValue
from kart.lfs_util import (
    copy_file_to_local_lfs_cache,
    dict_to_pointer_file_bytes,
    merge_pointer_file_dicts,
)
from kart.point_cloud.metadata_util import (
    RewriteMetadata,
    extract_pc_tile_metadata,
    rewrite_and_merge_metadata,
    get_format_summary,
)
from kart.point_cloud.pdal_convert import convert_tile_to_format
from kart.point_cloud.tilename_util import (
    remove_tile_extension,
    set_tile_extension,
    get_tile_path_pattern,
)


class PointCloudV1(TileDataset):
    """A V1 point-cloud (LIDAR) dataset."""

    VERSION = 1
    DATASET_TYPE = "point-cloud"
    DATASET_DIRNAME = ".point-cloud-dataset.v1"

    # Much of the implementation is common to all tile datasets - found in TileDataset.

    @classmethod
    def remove_tile_extension(cls, filename):
        """Given a tile filename, removes the suffix .las or .laz or .copc.las or .copc.laz"""
        return remove_tile_extension(filename)

    @classmethod
    def set_tile_extension(cls, filename, ext=None, tile_format=None):
        """Changes a tile's file extension to the given extension, or to the extension appropriate for its format."""
        return set_tile_extension(filename, ext=ext, tile_format=tile_format)

    @classmethod
    def extract_tile_metadata_from_filesystem_path(cls, path):
        return extract_pc_tile_metadata(path)

    @classmethod
    def get_tile_path_pattern(
        cls, tilename=None, *, parent_path=None, include_conflict_versions=False
    ):
        return get_tile_path_pattern(
            tilename,
            parent_path=parent_path,
            include_conflict_versions=include_conflict_versions,
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
                        pointer_dict = merge_pointer_file_dicts(
                            delta.new_value, pointer_dict
                        )
                    else:
                        # Committing in a new tile, preserving its format
                        source_name = delta.new_value.get("name")
                        path_in_wc = self._workdir_path(f"{self.path}/{source_name}")
                        oid_and_size = delta.new_value["oid"], delta.new_value["size"]
                        pointer_dict = copy_file_to_local_lfs_cache(
                            self.repo, path_in_wc, oid_and_size=oid_and_size
                        )
                        pointer_dict = merge_pointer_file_dicts(
                            delta.new_value, pointer_dict
                        )
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
