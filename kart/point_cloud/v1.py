from kart.tile.tile_dataset import TileDataset
from kart.list_of_conflicts import ListOfConflicts, InvalidNewValue
from kart.point_cloud.metadata_util import (
    RewriteMetadata,
    extract_pc_tile_metadata,
    rewrite_and_merge_metadata,
    get_format_summary,
    is_copc,
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
    def remove_tile_extension(cls, filename, remove_pam_suffix=None):
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
    def get_format_summary(self, format_json):
        return get_format_summary(format_json)

    @classmethod
    def convert_tile_to_format(self, source_path, dest_path, target_format):
        convert_tile_to_format(source_path, dest_path, target_format)

    @classmethod
    def get_tile_path_pattern(
        cls,
        tilename=None,
        *,
        parent_path=None,
        include_conflict_versions=False,
        is_pam=None,
        ignore_tile_case=False,
    ):
        return get_tile_path_pattern(
            tilename,
            parent_path=parent_path,
            include_conflict_versions=include_conflict_versions,
            ignore_tile_case=ignore_tile_case,
        )

    @classmethod
    def write_mosaic_for_directory(cls, directory_path):
        # TODO: Not yet implemented
        pass

    def get_dirty_dataset_paths(self, workdir_diff_cache):
        # TODO - improve finding and handling of non-standard tile filenames.
        return workdir_diff_cache.dirty_paths_for_dataset(self)

    def rewrite_and_merge_metadata(
        self, current_metadata, metadata_list, convert_to_dataset_format
    ):
        optimization_constraint = current_metadata["format.json"].get("optimization")
        if optimization_constraint == "copc":
            rewrite_metadata = (
                RewriteMetadata.AS_IF_CONVERTED_TO_COPC
                if convert_to_dataset_format
                else RewriteMetadata.NO_REWRITE
            )
        else:
            rewrite_metadata = (
                RewriteMetadata.DROP_FORMAT
                if convert_to_dataset_format == "copc"
                else RewriteMetadata.DROP_OPTIMIZATION
            )
        return rewrite_and_merge_metadata(metadata_list, rewrite_metadata)

    def check_merged_metadata(
        self, current_metadata, merged_metadata, convert_to_dataset_format=None
    ):
        super().check_merged_metadata(
            current_metadata, merged_metadata, convert_to_dataset_format
        )

        merged_format = merged_metadata["format.json"]

        def _ensure_list(arg):
            return arg if isinstance(arg, list) else [arg]

        def _ensure_error_value(arg):
            return arg if isinstance(arg, ListOfConflicts) else InvalidNewValue([arg])

        # The user can't commit LAS files at all unless they use --convert-to-dataset-format.
        if any(m.get("compression") == "las" for m in _ensure_list(merged_format)):
            merged_format = _ensure_error_value(merged_format)
            merged_format.error_message = "Committing LAS tiles is not supported, unless you specify the --convert-to-dataset-format flag"
            merged_metadata["format.json"] = merged_format

    def simplify_diff_for_dropping_cloud_optimized(self, current_format, merged_format):
        if (
            current_format.get("optimization") == "copc"
            and isinstance(merged_format, ListOfConflicts)
            and len(merged_format) == 2
        ):
            format_list = [current_format, merged_format[0], merged_format[1]]
            simplified_list = [
                {k: v for k, v in f.items() if not k.startswith("optimization")}
                for f in format_list
            ]
            if all(f == simplified_list[0] for f in simplified_list):
                return simplified_list[0]
        return merged_format

    def is_cloud_optimized(self, format_json=None):
        if format_json is None:
            format_json = self.get_meta_item("format.json")
        return is_copc(format_json)
