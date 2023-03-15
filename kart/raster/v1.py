from kart.diff_structs import DatasetDiff, DeltaDiff, Delta, KeyValue, WORKING_COPY_EDIT
from kart.key_filters import DatasetKeyFilter
from kart.tile.tile_dataset import TileDataset
from kart.raster.metadata_util import (
    extract_raster_tile_metadata,
    rewrite_and_merge_metadata,
)
from kart.raster.tilename_util import (
    get_tile_path_pattern,
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
        extract_metadata=True,
    ):
        """
        Returns a diff of all changes made to this dataset in the working copy.

        convert_to_dataset_format - user wants this converted to dataset's format as it is
            committed, and wants to see diffs of what this would look like.
        extract_metadata - if False, don't run gdal.Info to check the tile contents. The resulting diffs
            are missing almost all of the info about the new tiles, but this is faster and more
            reliable if this information is not needed.
        """
        if convert_to_dataset_format:
            raise NotImplementedError(
                "Sorry, convert_to_dataset_format is not yet implemented for raster datasets"
            )

        tile_filter = ds_filter.get("tile", ds_filter.child_type())

        current_metadata = self.tile_metadata
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
            elif extract_metadata:
                tile_metadata = extract_raster_tile_metadata(wc_path)
                tilename_to_metadata[wc_path.name] = tile_metadata
                new_tile_summary = self.get_tile_summary_from_workdir_path(
                    wc_path, tile_metadata=tile_metadata
                )

                new_half_delta = tilename, new_tile_summary
            else:
                new_half_delta = tilename, {"name": wc_path.name}

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

        if no_new_metadata:
            merged_metadata = current_metadata
        else:
            merged_metadata = rewrite_and_merge_metadata(metadata_list)

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