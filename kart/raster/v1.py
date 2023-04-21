import functools
import re

from kart.core import all_blobs_in_tree
from kart.diff_structs import DatasetDiff, DeltaDiff, Delta, WORKING_COPY_EDIT
from kart.key_filters import DatasetKeyFilter, FeatureKeyFilter
from kart.lfs_util import (
    copy_file_to_local_lfs_cache,
    dict_to_pointer_file_bytes,
    merge_pointer_file_dicts,
)
from kart.meta_items import MetaItemDefinition, MetaItemFileType
from kart.raster.gdal_convert import convert_tile_to_format
from kart.raster.metadata_util import (
    RewriteMetadata,
    extract_raster_tile_metadata,
    rewrite_and_merge_metadata,
    get_format_summary,
)
from kart.raster.tilename_util import (
    get_tile_path_pattern,
    remove_tile_extension,
    set_tile_extension,
)
from kart.tile.tile_dataset import TileDataset
from kart.tile.tilename_util import (
    find_similar_files_case_insensitive,
    PAM_SUFFIX,
    LEN_PAM_SUFFIX,
)


class RasterV1(TileDataset):
    """A V1 raster dataset."""

    VERSION = 1
    DATASET_TYPE = "raster"
    DATASET_DIRNAME = ".raster-dataset.v1"

    BAND_RATS = MetaItemDefinition(
        re.compile(r"band/band-(.*)-rat\.xml"), MetaItemFileType.XML
    )
    BAND_CATEGORIES = MetaItemDefinition(
        re.compile(r"band/band-(.*)-categories\.json"), MetaItemFileType.JSON
    )

    META_ITEMS = (
        TileDataset.TITLE,
        TileDataset.DESCRIPTION,
        TileDataset.TAGS_JSON,
        TileDataset.FORMAT_JSON,
        TileDataset.SCHEMA_JSON,
        TileDataset.CRS_WKT,
        BAND_RATS,
        BAND_CATEGORIES,
    )

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
        cls,
        tilename=None,
        *,
        parent_path=None,
        include_conflict_versions=False,
        is_pam=None,
    ):
        return get_tile_path_pattern(
            tilename,
            parent_path=parent_path,
            include_conflict_versions=include_conflict_versions,
            is_pam=is_pam,
        )

    def diff_tile(self, other, tile_filter=FeatureKeyFilter.MATCH_ALL, reverse=False):
        # We want one delta per changed tile including those tiles that have changed
        # due to a change in the PAM file.

        # super().diff_tile generates separate deltas for both changed tiles and changed PAM files:
        raw_diff = super().diff_tile(other, tile_filter=tile_filter, reverse=reverse)
        result = DeltaDiff()
        changed_pams = set()
        # Copy across the tile deltas (these already include pamOid fields etc where appropriate).
        for key, delta in raw_diff.items():
            if key.endswith(PAM_SUFFIX):
                changed_pams.add(key[:-LEN_PAM_SUFFIX])
            else:
                result[key] = delta

        # Add in deltas for tiles where only PAM files have changed.
        for key in changed_pams:
            if key not in result:
                pam_delta = Delta(
                    (key, self.get_tile_summary_promise(key)),
                    (key, other.get_tile_summary_promise(key)),
                )
                result[key] = ~pam_delta if reverse else pam_delta

        return result

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
        tile_filter = ds_filter.get("tile", ds_filter.child_type())

        current_metadata = self.tile_metadata
        dataset_format_to_apply = None
        if convert_to_dataset_format:
            dataset_format_to_apply = get_format_summary(current_metadata)

        tilename_to_metadata = {}

        wc_tiles_path_pattern = get_tile_path_pattern(parent_path=self.path)

        tile_diff = DeltaDiff()

        for tile_path in self._non_pam_wc_paths(
            workdir_diff_cache.dirty_paths_for_dataset(self)
        ):
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
                new_tile_summary = tile_metadata["tile"]

                if dataset_format_to_apply and not self.is_tile_compatible(
                    dataset_format_to_apply, new_tile_summary
                ):
                    new_tile_summary = self.pre_conversion_tile_summary(
                        dataset_format_to_apply, new_tile_summary
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

        rewrite_metadata = RewriteMetadata.NO_REWRITE
        profile_constraint = current_metadata["format.json"].get("profile")
        if profile_constraint == "cloud-optimized":
            rewrite_metadata = (
                RewriteMetadata.AS_IF_CONVERTED_TO_COG
                if convert_to_dataset_format
                else RewriteMetadata.NO_REWRITE
            )
        else:
            rewrite_metadata = RewriteMetadata.DROP_PROFILE

        if no_new_metadata:
            merged_metadata = current_metadata
        else:
            merged_metadata = rewrite_and_merge_metadata(
                metadata_list, rewrite_metadata
            )

        meta_diff = DeltaDiff()
        all_keys = set()
        for tm in (current_metadata, merged_metadata):
            all_keys.update(tm)
        for key in all_keys:
            old_value = current_metadata.get(key)
            new_value = merged_metadata.get(key)
            if old_value != new_value:
                old_half_delta = (key, old_value) if old_value else None
                new_half_delta = (key, new_value) if new_value else None
                meta_diff[key] = Delta(old_half_delta, new_half_delta)

        ds_diff = DatasetDiff()
        ds_diff["meta"] = meta_diff
        ds_diff["tile"] = tile_diff

        return ds_diff

    def _non_pam_wc_paths(self, tile_and_pam_paths):
        """
        Given a list of dirty paths, relative to the workdir -
        return the paths of the tiles (relative to the workdir) that have been affected
        either by editing directly or by editing their PAM files.
        Uses git-style paths: / is the part separator, regardless of the platform.
        """
        result = set()
        wc_path = self._workdir_path()
        for path in tile_and_pam_paths:
            if path.lower().endswith(PAM_SUFFIX):
                tile_paths = find_similar_files_case_insensitive(
                    self._workdir_path(path[:-LEN_PAM_SUFFIX])
                )
                result.update("/".join(t.relative_to(wc_path).parts) for t in tile_paths)
            else:
                result.add(path)
        return result

    @property
    def tile_metadata(self):
        return {
            "format.json": self.get_meta_item("format.json"),
            "schema.json": self.get_meta_item("schema.json"),
            "crs.wkt": self.get_meta_item("crs.wkt"),
            **self.get_meta_items_matching(self.BAND_RATS),
            **self.get_meta_items_matching(self.BAND_CATEGORIES),
        }

    @property
    def tile_count(self):
        """The total number of tiles in this dataset, not including PAM files."""
        if self.inner_tree is None:
            return 0
        try:
            subtree = self.inner_tree / self.TILE_PATH
        except KeyError:
            return 0
        return sum(
            1
            for blob in all_blobs_in_tree(subtree)
            if not blob.name.endswith(PAM_SUFFIX)
        )

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
        pam_path = path + PAM_SUFFIX
        pam_pointer_blob = self.get_blob_at(pam_path, missing_ok=True)
        return functools.partial(
            self.get_tile_summary_from_pointer_blob, pointer_blob, pam_pointer_blob
        )

    @classmethod
    def get_tile_summary_from_pointer_blob(
        cls, tile_pointer_blob, pam_pointer_blob=None
    ):
        result = super().get_tile_summary_from_pointer_blob(tile_pointer_blob)
        if pam_pointer_blob is not None:
            # PAM files deltas are output in the same delta as the tile they are attached to.
            pam_summary = super().get_tile_summary_from_pointer_blob(pam_pointer_blob)
            result["pamName"] = result["name"] + PAM_SUFFIX
            result["pamOid"] = pam_summary["oid"]
            result["pamSize"] = pam_summary["size"]
        return result

    def is_tile_compatible(self, target_format, tile_summary):
        """
        Given the a ormat - either format.json {"fileType": ... "profile": ...}
        or a format summary string eg "geotiff/cog" -
        and the tile summary eg {"name": ... "format": ... "oid": ... "size": ... }
        returns True if the tile is compatible with the target format.
        """
        tile_format = tile_summary["format"]
        if isinstance(target_format, dict):
            target_format = get_format_summary(target_format)
        return tile_format == target_format or tile_format.startswith(
            f"{target_format}/"
        )

    def pre_conversion_tile_summary(self, ds_format, tile_summary):
        """
        Converts a tile-summary - that is, updates the tile-summary to be a mix of the tiles current information
        (prefixed with "source") and its future information - what it will be once converted - where that is known.
        """
        if isinstance(ds_format, dict):
            ds_format = get_format_summary(ds_format)

        envisioned_summary = {
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
        """
        Applies a tile-diff to the given object builder (so that the diff can be committed),
        and in the process performs the necessary side effects - converting tiles to the
        relevant format (where needed) and copying them to the LFS cache (where needed).
        """
        with object_builder.chdir(self.inner_path):
            for delta in tile_diff.values():

                pam_name, pam_source_name, pam_oid, pam_size = None, None, None, None
                if delta.type in ("insert", "update"):
                    # TODO - check if committing .tiff vs .tif always works as intended.

                    new_val = delta.new_value
                    name = new_val.get("name")
                    source_name = new_val.get("sourceName") or name
                    path_in_wc = self._workdir_path(f"{self.path}/{source_name}")

                    if new_val.get("sourceFormat"):
                        # Converting and then committing a new tile

                        conversion_func = functools.partial(
                            convert_tile_to_format,
                            target_format=new_val["format"],
                        )
                        pointer_dict = copy_file_to_local_lfs_cache(
                            self.repo, path_in_wc, conversion_func
                        )
                    else:
                        # Committing in a new tile, preserving its format
                        oid_and_size = new_val["oid"], new_val["size"]
                        pointer_dict = copy_file_to_local_lfs_cache(
                            self.repo, path_in_wc, oid_and_size=oid_and_size
                        )

                    pointer_dict = merge_pointer_file_dicts(new_val, pointer_dict)

                    tilename = new_val["name"]
                    tile_blob_path = self.tilename_to_blob_path(tilename, relative=True)
                    object_builder.insert(
                        tile_blob_path, dict_to_pointer_file_bytes(pointer_dict)
                    )
                    # Update the diff to record what was stored - this is used to reset the workdir.
                    new_val.update(oid=pointer_dict["oid"], size=pointer_dict["size"])

                    pam_name = new_val.get("pamName")
                    pam_source_name = new_val.get("pamSourceName") or pam_name
                    pam_oid = new_val.get("pamOid")
                    pam_size = new_val.get("pamSize")

                else:  # delete:
                    tilename = delta.old_key
                    tile_blob_path = self.tilename_to_blob_path(tilename, relative=True)
                    object_builder.remove(tile_blob_path)

                pam_blob_path = tile_blob_path + PAM_SUFFIX
                if pam_name is not None:
                    path_in_wc = self._workdir_path(f"{self.path}/{pam_source_name}")
                    pointer_dict = copy_file_to_local_lfs_cache(
                        self.repo, path_in_wc, oid_and_size=(pam_oid, pam_size)
                    )
                    object_builder.insert(
                        pam_blob_path,
                        dict_to_pointer_file_bytes(pointer_dict),
                    )
                else:
                    object_builder.remove(pam_blob_path)
