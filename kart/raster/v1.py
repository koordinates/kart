import functools
import re

from kart.core import all_blobs_in_tree
from kart.diff_structs import DeltaDiff, Delta
from kart.key_filters import FeatureKeyFilter
from kart.lfs_util import (
    get_local_path_from_lfs_hash,
)
from kart.list_of_conflicts import ListOfConflicts
from kart.meta_items import MetaItemDefinition, MetaItemFileType
from kart.raster.gdal_convert import convert_tile_to_format
from kart.raster.metadata_util import (
    RewriteMetadata,
    extract_raster_tile_metadata,
    rewrite_and_merge_metadata,
    get_format_summary,
    is_cog,
)
from kart.raster.pam_util import is_same_xml_ignoring_stats
from kart.raster.tilename_util import (
    get_tile_path_pattern,
    remove_tile_extension,
    set_tile_extension,
)
from kart.tile.tile_dataset import TileDataset
from kart.tile.tilename_util import (
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
    def remove_tile_extension(cls, filename, remove_pam_suffix=None):
        """Given a tile filename, removes the suffix .tif or .tiff"""
        return remove_tile_extension(filename, remove_pam_suffix=remove_pam_suffix)

    @classmethod
    def set_tile_extension(cls, filename, ext=None, tile_format=None):
        """Changes a tile's file extension to the given extension, or to the extension appropriate for its format."""
        return set_tile_extension(filename, ext=ext, tile_format=tile_format)

    @classmethod
    def extract_tile_metadata_from_filesystem_path(cls, path):
        return extract_raster_tile_metadata(path)

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

    SUPPRESS_DIFF_KEYS = {
        "sourceName",
        "sourceOid",
        "sourceSize",
        "pamSourceName",
        "pamName",
        "pamOid",
        "pamSize",
    }

    def should_suppress_minor_tile_change(self, tile_delta):
        """
        Given a tile delta where the new value is the current state of the WC,
        return True if the only thing that has changed in the tile since the last commit
        is that stats have been added or removed.
        """

        # Check if something has obviously changed - something that isn't the PAM file:
        if not super().should_suppress_minor_tile_change(tile_delta):
            return False

        # Only the PAM file has changed, if anything. See if it is a minor change or not:

        old_value = tile_delta.old_value
        new_value = tile_delta.new_value

        old_pam_oid = old_value.get("pamOid")
        new_pam_oid = new_value.get("pamOid")
        if old_pam_oid == new_pam_oid:
            return True

        old_pam_path = None
        if old_pam_oid:
            old_pam_path = get_local_path_from_lfs_hash(self.repo, old_pam_oid)
            if not old_pam_path.is_file():
                return False  # Can't check the contents, so don't suppress the change.
            old_pam_path = str(old_pam_path)

        new_pam_path = None
        if new_pam_oid:
            new_pam_name = new_value.get("pamSourceName") or new_value.get("pamName")
            new_pam_path = self.path_in_workdir / new_pam_name
            if not new_pam_path.is_file():
                return False  # Can't check the contents, so don't suppress the change.
            new_pam_path = str(new_pam_path)

        return is_same_xml_ignoring_stats(old_pam_path, new_pam_path)

    @property
    def tile_metadata(self):
        return {
            "format.json": self.get_meta_item("format.json"),
            "schema.json": self.get_meta_item("schema.json"),
            "crs.wkt": self.get_meta_item("crs.wkt"),
            **self.get_meta_items_matching(self.BAND_RATS),
            **self.get_meta_items_matching(self.BAND_CATEGORIES),
        }

    def is_cloud_optimized(self, format_json=None):
        if format_json is None:
            format_json = self.get_meta_item("format.json")
        return is_cog(format_json)

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

    def rewrite_and_merge_metadata(
        self, current_metadata, metadata_list, convert_to_dataset_format
    ):
        profile_constraint = current_metadata["format.json"].get("profile")
        if profile_constraint == "cloud-optimized":
            rewrite_metadata = (
                RewriteMetadata.AS_IF_CONVERTED_TO_COG
                if convert_to_dataset_format
                else RewriteMetadata.NO_REWRITE
            )
        else:
            rewrite_metadata = RewriteMetadata.DROP_PROFILE
        return rewrite_and_merge_metadata(metadata_list, rewrite_metadata)

    def simplify_diff_for_dropping_cloud_optimized(self, current_format, merged_format):
        if (
            current_format.get("profile") == "cloud-optimized"
            and isinstance(merged_format, ListOfConflicts)
            and len(merged_format) == 2
        ):
            format_list = [current_format, merged_format[0], merged_format[1]]
            simplified_list = [
                {k: v for k, v in f.items() if k != "profile"} for f in format_list
            ]
            if all(f == simplified_list[0] for f in simplified_list):
                return simplified_list[0]
        return merged_format
