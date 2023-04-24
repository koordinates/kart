import functools
import os

from kart.base_dataset import BaseDataset
from kart.core import all_blobs_in_tree
from kart.decorators import allow_classmethod
from kart.diff_structs import DeltaDiff
from kart.diff_format import DiffFormat
from kart.key_filters import DatasetKeyFilter, FeatureKeyFilter
from kart.lfs_util import (
    get_hash_from_pointer_file,
    get_local_path_from_lfs_hash,
    pointer_file_bytes_to_dict,
)
from kart import meta_items
from kart.meta_items import MetaItemDefinition, MetaItemFileType
from kart.progress_util import progress_bar
from kart.serialise_util import hexhash
from kart.spatial_filter import SpatialFilter
from kart.tile.tilename_util import PAM_SUFFIX
from kart.working_copy import PartType


class TileDataset(BaseDataset):
    """
    An abstract tile-based dataset. Concrete implementations: point clouds, rasters.

    Tile-based dataset store large files in user-recognisable formats (contrast to tabular datasets,
    which store smaller features in Kart-specific formats that are only used internally and need to
    be converted to a user-recognisable format on checkout).
    The tiles are stored using Git LFS. The LFS pointer files contain extra metadata about the
    geographical extent of the tile that they point to, to allow for spatial filtering.
    Tiles are checked out into the file-system part of the working copy (see workdir.py)
    """

    ITEM_TYPE = "tile"

    WORKING_COPY_PART_TYPE = PartType.WORKDIR

    # All relative paths should be relative to self.inner_tree - that is, to the tree named DATASET_DIRNAME.
    TILE_PATH = "tile/"

    TITLE = meta_items.TITLE
    DESCRIPTION = meta_items.DESCRIPTION
    TAGS_JSON = meta_items.TAGS_JSON

    # Information about which tile format(s) this dataset requires / allows.
    FORMAT_JSON = MetaItemDefinition("format.json", MetaItemFileType.JSON)

    SCHEMA_JSON = meta_items.SCHEMA_JSON
    CRS_WKT = meta_items.CRS_WKT

    # Subclasses may override to add extra meta-items.
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
            for blob in all_blobs_in_tree(tile_tree):
                if not blob.name.endswith(PAM_SUFFIX):
                    n_read += 1
                tile_dict = None
                if parse_pointer_dicts:
                    tile_dict = pointer_file_bytes_to_dict(blob)
                # TODO - fix spatial filter to work properly with PAM files.
                if spatial_filter.matches(tile_dict if parse_pointer_dicts else blob):
                    if not blob.name.endswith(PAM_SUFFIX):
                        n_matched += 1
                    yield blob, tile_dict

                if not blob.name.endswith(PAM_SUFFIX):
                    p.update(1)

        if show_progress and not spatial_filter.match_all:
            p.write(
                f"(of {n_read} tiles read, wrote {n_matched} matching tiles to the working copy due to spatial filter)"
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
        """The total number of tiles in this dataset."""
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
        type of tile the blob is pointing to.
        """
        for blob, pointer_dict in self._tile_pointer_blobs_and_dicts(
            spatial_filter=spatial_filter, show_progress=show_progress
        ):
            if fix_extensions:
                tile_format = pointer_dict.get("format")
                oid = pointer_dict["oid"].split(":", maxsplit=1)[1]
                yield self.set_tile_extension(blob.name, tile_format=tile_format), oid
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
        assert relative or isinstance(self, TileDataset)

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
        """Given a path to a tile, return the tile's base name (without containing folders or file extension)."""
        return cls.remove_tile_extension(os.path.basename(tile_path))

    @classmethod
    def remove_tile_extension(cls, filename):
        """
        Remove the extension from the given tile's filename.
        What counts as an extension depends on what type of tiles this dataset stores.
        For instance, for a point cloud filename: "auckland.latest.copc.laz" - ".copc.laz" is the extension.
        """
        raise NotImplementedError()

    @classmethod
    def set_tile_extension(cls, tilename, ext=None, tile_format=None):
        """Changes a tile's file extension to the given extension, or to the extension appropriate for its format."""
        raise NotImplementedError()

    @classmethod
    def get_tile_summary_from_pointer_blob(cls, tile_pointer_blob):
        result = pointer_file_bytes_to_dict(
            tile_pointer_blob, {"name": tile_pointer_blob.name}
        )
        result["name"] = cls.set_tile_extension(
            result["name"], tile_format=result.get("format")
        )
        # LFS version info is in every pointer file but is not interesting to the user.
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

    def _workdir_path(self, wc_path=""):
        if isinstance(wc_path, str):
            return self.repo.workdir_file(wc_path)
        else:
            return wc_path

    @classmethod
    def extract_tile_metadata_from_filesystem_path(cls, path):
        raise NotImplementedError()

    def diff(
        self,
        other,
        ds_filter=DatasetKeyFilter.MATCH_ALL,
        reverse=False,
        diff_format=DiffFormat.FULL,
    ):
        """
        Generates a Diff from self -> other.
        If reverse is true, generates a diff from other -> self.
        """
        ds_diff = super().diff(other, ds_filter=ds_filter, reverse=reverse)

        tile_filter = ds_filter.get("tile", ds_filter.child_type())

        # If the user is asking for a no data changes diff, just check if the feature subtree is different.
        if diff_format == DiffFormat.NO_DATA_CHANGES:
            self_subtree = self.get_subtree("tile")
            other_subtree = other.get_subtree("tile") if other else self._empty_tree
            data_changes = self_subtree != other_subtree

            ds_diff["data_changes"]: bool = data_changes

        # Else do a full diff.
        else:
            ds_diff["tile"] = self.diff_tile(other, tile_filter, reverse=reverse)

        return ds_diff

    def diff_tile(self, other, tile_filter=FeatureKeyFilter.MATCH_ALL, reverse=False):
        """
        Returns a DeltaDiff of deltas from self -> other, but only for tiles that match the tile_filter.
        If reverse is true, returns a DeltaDiff of deltas from other -> self.
        """
        return DeltaDiff(
            self.diff_subtree(
                other,
                "tile",
                key_filter=tile_filter,
                key_decoder_method="tilename_from_path",
                value_decoder_method="get_tile_summary_promise_from_blob_path",
                reverse=reverse,
            )
        )

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
        raise NotImplementedError()

    @property
    def tile_metadata(self):
        return {
            "format.json": self.get_meta_item("format.json"),
            "schema.json": self.get_meta_item("schema.json"),
            "crs.wkt": self.get_meta_item("crs.wkt"),
        }
