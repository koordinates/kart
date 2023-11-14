import functools
import os

from kart.base_dataset import BaseDataset
from kart.core import all_blobs_with_parent_in_tree
from kart.decorators import allow_classmethod
from kart.diff_structs import DatasetDiff, DeltaDiff, Delta, WORKING_COPY_EDIT
from kart.diff_format import DiffFormat
from kart.exceptions import InvalidOperation
from kart.key_filters import DatasetKeyFilter, FeatureKeyFilter
from kart.list_of_conflicts import ListOfConflicts, InvalidNewValue
from kart.lfs_util import (
    get_hash_from_pointer_file,
    pointer_file_bytes_to_dict,
    copy_file_to_local_lfs_cache,
    merge_pointer_file_dicts,
    dict_to_pointer_file_bytes,
)
from kart import meta_items
from kart.meta_items import MetaItemDefinition, MetaItemFileType
from kart.progress_util import progress_bar
from kart.serialise_util import hexhash
from kart.spatial_filter import SpatialFilter
from kart.tile.tilename_util import (
    find_similar_files_case_insensitive,
    PAM_SUFFIX,
    LEN_PAM_SUFFIX,
)
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

    LINKED_STORAGE_JSON = meta_items.LINKED_STORAGE_JSON

    # Subclasses may override to add extra meta-items.
    META_ITEMS = (
        TITLE,
        DESCRIPTION,
        TAGS_JSON,
        FORMAT_JSON,
        SCHEMA_JSON,
        CRS_WKT,
        LINKED_STORAGE_JSON,
    )

    @property
    def tile_tree(self):
        return self.get_subtree(self.TILE_PATH)

    def tile_pointer_blobs_and_dicts(
        self,
        spatial_filter=SpatialFilter.MATCH_ALL,
        show_progress=False,
        *,
        parse_pointer_dicts=True,
    ):
        """
        Returns a generator that yields every tile pointer blob and every PAM file pointer blob in turn.
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
            for parent, blob in all_blobs_with_parent_in_tree(tile_tree):
                if blob.name.endswith(PAM_SUFFIX):
                    is_tile = False
                    try:
                        tile_blob = parent / blob.name[:-LEN_PAM_SUFFIX]
                    except KeyError:
                        tile_blob = None
                else:
                    tile_blob = blob
                    is_tile = True

                pointer_dict = None
                tile_dict = None
                if parse_pointer_dicts:
                    pointer_dict = pointer_file_bytes_to_dict(blob)
                    if is_tile:
                        tile_dict = pointer_dict

                # Use tile_dict if we have it - saves parsing the blob twice:
                if spatial_filter.matches(tile_dict or tile_blob):
                    yield blob, pointer_dict
                    if is_tile:
                        n_matched += 1

                if is_tile:
                    n_read += 1
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
        for blob, _ in self.tile_pointer_blobs_and_dicts(
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
    def tilename_from_path(cls, tile_path, remove_pam_suffix=False):
        """Given a path to a tile, return the tile's base name (without containing folders or file extension)."""
        return cls.remove_tile_extension(
            os.path.basename(tile_path), remove_pam_suffix=remove_pam_suffix
        )

    @classmethod
    def remove_tile_extension(cls, filename, remove_pam_suffix=False):
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
    def get_format_summary(cls, format_json):
        """Given a format.json meta-item, returns a string that summarizes the most important aspects of the format."""
        raise NotImplementedError()

    @classmethod
    def convert_tile_to_format(cls, source_path, dest_path, target_format):
        """Given a tile at source_path, writes the equivalent tile at dest_path in the target format."""
        raise NotImplementedError()

    @classmethod
    def write_mosaic_for_directory(cls, directory_path):
        """
        Given a folder containing some point-cloud / raster tiles, write a mosaic file that combines them all into a single
        "virtual" tile. The mosaic file will contain references to the tiles, rather than replicating their contents.
        """
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

    @property
    def path_in_workdir(self):
        """Returns a pathlib Path of the directory within the workdir where this dataset is checked out."""
        return self.repo.workdir_file(self.path)

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

    def is_tile_compatible(self, tile_summary, target_format):
        """
        Given the a target format - either format.json dict or a format summary string -
        and the tile summary eg {"name": ... "format": ... "oid": ... "size": ... }
        returns True if the tile is compatible with the target format.
        """
        if isinstance(target_format, dict):
            target_format = self.get_format_summary(target_format)
        tile_format = tile_summary["format"]
        return tile_format == target_format or tile_format.startswith(
            f"{target_format}/"
        )

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

    @property
    def tile_metadata(self):
        return {
            "format.json": self.get_meta_item("format.json"),
            "schema.json": self.get_meta_item("schema.json"),
            "crs.wkt": self.get_meta_item("crs.wkt"),
        }

    def is_cloud_optimized():
        """Returns True if this dataset is constrained so that only cloud-optimized tiles can be added to it."""
        raise NotImplementedError()

    def diff_to_working_copy(
        self,
        workdir_diff_cache,
        ds_filter=DatasetKeyFilter.MATCH_ALL,
        *,
        convert_to_dataset_format=None,
        extract_metadata=True,
    ):
        """
        Returns a diff of all changes made to this dataset in the working copy.

        convert_to_dataset_format - user wants this converted to dataset's format as it is
            committed, and wants to see diffs of what this would look like.
        extract_metadata - if False, don't run gdal / pdal to check the tile contents.
            The resulting diffs are missing almost all of the info about the new tiles,
            but this is faster and more reliable if this information is not needed.
        """
        workdir = self.repo.working_copy.workdir
        with workdir.state_session() as sess:
            if workdir._is_noncheckout_dataset(sess, self.path):
                return DatasetDiff()

        tile_filter = ds_filter.get("tile", ds_filter.child_type())

        current_metadata = self.tile_metadata
        target_format = None
        if convert_to_dataset_format:
            target_format = self.get_format_summary(current_metadata)

        tilename_to_metadata = {}

        tile_diff = DeltaDiff()

        for tilename, workdir_path in self.get_dirty_tile_paths(workdir_diff_cache):
            if tilename not in tile_filter:
                continue

            old_tile_summary = self.get_tile_summary_promise(tilename, missing_ok=True)
            old_half_delta = (tilename, old_tile_summary) if old_tile_summary else None

            if workdir_path is None:
                new_half_delta = None
            elif extract_metadata:
                tile_metadata = self.extract_tile_metadata_from_filesystem_path(
                    workdir_path
                )
                tilename_to_metadata[tilename] = tile_metadata
                new_tile_summary = self.get_envisioned_tile_summary(
                    tile_metadata["tile"], target_format
                )

                new_half_delta = tilename, new_tile_summary
            else:
                new_half_delta = tilename, {"sourceName": workdir_path.name}

            if old_half_delta is None and new_half_delta is None:
                # This can happen by eg editing a .tif.aux.xml file that's not attached to anything.
                # We detect such an edit, but we don't let you check it in on its own, so there's no diff.
                continue

            tile_delta = Delta(old_half_delta, new_half_delta)
            tile_delta.flags = WORKING_COPY_EDIT

            if self.should_suppress_minor_tile_change(tile_delta):
                continue

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
            merged_metadata = self.rewrite_and_merge_metadata(
                current_metadata, metadata_list, convert_to_dataset_format
            )

        self.check_merged_metadata(
            current_metadata, merged_metadata, convert_to_dataset_format
        )

        meta_diff = DeltaDiff.diff_dicts(current_metadata, merged_metadata)

        ds_diff = DatasetDiff()
        ds_diff["meta"] = meta_diff
        ds_diff["tile"] = tile_diff

        return ds_diff

    def get_dirty_tile_paths(self, workdir_diff_cache):
        """
        Given the workdir_diff_cache - which contains a git-formatted list of dirty paths within this dataset -
        yields a list of (tilename, workdir_path) tuples where tilename is the normalised name (without any extension)
        of the tile affected, and workdir_path is a pathlib Path to the new version of that tile where it currently
        exists in the workdir (if there is a new version, or None if there is not).
        Note that the pathlib Path will include an extension, which could be any one of a variety of standard or
        non-standard extensions: .las .laz .copc.laz .tif .tiff .LAS .LAZ .COPC.LAZ .TIF .TIFF .copc.LAZ etc
        """
        dataset_path_in_workdir = self.path_in_workdir

        tile_or_pam_path_pattern = self.get_tile_path_pattern(parent_path=self.path)

        dirty_tilenames = set()
        for path in workdir_diff_cache.dirty_paths_for_dataset(self):
            if not tile_or_pam_path_pattern.fullmatch(path):
                continue
            tilename = self.tilename_from_path(path, remove_pam_suffix=False)
            if tilename.endswith(PAM_SUFFIX):
                # The tile and the PAM file might not perfectly match in terms of case.
                # We want to use commit using case of the tile, rather than the the case of the PAM file.
                tile_path = (
                    dataset_path_in_workdir / os.path.basename(path)[:-LEN_PAM_SUFFIX]
                )
                tile_paths = find_similar_files_case_insensitive(tile_path)
                for tile_path in tile_paths:
                    dirty_tilenames.add(self.tilename_from_path(tile_path))
            else:
                dirty_tilenames.add(tilename)

        if not dataset_path_in_workdir.is_dir():
            for tilename in dirty_tilenames:
                yield tilename, None

        tile_path_pattern = self.get_tile_path_pattern(is_pam=False)
        for tilename in dirty_tilenames:
            related_paths = dataset_path_in_workdir.glob(f"{tilename}.*")
            tile_paths = [
                p
                for p in related_paths
                if p.name.startswith(tilename) and tile_path_pattern.fullmatch(p.name)
            ]
            if not tile_paths:
                yield tilename, None
            elif len(tile_paths) == 1:
                yield tilename, tile_paths[0]
            else:
                # TODO - instead of erroring immediately, we could instead flag this as a ListOfConflicts.
                raise InvalidOperation(
                    f"More than one tile found in working copy with the same name: {self.path}:tile:{tilename}"
                )

    def get_envisioned_tile_summary(self, tile_summary, target_format):
        """
        Converts a tile-summary (not the tile itself) to the target_format - that is, updates the tile-summary to be a
        mix of the tiles' current information (prefixed with "source") and its future information - what it will be once
        converted - where that information is known.
        Not everything can be known: for instance, we cannot know the OID of the converted tile until after
        the conversion is done.
        If the target_format is None or the tile will not be converted, the only thing we need to envision is that
        we will standardise the filename (if it is currently non-standard).
        """
        if isinstance(target_format, dict):
            target_format = self.get_format_summary(target_format)

        if target_format is None or self.is_tile_compatible(
            tile_summary, target_format
        ):
            envisioned_summary = {
                "name": self.set_tile_extension(
                    tile_summary["name"], tile_format=tile_summary["format"]
                )
            }
        else:
            envisioned_summary = {
                "name": self.set_tile_extension(
                    tile_summary["name"], tile_format=target_format
                ),
                "format": target_format,
                "oid": None,
                "size": None,
            }

        result = {}
        for key, value in tile_summary.items():
            if envisioned_summary.get(key):
                result[key] = envisioned_summary[key]
            if key in envisioned_summary and envisioned_summary[key] != value:
                source_key = "source" + key[0].upper() + key[1:]
                if source_key not in tile_summary:
                    result[source_key] = value
            else:
                result[key] = value
        return result

    SUPPRESS_DIFF_KEYS = {"sourceName", "sourceOid", "sourceSize"}

    def should_suppress_minor_tile_change(self, tile_delta):
        """
        Return True if a change to a particular tile is irrelevant or inadvertent and shouldn't
        be shown when a user runs kart diff or kart status.
        """
        if tile_delta.type != "update":
            return False

        old_value = tile_delta.old_value
        new_value = tile_delta.new_value

        all_keys = set()
        all_keys.update(old_value)
        all_keys.update(new_value)
        for key in all_keys:
            if key in self.SUPPRESS_DIFF_KEYS:
                continue
            if old_value.get(key) != new_value.get(key):
                return False

        return True

    def rewrite_and_merge_metadata(
        self, current_metadata, metadata_list, convert_to_dataset_format
    ):
        """
        Attempts to merge all the metadata in metadata_list into a single piece of metadata
        that describes them all, using current_metadata + convert_to_dataset_format to decide
        which parts of the metadata can be dropped or modified during commit.
        """
        raise NotImplementedError()

    def check_merged_metadata(
        self, current_metadata, merged_metadata, convert_to_dataset_format=None
    ):
        """
        Checks if the resulting merged metadata is allowed by the Kart model.
        Any disallowed meta-items can be flagged by wrapping them in an InvalidNewValue.
        A ListOfConflicts is already flagged as disallowed -
        it doesn't need further handling, unless the error message can be improved
        """
        current_format = current_metadata.get("format.json")
        merged_format = merged_metadata.get("format.json")

        # Check for a ListOfConflicts that would go away if we dropped the optimization constraint.
        # Note that this will still be disallowed by the rest of this function unless --no-convert-to-dataset-format is specified.
        merged_format = self.simplify_diff_for_dropping_cloud_optimized(
            current_format, merged_format
        )
        merged_metadata["format.json"] = merged_format

        # Check if format.json has changed - this is allowed, so long as the user explicitly requests
        # it using --no-convert-to-dataset-format. Otherwise it is not allowed.

        if isinstance(merged_format, ListOfConflicts):
            # This is already flagged as disallowed - no need to do anything else.
            return

        if current_format == merged_format:
            # This is always allowed, so no need to do anything else.
            return

        # If --convert_to_dataset_format was specified, the new format should already match the old one
        # thanks to the "envisioned tile summary" logic.
        assert convert_to_dataset_format is not True

        # If --no-convert-to-dataset-format was specified, we allow format.json to chage.
        if convert_to_dataset_format is False:
            return

        # The third and final case is where they have not specified either way. We don't allow a change to
        # format.json without specifying --no-convert-to-dataset-format.
        assert convert_to_dataset_format is None

        extra_desc = ""
        if self.is_cloud_optimized(current_format) and not self.is_cloud_optimized(
            merged_format
        ):
            extra_desc = " from cloud-optimized to non-cloud-optimized"

        merged_format = InvalidNewValue([merged_format])
        merged_format.error_message = (
            f"Committing these tiles as-is would change the format of dataset {self.path!r}{extra_desc}. "
            "Use --convert-to-dataset-format to convert the tiles to the dataset's format while committing them, "
            "or use --no-convert-to-dataset-format to keep the tiles as they are and change the dataset's format."
        )
        merged_metadata["format.json"] = merged_format

    def simplify_diff_for_dropping_cloud_optimized(self, current_format, merged_format):
        """
        The diff_to_working_copy function uses roughly the same logic to generate deltas every type of meta-item.
        Adding new tiles that are incompatible with the old tiles without removing the old tiles will result
        in a delta like the following (eg if you added a new CRS that was not compatible):
        crs_delta = Delta(old=old_crs, new=ListOfConflicts(old_crs, new_crs))

        The same logic results in a similar delta when you try to add new tiles that are not cloud-optimized to a
        cloud-optimized dataset:
        format_delta = Delta(old=cloud-optimized-format, new=ListOfConflicts(cloud-optimized-format, non-cloud-optimized-format)).

        This function's job is to simplify that to be Delta(old=cloud-optimized-format, non-cloud-optimized-format) since
        the non-cloud-optimized-format is also valid for the existing cloud-optimized tiles.

        Note that this Delta will still be disallowed by the code in check_merged_metadata if the user has not specified
        --no-convert-to-dataset-format, which is required whenever the user is making changes to format.json
        """
        raise NotImplementedError()

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
                if delta.type in ("insert", "update"):
                    new_val = delta.new_value
                    name = new_val.get("name")
                    source_name = new_val.get("sourceName") or name
                    path_in_wc = self.path_in_workdir / source_name

                    if new_val.get("sourceFormat"):
                        # Converting and then committing a new tile

                        conversion_func = functools.partial(
                            self.convert_tile_to_format,
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

                else:  # delete:
                    new_val = None
                    tilename = delta.old_key
                    tile_blob_path = self.tilename_to_blob_path(tilename, relative=True)
                    object_builder.remove(tile_blob_path)

                pam_blob_path = tile_blob_path + PAM_SUFFIX

                if new_val and new_val.get("pamOid"):
                    pam_name = new_val.get("pamName")
                    pam_source_name = new_val.get("pamSourceName") or pam_name
                    pam_oid = new_val.get("pamOid")
                    pam_size = new_val.get("pamSize")

                    path_in_wc = self.path_in_workdir / pam_source_name
                    pointer_dict = copy_file_to_local_lfs_cache(
                        self.repo, path_in_wc, oid_and_size=(pam_oid, pam_size)
                    )
                    object_builder.insert(
                        pam_blob_path,
                        dict_to_pointer_file_bytes(pointer_dict),
                    )
                else:
                    object_builder.remove(pam_blob_path)
