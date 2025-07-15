import concurrent.futures
from functools import cached_property
import glob
import logging
import math
import os
from pathlib import Path
import sys
import uuid

import click
import pygit2

from kart.cli_util import find_param
from kart.dataset_util import validate_dataset_paths
from kart.exceptions import (
    InvalidOperation,
    NotFound,
    NO_IMPORT_SOURCE,
    NO_DATA,
    NO_CHANGES,
    WORKING_COPY_OR_IMPORT_CONFLICT,
)
from kart.fast_import import (
    FastImportSettings,
    git_fast_import,
    generate_header,
    write_blob_to_stream,
    write_blobs_to_stream,
)
from kart.key_filters import RepoKeyFilter
from kart.lfs_util import (
    install_lfs_hooks,
    dict_to_pointer_file_bytes,
)
from kart.list_of_conflicts import ListOfConflicts
from kart.meta_items import MetaItemFileType
from kart.s3_util import expand_s3_glob
from kart.tile.tile_source import TileSource
from kart.progress_util import progress_bar
from kart.output_util import (
    format_json_for_output,
    format_wkt_for_output,
    InputMode,
    get_input_mode,
)

from kart.tabular.version import (
    SUPPORTED_VERSIONS,
    extra_blobs_for_version,
)
from kart.utils import get_num_available_cores
from kart.working_copy import PartType


L = logging.getLogger(__name__)


class TileImporter:
    """Subclassable logic for importing tile-based datasets - see tile_dataset.py"""

    def __init__(
        self,
        *,
        repo,
        ctx,
        dataset_path,
        convert_to_cloud_optimized,
        message,
        do_checkout,
        replace_existing,
        update_existing,
        delete,
        amend,
        allow_empty,
        num_workers,
        do_link,
        sources,
        override_crs=None,
    ):
        """
        repo - the Kart repo from the context.
        ctx - the current Click context.
        dataset_path - path to the dataset where the tiles will be imported.
        convert_to_cloud_optimized - whether to automatically convert tiles to cloud-optimized (COPC or COG) while importing.
            If True, the resulting dataset will also be constrained to only contain cloud-optimized tiles.
            If False, the user has explicitly opted out of this constraint.
            If None, we still need to check if the user is aware of this possibility and prompt them to see what they would prefer.
        message - commit message for the import commit.
        do_checkout - Whether to create a working copy once the import is finished, if no working copy exists yet.
        replace_existing - if True, replace any existing dataset at dataset_path with a new one containing only these tiles.
        update_existing - if True, update any existing dataset at the same path. Existing tiles will be replaced by source.
            tiles with the same name, other existing tiles remain unchanged.
        delete - list of existing tiles to delete, relevant when updating an existing dataset.
        amend - if True, amends the previous commit rather than creating a new import commit.
        allow_empty - if True, the import commit will be created even if the dataset is not changed.
        num_workers - specify the number of workers to use, or set to None to use the number of detected cores.
        sources - paths to tiles to import.
        override_crs - if specified, override the CRS of all source tiles and set the dataset CRS.
        """
        self.repo = repo
        self.ctx = ctx

        self.dataset_path = dataset_path
        self.convert_to_cloud_optimized = convert_to_cloud_optimized
        self.message = message
        self.do_checkout = do_checkout
        self.replace_existing = replace_existing
        self.update_existing = update_existing
        self.delete = delete
        self.amend = amend
        self.allow_empty = allow_empty
        self.num_workers = num_workers
        self.do_link = do_link
        self.sources = sources
        self.override_crs = override_crs

        need_to_store_tiles = not self.do_link
        need_tiles_for_wc = self.do_checkout and not self.repo.is_bare
        self.do_fetch_tiles = need_to_store_tiles or need_tiles_for_wc

        # When doing any kind of initial import we still have to write the table_dataset_version,
        # even though it's not really relevant to tile imports.
        assert self.repo.table_dataset_version in SUPPORTED_VERSIONS

    # A dict of the form {sidecar-key-prefix: sidecar-filename-suffix}
    # For example: {"pam": ".aux.xml"} since we use the prefix pam to label keys from .aux.xml aka PAM files.
    SIDECAR_FILES: dict[str, str] = {}

    @cached_property
    def ALLOWED_SCHEMES(self):
        if self.do_link:
            return ("s3",)
        else:
            return (None, "s3")

    @cached_property
    def ALLOWED_SCHEMES_DESC(self):
        if self.do_link:
            return "an S3 URL"
        else:
            return "a path to a file or an S3 URL"

    @cached_property
    def FETCH_OR_EXTRACT_TILE_METADATA_DESC(self):
        if not self.all_source_schemes or self.all_source_schemes == {None}:
            # The 'None' scheme means local file. All files are local:
            return "Checking tiles"
        else:
            # Some tiles are remote
            return "Fetching tiles" if self.do_fetch_tiles else "Fetching tile metadata"

    @cached_property
    def IMPORT_TILES_TO_STREAM_DESC(self):
        return "Importing tiles" if self.do_fetch_tiles else "Importing tile metadata"

    def import_tiles(self):
        """
        Import the tiles at sources as a new dataset / use them to update an existing dataset.
        """

        self.num_workers = self.check_num_workers(self.num_workers)

        if not self.sources and not self.delete:
            # sources aren't required if you use --delete;
            # this allows you to use this command to solely delete tiles.
            # otherwise, sources are required.
            raise self.missing_parameter("args")

        if self.delete and not self.dataset_path:
            # Dataset-path is required if you use --delete.
            raise self.missing_parameter("dataset_path")

        if self.do_link:
            if self.convert_to_cloud_optimized:
                raise click.UsageError(
                    f"Sorry, converting a linked dataset to {self.CLOUD_OPTIMIZED_VARIANT_ACRONYM} is not supported - "
                    "the data must remain in its original location and its original format as the authoritative source."
                )
            self.convert_to_cloud_optimized = False

        if not self.dataset_path:
            self.dataset_path = self.infer_dataset_path(self.sources)
            if self.dataset_path:
                click.echo(
                    f"Defaulting to '{self.dataset_path}' as the dataset path..."
                )
            else:
                raise self.missing_parameter("dataset_path")

        if self.delete:
            # --delete kind of implies --update-existing (we're modifying an existing dataset)
            # But a common way for this to do the wrong thing might be this:
            #   kart ... --delete auckland/auckland_3_*.laz
            # i.e. if --delete is used with a glob, then we don't want to treat the remaining paths as
            # sources and import them. In that case, *not* setting update_existing here will fall through
            # to cause an error below:
            #  * either the dataset exists, and we fail with a dataset conflict
            #  * or the dataset doesn't exist, and the --delete fails
            if not self.sources:
                self.update_existing = True

        if self.replace_existing or self.update_existing:
            validate_dataset_paths([self.dataset_path])
        else:
            old_dataset_paths = [ds.path for ds in self.repo.datasets()]
            validate_dataset_paths([*old_dataset_paths, self.dataset_path])

        if (
            self.replace_existing or self.update_existing or self.delete
        ) and self.repo.working_copy.workdir:
            # Avoid conflicts by ensuring the WC is clean.
            # NOTE: Technically we could allow anything to be dirty except the single dataset
            # we're importing (or even a subset of that dataset). But this'll do for now
            self.repo.working_copy.workdir.check_not_dirty()

        self.existing_dataset = self.get_existing_dataset()
        self.existing_metadata = (
            self.existing_dataset.tile_metadata if self.existing_dataset else None
        )
        self.include_existing_metadata = (
            self.update_existing and self.existing_dataset is not None
        )

        self.tile_sources = self.preprocess_sources(self.sources)

        if self.delete and self.existing_dataset is None:
            # Trying to delete specific paths from a nonexistent dataset?
            # This suggests the caller is confused.
            raise InvalidOperation(
                f"Dataset {self.dataset_path} does not exist. Cannot delete paths from it."
            )

        if self.tile_sources:
            if self.convert_to_cloud_optimized is None:
                self.convert_to_cloud_optimized = (
                    self.prompt_for_convert_to_cloud_optimized()
                )

            progress = progress_bar(
                total=len(self.tile_sources),
                unit="tile",
                desc=self.FETCH_OR_EXTRACT_TILE_METADATA_DESC,
            )
            with progress as p:
                for _ in self.fetch_or_extract_multiple_tiles_metadata(
                    self.tile_sources
                ):
                    p.update(1)

            self.check_metadata_pre_convert()

            # All these checks are just so we can give slightly better error messages
            # is the source wrong pre-conversion, or will it be wrong post-conversion?

            all_metadata = [s.metadata for s in self.tile_sources]
            merged_source_metadata = self.get_merged_source_metadata(all_metadata)
            self.check_for_non_homogenous_metadata(
                merged_source_metadata, future_tense=False
            )
            if self.include_existing_metadata:
                all_metadata.append(self.existing_metadata)
            self.predicted_merged_metadata = self.get_predicted_merged_metadata(
                all_metadata
            )
            self.check_for_non_homogenous_metadata(
                self.predicted_merged_metadata, future_tense=True
            )

        # Set up LFS hooks. This is also in `kart init`, but not every existing Kart repo will have these hooks.
        install_lfs_hooks(self.repo)

        # fast-import doesn't really have a way to amend a commit.
        # So we'll use a temporary branch for this fast-import,
        # And create a new commit on top of the head commit, without advancing HEAD.
        # Then we'll squash the two commits after the fast-import,
        # and move the HEAD branch to the new commit.
        # This also comes in useful for checking tree equivalence when --allow-empty is not used.
        fast_import_on_branch = f"refs/kart-import/{uuid.uuid4()}"
        if self.amend:
            if not self.repo.head_commit:
                raise InvalidOperation(
                    "Cannot amend in an empty repository", exit_code=NO_DATA
                )
            if not self.message:
                self.message = self.repo.head_commit.message
        else:
            if self.message is None:
                self.message = self.get_default_message()

        header = generate_header(
            self.repo, None, self.message, fast_import_on_branch, self.repo.head_commit
        )

        self.dataset_inner_path = (
            f"{self.dataset_path}/{self.DATASET_CLASS.DATASET_DIRNAME}"
        )

        with git_fast_import(
            self.repo, *FastImportSettings().as_args(), "--quiet"
        ) as proc:
            proc.stdin.write(header.encode("utf8"))
            self.write_extra_blobs(proc.stdin)

            if not self.update_existing:
                # Delete the entire existing dataset, before we re-import it.
                proc.stdin.write(f"D {self.dataset_path}\n".encode("utf8"))

            if self.delete:
                root_tree = self.repo.head_tree
                for tile_name in self.delete:
                    # Check that the blob exists; if not, error out
                    blob_path = self.existing_dataset.tilename_to_blob_path(tile_name)
                    try:
                        root_tree / blob_path
                    except KeyError:
                        raise NotFound(f"{tile_name} does not exist, can't delete it")

                    proc.stdin.write(f"D {blob_path}\n".encode("utf8"))

            if self.sources:
                self.import_tiles_to_stream(proc.stdin, self.tile_sources)

                all_metadata = (
                    [self.existing_metadata] if self.include_existing_metadata else []
                )
                all_metadata.extend(
                    source.imported_metadata
                    for source in self.tile_sources
                    if source.imported_metadata
                )
                self.actual_merged_metadata = self.get_actual_merged_metadata(
                    all_metadata
                )
                self.check_for_non_homogenous_metadata(
                    self.actual_merged_metadata, future_tense=True
                )
                self.write_meta_blobs_to_stream(proc.stdin, self.actual_merged_metadata)

        try:
            if self.amend:
                # Squash the commit we just created into its parent, replacing both commits on the head branch.
                new_tree = self.repo.references[fast_import_on_branch].peel(pygit2.Tree)
                new_commit_oid = self.repo.create_commit(
                    # Don't move a branch tip. pygit2 doesn't allow us to use head_branch here
                    # (because we're not using its tip as the first parent)
                    # so we just create a detached commit and then move the branch tip afterwards.
                    None,
                    self.repo.head_commit.author,
                    self.repo.committer_signature(),
                    self.message,
                    new_tree.oid,
                    self.repo.head_commit.parent_ids,
                )
            else:
                # Just reset the head branch tip to the new commit we created on the temp branch
                new_commit = self.repo.references[fast_import_on_branch].peel(
                    pygit2.Commit
                )
                new_commit_oid = new_commit.oid
                if (not self.allow_empty) and self.repo.head_tree:
                    if new_commit.peel(pygit2.Tree).oid == self.repo.head_tree.oid:
                        raise NotFound("No changes to commit", exit_code=NO_CHANGES)
            if self.repo.head_branch not in self.repo.references:
                # unborn head
                self.repo.references.create(self.repo.head_branch, new_commit_oid)
            else:
                self.repo.references[self.repo.head_branch].set_target(new_commit_oid)
        finally:
            # Clean up the temp branch
            self.repo.references[fast_import_on_branch].delete()
            # Clean up the temporary local copies of tiles (the tiles are now stored properly in the LFS cache).
            for source in self.tile_sources:
                source.cleanup()

        parts_to_create = [PartType.WORKDIR] if self.do_checkout else []
        self.repo.configure_do_checkout_datasets([self.dataset_path], self.do_checkout)
        # During imports we can keep old changes since they won't conflict with newly imported datasets.
        self.repo.working_copy.reset_to_head(
            repo_key_filter=RepoKeyFilter.datasets([self.dataset_path]),
            create_parts_if_missing=parts_to_create,
        )

    def infer_dataset_path(self, sources):
        """Given a list of sources to import, choose a reasonable name for the dataset."""
        names = set()
        parent_names = set()
        for source in sources:
            path = Path(source)
            names.add(self.DATASET_CLASS.remove_tile_extension(path.name))
            parent_names.add(path.parents[0].name if path.parents else "*")
        result = self._common_prefix(names)
        if result is None:
            result = self._common_prefix(parent_names)
        return result

    def _common_prefix(self, collection, min_length=4):
        prefix = os.path.commonprefix(list(collection))
        prefix = prefix.split("*", maxsplit=1)[0]
        prefix = prefix.rstrip("_-.,/\\")
        if len(prefix) < min_length:
            return None
        return prefix

    def preprocess_sources(self, sources):
        """
        Goes through the source specification as supplied by the user and makes a TileSource for each
        individual tile to be imported. Wildcards are expanded at this step.
        """
        # Sanity check - make sure we support this type of file / URL, make sure that the specified files exist.
        self.all_source_schemes = set()
        for source in sources:
            scheme = TileSource.parse_scheme(source)
            if scheme not in self.ALLOWED_SCHEMES:
                suffix = f", not a {scheme} URI" if scheme else ""
                raise click.UsageError(
                    f"SOURCE {source} should be {self.ALLOWED_SCHEMES_DESC}{suffix}"
                )
            if scheme is None and not (Path() / source).is_file():
                raise NotFound(f"No data found at {source}", exit_code=NO_IMPORT_SOURCE)
            self.all_source_schemes.add(scheme)

        result = []
        for source in sources:
            if "*" in source:
                for match in self.expand_source_wildcard(source):
                    result.append(TileSource(match))
            else:
                result.append(TileSource(source))
        return result

    def expand_source_wildcard(self, source):
        """Given a source with a wildcard '*' in it, expand it into the list of sources it represents."""
        scheme = TileSource.parse_scheme(source)
        if scheme == "s3":
            return expand_s3_glob(source)
        elif scheme is None:
            expanded = glob.glob(source)
            if not expanded:
                raise NotFound(f"No data found at {source}", exit_code=NO_IMPORT_SOURCE)
            return expanded
        else:
            raise click.UsageError(
                f"SOURCE {source} should be {self.ALLOWED_SCHEMES_DESC}, not a {scheme} URI"
            )

    def get_default_message(self):
        """Return a default commit message to describe this import."""
        raise NotImplementedError()

    def fetch_or_extract_tile_metadata(self, tile_source):
        """
        Read the metadata for the given tile source. Includes both "dataset" metadata and "tile" metadata -
        that is, metadata that we expect to be homogenous for a dataset, such as the CRS,
        and metadata that we expect to vary per tile, such as the extent.
        """
        if self.do_fetch_tiles and not tile_source.local_path:
            tile_source.fetch_if_remote(self.repo.lfs_tmp_path)
        # We always fetch the entire sidecar files even when remote - they are small compared to tiles.
        tile_source.find_or_fetch_sidecar_files(
            self.repo.lfs_tmp_path, self.SIDECAR_FILES
        )
        metadata = tile_source.extract_metadata(self)
        if self.do_link:
            metadata["tile"]["url"] = tile_source.spec
            for prefix, suffix in self.SIDECAR_FILES.items():
                if f"{prefix}Oid" in metadata["tile"]:
                    metadata["tile"][f"{prefix}Url"] = tile_source.spec + suffix

        return metadata

    def fetch_or_extract_multiple_tiles_metadata(self, tile_sources):
        """
        Like fetch_or_extract_tile_metadata, but works for a list of several tiles. The metadata may
        be extracted serially or with a thread-pool, depending on the value of self.num_workers.
        """
        # Single-threaded variant - uses the calling thread.
        if self.num_workers == 1:
            for source in tile_sources:
                yield self.fetch_or_extract_tile_metadata(source)
            return

        # Multi-worker variant - uses a thread-pool, calling thread just receives the results.
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.num_workers
        ) as executor:
            futures = [
                executor.submit(self.fetch_or_extract_tile_metadata, source)
                for source in tile_sources
            ]
            for future in concurrent.futures.as_completed(futures):
                yield future.result()

    def extract_tile_metadata(self, tile_path, **kwargs):
        return self.DATASET_CLASS.extract_tile_metadata(tile_path, **kwargs)

    def check_metadata_pre_convert(self):
        """
        Use the self.source_to_metadata dict to see if any of the sources individually have properties that prevent
        from being imported as required. This is separate to homogeneity checks.
        """
        raise NotImplementedError()

    def check_metadata_post_convert(self):
        """
        Use the self.source_to_metadata dict to see if any of the sources individually have properties that prevent
        from being imported as required. This is separate to homogeneity checks.
        """
        raise NotImplementedError()

    def get_merged_source_metadata(self, all_metadata):
        """
        Merge all of the source metadata into a single piece of metadata. Drop any fields that are per-tile and so
        cannot be merged. Drop any fields that would change during the conversion step.
        Use ListOfConflicts objects to mark any fields that are expected to be homogenous but are not.
        These will be raised in the following manner: "The input files have more than one..."
        """
        raise NotImplementedError()

    def get_predicted_merged_metadata(self, all_metadata):
        """
        Without doing a full conversion, predict what the given metadata would look like once converted, then merge.
        Drop any fields that are per-tile and so cannot be merged. Drop any fields where the converted form cannot be
        preficted. Use ListOfConflicts objects to mark any fields that are expected to be homogenous but are not.
        These will be raised in the following manner: "The imported files would have more than one..."
        """
        raise NotImplementedError()

    def get_actual_merged_metadata(self, all_metadata):
        """
        Once any necessary conversion is done, this merges the metadata extracted from the converted tiles.
        Drop any fields that are per-tile and so cannot be merged.
        Use ListOfConflicts objects to mark any fields that are expected to be homogenous but are not.
        These will be raised in the following manner: "The imported files would have more than one..."
        """
        raise NotImplementedError()

    def get_existing_dataset(self):
        """Return the dataset to be updated / replaced that already exists at self.dataset_path, if any."""
        result = self.repo.datasets().get(self.dataset_path)
        if result and result.DATASET_DIRNAME != self.DATASET_CLASS.DATASET_DIRNAME:
            raise InvalidOperation(
                f"A dataset of type {result.DATASET_DIRNAME} already exists at {self.dataset_path}"
            )
        return result

    def write_extra_blobs(self, stream):
        # We still need to write .kart.repostructure.version unfortunately, even though it's only relevant to tabular datasets.
        extra_blobs = (
            extra_blobs_for_version(self.repo.table_dataset_version)
            if not self.repo.head_commit
            else []
        )
        for i, blob_path in write_blobs_to_stream(stream, extra_blobs):
            pass

    def check_for_non_homogenous_metadata(self, merged_metadata, future_tense=False):
        for key in merged_metadata:
            if key == "tile":
                # This is the metadata we treat as "tile-specific" - we don't expect it to be homogenous.
                continue
            self._check_for_non_homogenous_meta_item(
                merged_metadata, key, future_tense=future_tense
            )

    HUMAN_READABLE_META_ITEM_NAMES = {
        "format.json": "file format",
        "schema.json": "schema",
        "crs.wkt": "CRS",
    }

    def _check_for_non_homogenous_meta_item(
        self, merged_metadata, key, future_tense=False
    ):
        output_name = self.HUMAN_READABLE_META_ITEM_NAMES.get(key, key)
        value = merged_metadata[key]

        if not isinstance(value, ListOfConflicts):
            return

        format_func = (
            format_wkt_for_output if key.endswith(".wkt") else format_json_for_output
        )
        disparity = " vs \n".join(
            (format_func(value, sys.stderr) for value in merged_metadata[key])
        )
        click.echo(
            f"Kart constrains certain aspects of {self.DATASET_CLASS.DATASET_TYPE} datasets to be homogenous.",
            err=True,
        )
        if future_tense:
            click.echo(
                f"The imported files would have more than one {output_name}:",
                err=True,
            )
        else:
            click.echo(f"The input files have more than one {output_name}:", err=True)
        click.echo(disparity, err=True)
        raise InvalidOperation(
            "Non-homogenous dataset supplied",
            exit_code=WORKING_COPY_OR_IMPORT_CONFLICT,
        )

    def get_conversion_func(self, tile_source):
        """
        Given the metadata for a particular tile, return a function to convert it as required during import
        - eg, to make it cloud-optimized if required - or None if nothing is required.
        The conversion function has the following interface: convert(source, dest)
        where source is the path to the tile pre-conversion, and dest is the path where the converted tile is written.
        """
        raise NotImplementedError()

    def import_tiles_to_stream(self, stream, tile_sources):
        already_imported = set()

        progress = progress_bar(
            total=len(tile_sources), unit="tile", desc=self.IMPORT_TILES_TO_STREAM_DESC
        )
        with progress as p:
            # First pass - check if the tile is already imported.
            # If already-imported tiles are found, they can be skipped in the second pass.
            # This is fast so we just do it up-front on the calling thread.
            if self.existing_dataset is not None:
                for source in tile_sources:
                    tilename = self.DATASET_CLASS.tilename_from_path(source.spec)
                    existing_summary = self.existing_dataset.get_tile_summary(
                        tilename, missing_ok=True
                    )
                    if not existing_summary:
                        continue
                    if not self.existing_tile_matches_source(
                        source.oid, existing_summary
                    ):
                        continue
                    # This tile has already been imported before. Reuse it rather than re-importing it.
                    # Re-importing it could cause it to be re-converted, which is a waste of time,
                    # and it may not convert the same the second time, which is then a waste of space
                    # and shows up as a pointless diff.
                    rel_blob_path = self.DATASET_CLASS.tilename_to_blob_path(
                        tilename, relative=True
                    )
                    blob_path = f"{self.dataset_inner_path}/{rel_blob_path}"
                    prev_imported_blob_data = (
                        self.existing_dataset.inner_tree / rel_blob_path
                    ).data
                    write_blob_to_stream(stream, blob_path, prev_imported_blob_data)
                    source.imported_metadata = None
                    self.include_existing_metadata = True
                    already_imported.add(source)
                    p.update(1)

            # Second pass - actually convert / hash / copy the tile. This part can be multi-worker.
            not_yet_imported = [
                source for source in tile_sources if source not in already_imported
            ]
            for source, imported_metadata in self.copy_or_convert_multiple_tiles(
                not_yet_imported
            ):
                tile_metadata, sidecar_metadata = self.separate_sidecar_metadata(
                    imported_metadata["tile"]
                )

                tilename = self.DATASET_CLASS.tilename_from_path(source.spec)
                rel_blob_path = self.DATASET_CLASS.tilename_to_blob_path(
                    tilename, relative=True
                )
                blob_path = f"{self.dataset_inner_path}/{rel_blob_path}"
                write_blob_to_stream(
                    stream, blob_path, dict_to_pointer_file_bytes(tile_metadata)
                )

                for suffix, metadata in sidecar_metadata.items():
                    write_blob_to_stream(
                        stream, blob_path + suffix, dict_to_pointer_file_bytes(metadata)
                    )

                p.update(1)

    def copy_or_convert_multiple_tiles(self, tile_sources):
        """
        Calls copy_or_convert on each TileSource in tile_sources, which causes each tile to be simply copied
        from its source into the LFS cache, or converted (if self.convert_to_cloud_optimized is True) with
        the converted result placed in the LFS cache.
        """
        # Single-threaded variant - uses the calling thread.
        if self.num_workers == 1:
            for source in tile_sources:
                yield source, source.copy_or_convert(self)
            return

        # Multi-worker variant - uses a thread-pool, calling thread just receives the results.
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.num_workers
        ) as executor:
            future_to_source = {
                executor.submit(source.copy_or_convert, self): source
                for source in tile_sources
            }
            for future in concurrent.futures.as_completed(future_to_source):
                yield future_to_source[future], future.result()

    def write_meta_blobs_to_stream(self, stream, merged_metadata):
        """Writes the format.json, schema.json and crs.wkt meta items to the dataset."""
        if self.do_link:
            merged_metadata = {
                **merged_metadata,
                "linked-storage.json": {"urlRedirects": {}},
            }

        for key, value in merged_metadata.items():
            definition = self.DATASET_CLASS.get_meta_item_definition(key)
            file_type = MetaItemFileType.get_from_definition_or_suffix(definition, key)
            write_blob_to_stream(
                stream,
                f"{self.dataset_inner_path}/meta/{key}",
                file_type.encode_to_bytes(value),
            )

    def missing_parameter(self, param_name):
        """Raise a MissingParameter exception."""
        return click.MissingParameter(param=find_param(self.ctx, param_name))

    def separate_sidecar_metadata(self, all_tile_metadata):
        """
        Puts all the side-car prefixed keys eg "pam..." into separate dict(s) from the main tile_metadata dict.
        """
        if not self.SIDECAR_FILES:
            return all_tile_metadata, {}

        def _remove_prefix(key, prefix):
            key = key[len(prefix) :]
            return key[0].lower() + key[1:]

        tile_metadata = {}
        sidecar_metadata = {}
        for key, value in all_tile_metadata.items():
            for prefix, suffix in self.SIDECAR_FILES.items():
                if key.startswith(prefix):
                    sidecar_metadata.setdefault(suffix, {})[
                        _remove_prefix(key, prefix)
                    ] = value
                    break
            else:
                tile_metadata[key] = value

        return tile_metadata, sidecar_metadata

    def check_num_workers(self, num_workers):
        if num_workers is None:
            return self.get_default_num_workers()
        else:
            return max(1, num_workers)

    def get_default_num_workers(self):
        num_workers = get_num_available_cores()
        # that's a float, but we need an int
        return max(1, int(math.ceil(num_workers)))

    def prompt_for_convert_to_cloud_optimized(self):
        variant = self.CLOUD_OPTIMIZED_VARIANT
        acronym = self.CLOUD_OPTIMIZED_VARIANT_ACRONYM
        message = (
            f"Datasets that contain only {variant} files ({acronym} files) "
            "are better suited to viewing on the web, and it's easier to decide up front to exclusively store "
            "cloud-optimized tiles than to make the switch later on."
        )

        if get_input_mode() == InputMode.NO_INPUT:
            click.echo(message, err=True)
            click.echo(
                f"Add the --cloud-optimized option to import these tiles into a {acronym} dataset, converting them to {acronym} files where needed.",
                err=True,
            )
            click.echo(
                f"Or, add the --preserve-format option to import these tiles as-is into a dataset that allows both {acronym} and non-{acronym} tiles.",
                err=True,
            )
            raise click.UsageError("Choose dataset subtype")

        click.echo(message)
        return click.confirm(
            f"Import these tiles into a {acronym}-only dataset, converting them to {acronym} files where needed?",
        )
