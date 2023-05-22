import concurrent.futures
import functools
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
    merge_dicts_to_pointer_file_bytes,
    dict_to_pointer_file_bytes,
    copy_file_to_local_lfs_cache,
)
from kart.list_of_conflicts import ListOfConflicts
from kart.meta_items import MetaItemFileType
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

    def __init__(self, repo, ctx):
        self.repo = repo
        self.ctx = ctx

        # When doing any kind of initial import we still have to write the table_dataset_version,
        # even though it's not really relevant to tile imports.
        assert self.repo.table_dataset_version in SUPPORTED_VERSIONS

    def import_tiles(
        self,
        *,
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
        sources,
    ):
        """
        Import the tiles at sources as a new dataset / use them to update an existing dataset.

        dataset_path - path to the dataset where the tiles will be imported
        convert_to_cloud_optimized - whether to automatically convert tiles to cloud-optimized (COPC or COG) while importing.
            If True, the resulting dataset will also be constrained to only contain cloud-optimized tiles.
            If False, the user has explicitly opted out of this constraint.
            If None, we still need to check if the user is aware of this possibility and prompt them to see what they would prefer.
        message - commit message for the import commit
        do_checkout - Whether to create a working copy once the import is finished, if no working copy exists yet
        replace_existing - if True, replace any existing dataset at dataset_path with a new one containing only these tiles
        update_existing - if True, update any existing dataset at the same path. Existing tiles will be replaced by source
            tiles with the same name, other existing tiles remain unchanged.
        delete - list of existing tiles to delete, relevant when updating an existing dataset
        amend - if True, amends the previous commit rather than creating a new import commit
        allow_empty - if True, the import commit will be created even if the dataset is not changed
        num_workers - specify the number of workers to use, or set to None to use the number of detected cores.
        sources - paths to tiles to import
        """

        self.sources = sources
        self.num_workers = self.check_num_workers(num_workers)

        if not sources and not delete:
            # sources aren't required if you use --delete;
            # this allows you to use this command to solely delete tiles.
            # otherwise, sources are required.
            raise self.missing_parameter("args")

        if delete and not dataset_path:
            # Dataset-path is required if you use --delete.
            raise self.missing_parameter("dataset_path")

        if not dataset_path:
            dataset_path = self.infer_dataset_path(sources)
            if dataset_path:
                click.echo(f"Defaulting to '{dataset_path}' as the dataset path...")
            else:
                raise self.missing_parameter("dataset_path")

        self.dataset_path = dataset_path

        if delete:
            # --delete kind of implies --update-existing (we're modifying an existing dataset)
            # But a common way for this to do the wrong thing might be this:
            #   kart ... --delete auckland/auckland_3_*.laz
            # i.e. if --delete is used with a glob, then we don't want to treat the remaining paths as
            # sources and import them. In that case, *not* setting update_existing here will fall through
            # to cause an error below:
            #  * either the dataset exists, and we fail with a dataset conflict
            #  * or the dataset doesn't exist, and the --delete fails
            if not sources:
                update_existing = True

        if replace_existing or update_existing:
            validate_dataset_paths([dataset_path])
        else:
            old_dataset_paths = [ds.path for ds in self.repo.datasets()]
            validate_dataset_paths([*old_dataset_paths, dataset_path])

        if (
            replace_existing or update_existing or delete
        ) and self.repo.working_copy.workdir:
            # Avoid conflicts by ensuring the WC is clean.
            # NOTE: Technically we could allow anything to be dirty except the single dataset
            # we're importing (or even a subset of that dataset). But this'll do for now
            self.repo.working_copy.workdir.check_not_dirty()

        for source in sources:
            if not (Path() / source).is_file():
                raise NotFound(f"No data found at {source}", exit_code=NO_IMPORT_SOURCE)

        self.existing_dataset = self.get_existing_dataset()
        self.existing_metadata = (
            self.existing_dataset.tile_metadata if self.existing_dataset else None
        )
        self.include_existing_metadata = (
            update_existing and self.existing_dataset is not None
        )

        if delete and self.existing_dataset is None:
            # Trying to delete specific paths from a nonexistent dataset?
            # This suggests the caller is confused.
            raise InvalidOperation(
                f"Dataset {dataset_path} does not exist. Cannot delete paths from it."
            )

        # These two dicts contain information about the sources, pre-conversion.
        self.source_to_metadata = {}
        self.source_to_hash_and_size = {}

        if sources:
            self.convert_to_cloud_optimized = convert_to_cloud_optimized
            if self.convert_to_cloud_optimized is None:
                self.convert_to_cloud_optimized = (
                    self.prompt_for_convert_to_cloud_optimized()
                )

            progress = progress_bar(
                total=len(sources), unit="tile", desc="Checking tiles"
            )
            with progress as p:
                for source, tile_metadata in self.extract_multiple_tiles_metadata(
                    sources
                ):
                    self.source_to_metadata[source] = tile_metadata
                    self.source_to_hash_and_size[source] = (
                        tile_metadata["tile"]["oid"],
                        tile_metadata["tile"]["size"],
                    )
                    p.update(1)

            self.check_metadata_pre_convert()

            # All these checks are just so we can give slightly better error messages
            # is the source wrong pre-conversion, or will it be wrong post-conversion?

            all_metadata = list(self.source_to_metadata.values())
            merged_source_metadata = self.get_merged_source_metadata(all_metadata)
            self.check_for_non_homogenous_metadata(
                merged_source_metadata, future_tense=0
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

        # Metadata in this dict is updated as we convert some or all tiles to COPC.
        self.source_to_imported_metadata = {}

        # fast-import doesn't really have a way to amend a commit.
        # So we'll use a temporary branch for this fast-import,
        # And create a new commit on top of the head commit, without advancing HEAD.
        # Then we'll squash the two commits after the fast-import,
        # and move the HEAD branch to the new commit.
        # This also comes in useful for checking tree equivalence when --allow-empty is not used.
        fast_import_on_branch = f"refs/kart-import/{uuid.uuid4()}"
        if amend:
            if not self.repo.head_commit:
                raise InvalidOperation(
                    "Cannot amend in an empty repository", exit_code=NO_DATA
                )
            if not message:
                message = self.repo.head_commit.message
        else:
            if message is None:
                message = self.get_default_message()

        header = generate_header(
            self.repo, None, message, fast_import_on_branch, self.repo.head_commit
        )

        self.dataset_inner_path = (
            f"{self.dataset_path}/{self.DATASET_CLASS.DATASET_DIRNAME}"
        )

        with git_fast_import(
            self.repo, *FastImportSettings().as_args(), "--quiet"
        ) as proc:
            proc.stdin.write(header.encode("utf8"))
            self.write_extra_blobs(proc.stdin)

            if not update_existing:
                # Delete the entire existing dataset, before we re-import it.
                proc.stdin.write(f"D {dataset_path}\n".encode("utf8"))

            if delete:
                root_tree = self.repo.head_tree
                for tile_name in delete:
                    # Check that the blob exists; if not, error out
                    blob_path = self.existing_dataset.tilename_to_blob_path(tile_name)
                    try:
                        root_tree / blob_path
                    except KeyError:
                        raise NotFound(f"{tile_name} does not exist, can't delete it")

                    proc.stdin.write(f"D {blob_path}\n".encode("utf8"))

            if sources:
                self.import_tiles_to_stream(proc.stdin, sources)

                all_metadata = (
                    [self.existing_metadata] if self.include_existing_metadata else []
                )
                all_metadata.extend(self.source_to_imported_metadata.values())
                self.actual_merged_metadata = self.get_actual_merged_metadata(
                    all_metadata
                )
                self.check_for_non_homogenous_metadata(
                    self.actual_merged_metadata, future_tense=True
                )
                self.write_meta_blobs_to_stream(proc.stdin, self.actual_merged_metadata)

        try:
            if amend:
                # Squash the commit we just created into its parent, replacing both commits on the head branch.
                new_tree = self.repo.references[fast_import_on_branch].peel(pygit2.Tree)
                new_commit_oid = self.repo.create_commit(
                    # Don't move a branch tip. pygit2 doesn't allow us to use head_branch here
                    # (because we're not using its tip as the first parent)
                    # so we just create a detached commit and then move the branch tip afterwards.
                    None,
                    self.repo.head_commit.author,
                    self.repo.committer_signature(),
                    message,
                    new_tree.oid,
                    self.repo.head_commit.parent_ids,
                )
            else:
                # Just reset the head branch tip to the new commit we created on the temp branch
                new_commit = self.repo.references[fast_import_on_branch].peel(
                    pygit2.Commit
                )
                new_commit_oid = new_commit.oid
                if (not allow_empty) and self.repo.head_tree:
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

        parts_to_create = [PartType.WORKDIR] if do_checkout else []
        # During imports we can keep old changes since they won't conflict with newly imported datasets.
        self.repo.working_copy.reset_to_head(
            repo_key_filter=RepoKeyFilter.datasets([dataset_path]),
            create_parts_if_missing=parts_to_create,
        )

    def infer_dataset_path(self, sources):
        """Given a list of sources to import, choose a reasonable name for the dataset."""
        if len(sources) == 1:
            return self.DATASET_CLASS.remove_tile_extension(Path(sources[0]).name)
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

    def get_default_message(self):
        """Return a default commit message to describe this import."""
        raise NotImplementedError()

    def extract_tile_metadata(self, tile_path):
        """
        Read the metadata for the given tile. Includes both "dataset" metadata and "tile" metadata -
        that is, metadata that we expect to be homogenous for a dataset, such as the CRS,
        and metadata that we expect to vary per tile, such as the extent.
        """
        return self.DATASET_CLASS.extract_tile_metadata_from_filesystem_path(tile_path)

    def extract_multiple_tiles_metadata(self, sources):
        """
        Like extract_tile_metadata, but works for a list of several tiles. The metadata may
        be extracted serially or with a thread-pool, depending on the value of self.num_workers.
        Yields a tuple (source, metadata) for each tile in turn, in some unspecified order.
        """
        # Single-threaded variant - uses the calling thread.
        if self.num_workers == 1:
            for source in sources:
                yield source, self.extract_tile_metadata(source)
            return

        # Multi-worker variant - uses a thread-pool, calling thread just receives the results.
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.num_workers
        ) as executor:
            future_to_source = {
                executor.submit(self.extract_tile_metadata, source): source
                for source in sources
            }
            for future in concurrent.futures.as_completed(future_to_source):
                source = future_to_source[future]
                yield source, future.result()

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

    def get_conversion_func(self, source_metadata):
        """
        Given the metadata for a particular tile, return a function to convert it as required during import
        - eg, to make it cloud-optimized if required -  or None if nothing is required.
        The conversion function has the following interface: convert(source, dest)
        where source is the path to the tile pre-conversion,
        and dest is the path where the converted tile is written.
        """
        raise NotImplementedError()

    def wrap_conversion_func(self, conversion_func):
        """
        Given a conversion function - as produced by get_conversion_func - creates a wrapped
        version of it that also updates self.source_to_imported_metadata once the conversion completes.
        """
        if conversion_func is None:
            return None

        def wrapped_func(source, dest):
            conversion_func(source, dest)
            self.source_to_imported_metadata[source] = self.extract_tile_metadata(dest)
            source_oid = self.source_to_hash_and_size[source][0]
            if not source_oid.startswith("sha256:"):
                source_oid = "sha256:" + source_oid
            self.source_to_imported_metadata[source]["tile"]["sourceOid"] = source_oid

        return wrapped_func

    def import_tiles_to_stream(self, stream, sources):
        already_imported = 0
        copy_and_convert_tasks = {}

        # First pass - check if the tile is already imported or if it needs to be converted,
        # set up a callable function that will convert / hash / copy the tile to the right place as needed.
        # This part is fast and runs single-threaded.
        for source in sources:
            source_metadata = self.source_to_metadata[source]
            tilename = self.DATASET_CLASS.tilename_from_path(source)
            rel_blob_path = self.DATASET_CLASS.tilename_to_blob_path(
                tilename, relative=True
            )
            blob_path = f"{self.dataset_inner_path}/{rel_blob_path}"

            # Check if tile has already been imported previously:
            if self.existing_dataset is not None:
                existing_summary = self.existing_dataset.get_tile_summary(
                    tilename, missing_ok=True
                )
                if existing_summary:
                    source_oid = self.source_to_hash_and_size[source][0]
                    if self.existing_tile_matches_source(source_oid, existing_summary):
                        # This tile has already been imported before. Reuse it rather than re-importing it.
                        # Re-importing it could cause it to be re-converted, which is a waste of time,
                        # and it may not convert the same the second time, which is then a waste of space
                        # and shows up as a pointless diff.
                        write_blob_to_stream(
                            stream,
                            blob_path,
                            (self.existing_dataset.inner_tree / rel_blob_path).data,
                        )
                        self.include_existing_metadata = True
                        already_imported += 1
                        continue

            conversion_func = self.wrap_conversion_func(
                self.get_conversion_func(source_metadata)
            )
            if conversion_func is None:
                self.source_to_imported_metadata[source] = self.source_to_metadata[
                    source
                ]
                oid_and_size = self.source_to_hash_and_size[source]
            else:
                oid_and_size = None

            copy_and_convert_tasks[source] = functools.partial(
                copy_file_to_local_lfs_cache,
                self.repo,
                source,
                conversion_func,
                oid_and_size=oid_and_size,
            )

        # Second pass - actually convert / hash / copy the tile. This part can be multi-worker.
        progress = progress_bar(total=len(sources), unit="tile", desc="Importing tiles")
        with progress as p:
            p.update(already_imported)

            for source, pointer_dict in self.copy_multiple_files_to_lfs_cache(
                copy_and_convert_tasks
            ):
                pointer_data = merge_dicts_to_pointer_file_bytes(
                    self.source_to_imported_metadata[source]["tile"], pointer_dict
                )

                tilename = self.DATASET_CLASS.tilename_from_path(source)
                rel_blob_path = self.DATASET_CLASS.tilename_to_blob_path(
                    tilename, relative=True
                )
                blob_path = f"{self.dataset_inner_path}/{rel_blob_path}"
                write_blob_to_stream(stream, blob_path, pointer_data)

                for sidecar_file, suffix in self.sidecar_files(source):
                    pointer_dict = copy_file_to_local_lfs_cache(self.repo, sidecar_file)
                    pointer_data = dict_to_pointer_file_bytes(pointer_dict)
                    write_blob_to_stream(stream, blob_path + suffix, pointer_data)

                p.update(1)

    def copy_multiple_files_to_lfs_cache(self, copy_and_convert_tasks):
        """
        Runs all the supplied tasks which hash / convert / copy the source files to the LFS cache.
        Tasks may be run in series or by a threadpool, depending on self.num_workers.
        Yields a tuple (source, pointer_dict) for each tile in turn, in some unspecified order, where
        pointer_dict contains the OID and size that should be written as a pointer-file in order to
        reference the tile that has been imported to the LFS cache.

        copy_and_convert_tasks - a dict keyed by the source file, as supplied by user, where each
            value is a task to be run that hashes / converts / copies the tile to the LFS cache as required.
        """
        # Single-threaded variant - uses the calling thread.
        if self.num_workers == 1:
            for source, task in copy_and_convert_tasks.items():
                yield source, task()
            return

        # Multi-worker variant - uses a thread-pool, calling thread just receives the results.
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.num_workers
        ) as executor:
            future_to_source = {
                executor.submit(task): source
                for source, task in copy_and_convert_tasks.items()
            }
            for future in concurrent.futures.as_completed(future_to_source):
                source = future_to_source[future]
                yield source, future.result()

    def write_meta_blobs_to_stream(self, stream, merged_metadata):
        """Writes the format.json, schema.json and crs.wkt meta items to the dataset."""
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

    def sidecar_files(self, source):
        return []

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
