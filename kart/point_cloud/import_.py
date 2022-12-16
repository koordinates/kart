import logging
import os
import uuid
from pathlib import Path

import click
import pygit2

from kart.cli_util import StringFromFile, MutexOption, KartCommand, find_param
from kart.completion_shared import file_path_completer
from kart.crs_util import normalise_wkt
from kart.dataset_util import validate_dataset_paths
from kart.exceptions import (
    InvalidOperation,
    NotFound,
    NO_IMPORT_SOURCE,
    INVALID_FILE_FORMAT,
    NO_DATA,
    NO_CHANGES,
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
    copy_file_to_local_lfs_cache,
    get_hash_and_size_of_file,
)
from kart.parse_args import parse_import_sources_and_datasets
from kart.point_cloud.metadata_util import (
    RewriteMetadata,
    extract_pc_tile_metadata,
    rewrite_and_merge_metadata,
    check_for_non_homogenous_metadata,
    format_tile_for_pointer_file,
)
from kart.point_cloud.pdal_convert import convert_tile_to_copc
from kart.point_cloud.v1 import PointCloudV1
from kart.serialise_util import json_pack, ensure_bytes
from kart.tabular.version import (
    SUPPORTED_VERSIONS,
    extra_blobs_for_version,
)
from kart.point_cloud.tilename_util import remove_tile_extension
from kart.working_copy import PartType


L = logging.getLogger(__name__)


@click.command("point-cloud-import", hidden=True, cls=KartCommand)
@click.pass_context
@click.option(
    "--convert-to-copc/--no-convert-to-copc",
    " /--preserve-format",
    is_flag=True,
    default=True,
    help="Whether to convert all non-COPC LAS or LAZ files to COPC LAZ files, or to import all files in their native format.",
)
@click.option(
    "--message",
    "-m",
    type=StringFromFile(encoding="utf-8"),
    help="Commit message. By default this is auto-generated.",
)
@click.option(
    "--checkout/--no-checkout",
    "do_checkout",
    is_flag=True,
    default=True,
    help="Whether to create a working copy once the import is finished, if no working copy exists yet.",
)
@click.option(
    "--replace-existing",
    is_flag=True,
    cls=MutexOption,
    exclusive_with=["--delete", "--update-existing"],
    help="Replace existing dataset at the same path.",
)
@click.option(
    "--update-existing",
    is_flag=True,
    cls=MutexOption,
    exclusive_with=["--replace-existing"],
    help=(
        "Update existing dataset at the same path. "
        "Tiles will be replaced by source tiles with the same name. "
        "Tiles in the existing dataset which are not present in SOURCES will remain untouched."
    ),
)
@click.option(
    "--delete",
    type=StringFromFile(encoding="utf-8"),
    cls=MutexOption,
    exclusive_with=["--replace-existing"],
    multiple=True,
    help=("Deletes the given tile. Can be used multiple times."),
)
@click.option(
    "--amend",
    default=False,
    is_flag=True,
    help="Amend the previous commit instead of adding a new commit",
)
@click.option(
    "--allow-empty",
    is_flag=True,
    default=False,
    help=(
        "Usually recording a commit that has the exact same tree as its sole "
        "parent commit is a mistake, and the command prevents you from making "
        "such a commit. This option bypasses the safety"
    ),
)
@click.option(
    "--num-processes",
    help="Parallel import using multiple processes. Not yet supported",
    default=None,
    hidden=True,
)
@click.option(
    "--dataset-path", "--dataset", "ds_path", help="The dataset's path once imported"
)
@click.argument(
    "args",
    nargs=-1,
    metavar="SOURCE [SOURCES...]",
    shell_complete=file_path_completer,
)
def point_cloud_import(
    ctx,
    convert_to_copc,
    ds_path,
    message,
    do_checkout,
    replace_existing,
    update_existing,
    delete,
    amend,
    allow_empty,
    num_processes,
    args,
):
    """
    Experimental command for importing point cloud datasets. Work-in-progress.
    Will eventually be merged with the main `import` command.

    SOURCES should be one or more LAZ or LAS files (or wildcards that match multiple LAZ or LAS files).
    """
    repo = ctx.obj.repo

    sources, datasets = parse_import_sources_and_datasets(args)
    if datasets:
        problem = "    \n".join(datasets)
        raise click.UsageError(
            f"For point-cloud import, every argument should be a LAS/LAZ file:\n    {problem}"
        )

    if not sources and not delete:
        # sources aren't required if you use --delete;
        # this allows you to use this command to solely delete tiles.
        # otherwise, sources are required.
        raise click.MissingParameter(param=find_param(ctx, "args"))

    if delete and not ds_path:
        # Dataset-path is required if you use --delete.
        raise click.MissingParameter(param=find_param(ctx, "ds_path"))

    if not ds_path:
        ds_path = infer_ds_path(sources)
        if ds_path:
            click.echo(f"Defaulting to '{ds_path}' as the dataset path...")
        else:
            raise click.MissingParameter(param=find_param(ctx, "ds_path"))

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
        validate_dataset_paths([ds_path])
    else:
        old_ds_paths = [ds.path for ds in repo.datasets()]
        validate_dataset_paths([*old_ds_paths, ds_path])

    if (replace_existing or update_existing or delete) and repo.working_copy.workdir:
        # Avoid conflicts by ensuring the WC is clean.
        # NOTE: Technically we could allow anything to be dirty except the single dataset
        # we're importing (or even a subset of that dataset). But this'll do for now
        repo.working_copy.workdir.check_not_dirty()

    for source in sources:
        if not (Path() / source).is_file():
            raise NotFound(f"No data found at {source}", exit_code=NO_IMPORT_SOURCE)

    source_to_metadata = {}
    source_to_hash_and_size = {}

    if sources:
        for source in sources:
            click.echo(f"Checking {source}...          \r", nl=False)
            source_to_metadata[source] = extract_pc_tile_metadata(source)
            source_to_hash_and_size[source] = get_hash_and_size_of_file(source)
        click.echo()

    if not convert_to_copc:
        if any(
            v["format"]["compression"] == "las" for v in source_to_metadata.values()
        ):
            raise InvalidOperation(
                "LAS datasets are not supported - dataset must be converted to LAZ / COPC",
                exit_code=INVALID_FILE_FORMAT,
            )

    if convert_to_copc:
        # As we check the sources for validity, we care about what the schema will be when we convert to COPC.
        # We don't need to check the format since if a set of tiles are all COPC and all have the same schema,
        # then they all will have the same format. Also, we would rather show the user that, post-conversion, the
        # tile's schema's won't match - quite a concrete idea even for those new to the LAZ format - rather than
        # trying to explain that post-conversion, the tile's Point Data Record Format numbers won't match.
        rewrite_metadata = (
            RewriteMetadata.AS_IF_CONVERTED_TO_COPC | RewriteMetadata.DROP_FORMAT
        )
    else:
        # For --preserve-format we allow both COPC and non-COPC tiles, so we don't need to check or store this information.
        rewrite_metadata = RewriteMetadata.DROP_OPTIMIZATION

    if sources:
        merged_metadata = rewrite_and_merge_metadata(
            source_to_metadata.values(), rewrite_metadata
        )
        check_for_non_homogenous_metadata(merged_metadata, convert_to_copc)

    # Set up LFS hooks. This is also in `kart init`, but not every existing Kart repo will have these hooks.
    install_lfs_hooks(repo)

    # We still need to write .kart.repostructure.version unfortunately, even though it's only relevant to tabular datasets.
    assert repo.table_dataset_version in SUPPORTED_VERSIONS
    extra_blobs = (
        extra_blobs_for_version(repo.table_dataset_version)
        if not repo.head_commit
        else []
    )

    ds_inner_path = f"{ds_path}/.point-cloud-dataset.v1"

    def convert_tile_to_copc_and_reextract_metadata(source, dest):
        nonlocal source_to_metadata

        convert_tile_to_copc(source, dest)
        source_to_metadata[source] = extract_pc_tile_metadata(dest, extract_schema=True)
        source_hash = "sha256:" + source_to_hash_and_size[source][0]
        source_to_metadata[source]["tile"]["sourceOid"] = source_hash

    # fast-import doesn't really have a way to amend a commit.
    # So we'll use a temporary branch for this fast-import,
    # And create a new commit on top of the head commit, without advancing HEAD.
    # Then we'll squash the two commits after the fast-import,
    # and move the HEAD branch to the new commit.
    # This also comes in useful for checking tree equivalence when --allow-empty is not used.
    fast_import_on_branch = f"refs/kart-import/{uuid.uuid4()}"
    if amend:
        if not repo.head_commit:
            raise InvalidOperation(
                "Cannot amend in an empty repository", exit_code=NO_DATA
            )
        if not message:
            message = repo.head_commit.message
    else:
        if message is None:
            message = f"Importing {len(sources)} LAZ tiles as {ds_path}"

    header = generate_header(
        repo,
        None,
        message,
        fast_import_on_branch,
        repo.head_commit,
    )

    with git_fast_import(repo, *FastImportSettings().as_args(), "--quiet") as proc:
        proc.stdin.write(header.encode("utf8"))

        all_metadatas = []
        existing_dataset = None
        if update_existing:
            try:
                existing_dataset = repo.datasets()[ds_path]
            except KeyError:
                # Should it be an error to use --update-existing for a new dataset?
                # Potentially not; it might be useful for callers to be agnostic
                # about whether a dataset exists yet.
                existing_dataset = None
            else:
                # Check that the metadata for the existing dataset matches the new tiles
                all_metadatas.append(
                    {
                        "crs": existing_dataset.get_meta_item("crs.wkt"),
                        "format": existing_dataset.get_meta_item("format.json"),
                        "schema": existing_dataset.get_meta_item("schema.json"),
                    }
                )
            if delete:
                if existing_dataset is None:
                    # Trying to delete specific paths from a nonexistent dataset?
                    # This suggests the caller is confused.
                    raise InvalidOperation(
                        f"Dataset {ds_path} does not exist. Cannot delete paths from it."
                    )
                root_tree = repo.head_tree
                for tile_name in delete:
                    # Check that the blob exists; if not, error out
                    blob_path = existing_dataset.tilename_to_blob_path(tile_name)
                    try:
                        root_tree / blob_path
                    except KeyError:
                        raise NotFound(f"{tile_name} does not exist, can't delete it")

                    proc.stdin.write(f"D {blob_path}\n".encode("utf8"))
        else:
            # Delete the entire existing dataset, before we re-import it.
            proc.stdin.write(f"D {ds_path}\n".encode("utf8"))

        for i, blob_path in write_blobs_to_stream(proc.stdin, extra_blobs):
            pass

        for source in sources:
            click.echo(f"Importing {source}...")
            source_metadata = source_to_metadata[source]
            tilename = PointCloudV1.tilename_from_path(source)
            rel_blob_path = PointCloudV1.tilename_to_blob_path(tilename, relative=True)
            blob_path = f"{ds_inner_path}/{rel_blob_path}"

            # Check if tile has already been imported previously:
            if existing_dataset is not None:
                existing_summary = existing_dataset.get_tile_summary(
                    tilename, missing_ok=True
                )
                if existing_summary:
                    source_oid = "sha256:" + source_to_hash_and_size[source][0]
                    if (
                        existing_summary["oid"] == source_oid
                        or existing_summary.get("sourceOid") == source_oid
                    ):
                        # This tile has already been imported before. Reuse it rather than re-importing it.
                        # (Especially don't use PDAL to reconvert it - that creates pointless diffs due to recompression).
                        write_blob_to_stream(
                            proc.stdin,
                            blob_path,
                            (existing_dataset.inner_tree / rel_blob_path).data,
                        )
                        del source_to_metadata[source]
                        continue

            tile_is_copc = source_metadata["format"]["optimization"] == "copc"
            conversion_func = None

            if convert_to_copc and not tile_is_copc:
                conversion_func = convert_tile_to_copc_and_reextract_metadata

            pointer_dict = copy_file_to_local_lfs_cache(
                repo,
                source,
                conversion_func,
                oid_and_size=source_to_hash_and_size[source],
            )
            pointer_dict = format_tile_for_pointer_file(
                source_to_metadata[source]["tile"], pointer_dict
            )

            write_blob_to_stream(
                proc.stdin, blob_path, dict_to_pointer_file_bytes(pointer_dict)
            )

        rewrite_metadata = (
            None if convert_to_copc else RewriteMetadata.DROP_OPTIMIZATION
        )
        all_metadatas.extend(source_to_metadata.values())
        merged_metadata = rewrite_and_merge_metadata(all_metadatas, rewrite_metadata)
        check_for_non_homogenous_metadata(merged_metadata)

        write_blob_to_stream(
            proc.stdin,
            f"{ds_inner_path}/meta/format.json",
            json_pack(merged_metadata["format"]),
        )
        write_blob_to_stream(
            proc.stdin,
            f"{ds_inner_path}/meta/schema.json",
            json_pack(merged_metadata["schema"]),
        )
        write_blob_to_stream(
            proc.stdin,
            f"{ds_inner_path}/meta/crs.wkt",
            ensure_bytes(normalise_wkt(merged_metadata["crs"])),
        )

    try:
        if amend:
            # Squash the commit we just created into its parent, replacing both commits on the head branch.
            new_tree = repo.references[fast_import_on_branch].peel(pygit2.Tree)
            new_commit_oid = repo.create_commit(
                # Don't move a branch tip. pygit2 doesn't allow us to use head_branch here
                # (because we're not using its tip as the first parent)
                # so we just create a detached commit and then move the branch tip afterwards.
                None,
                repo.head_commit.author,
                repo.committer_signature(),
                message,
                new_tree.oid,
                repo.head_commit.parent_ids,
            )
        else:
            # Just reset the head branch tip to the new commit we created on the temp branch
            new_commit = repo.references[fast_import_on_branch].peel(pygit2.Commit)
            new_commit_oid = new_commit.oid
            if (not allow_empty) and repo.head_tree:
                if new_commit.peel(pygit2.Tree).oid == repo.head_tree.oid:
                    raise NotFound("No changes to commit", exit_code=NO_CHANGES)
        if repo.head_branch not in repo.references:
            # unborn head
            repo.references.create(repo.head_branch, new_commit_oid)
        else:
            repo.references[repo.head_branch].set_target(new_commit_oid)
    finally:
        # Clean up the temp branch
        repo.references[fast_import_on_branch].delete()

    parts_to_create = [PartType.WORKDIR] if do_checkout else []
    # During imports we can keep old changes since they won't conflict with newly imported datasets.
    repo.working_copy.reset_to_head(
        repo_key_filter=RepoKeyFilter.datasets([ds_path]),
        create_parts_if_missing=parts_to_create,
    )


def infer_ds_path(sources):
    """Given a list of sources to import, choose a reasonable name for the dataset."""
    if len(sources) == 1:
        return remove_tile_extension(Path(sources[0]).name)
    names = set()
    parent_names = set()
    for source in sources:
        path = Path(source)
        names.add(path.name)
        parent_names.add(path.parents[0].name if path.parents else "*")
    result = _common_prefix(names)
    if result is None:
        result = _common_prefix(parent_names)
    return result


def _common_prefix(collection, min_length=4):
    prefix = os.path.commonprefix(list(collection))
    prefix = prefix.split("*", maxsplit=1)[0]
    prefix = prefix.rstrip("_-.,/\\")
    if len(prefix) < min_length:
        return None
    return prefix
