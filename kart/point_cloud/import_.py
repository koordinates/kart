import logging
import os
from pathlib import Path

import click

from kart.crs_util import normalise_wkt
from kart.dataset_util import validate_dataset_paths
from kart.exceptions import (
    InvalidOperation,
    NotFound,
    NO_IMPORT_SOURCE,
    INVALID_FILE_FORMAT,
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
)
from kart.point_cloud.metadata_util import (
    RewriteMetadata,
    extract_pc_tile_metadata,
    rewrite_and_merge_metadata,
    check_for_non_homogenous_metadata,
    format_tile_for_pointer_file,
    remove_las_extension,
)
from kart.point_cloud.pdal_convert import convert_tile_to_copc
from kart.serialise_util import hexhash, json_pack, ensure_bytes
from kart.tabular.version import (
    SUPPORTED_VERSIONS,
    extra_blobs_for_version,
)
from kart.working_copy import PartType


L = logging.getLogger(__name__)


@click.command("point-cloud-import", hidden=True)
@click.pass_context
@click.option(
    "--convert-to-copc/--no-convert-to-copc",
    " /--preserve-format",
    is_flag=True,
    default=True,
    help="Whether to convert all non-COPC LAS or LAZ files to COPC LAZ files, or to import all files in their native format.",
)
@click.option(
    "--dataset-path", "ds_path", help="The dataset's path once imported", required=True
)
@click.option(
    "--checkout/--no-checkout",
    "do_checkout",
    is_flag=True,
    default=True,
    help="Whether to create a working copy once the import is finished, if no working copy exists yet.",
)
@click.argument("sources", metavar="SOURCES", nargs=-1, required=True)
def point_cloud_import(ctx, convert_to_copc, ds_path, do_checkout, sources):
    """
    Experimental command for importing point cloud datasets. Work-in-progress.
    Will eventually be merged with the main `import` command.

    SOURCES should be one or more LAZ or LAS files (or wildcards that match multiple LAZ or LAS files).
    """
    repo = ctx.obj.repo

    # TODO - improve path validation to make sure datasets of any type don't collide with each other
    # or with attachments.
    validate_dataset_paths([ds_path])

    for source in sources:
        if not (Path() / source).is_file():
            raise NotFound(f"No data found at {source}", exit_code=NO_IMPORT_SOURCE)

    source_to_metadata = {}

    for source in sources:
        click.echo(f"Checking {source}...          \r", nl=False)
        source_to_metadata[source] = extract_pc_tile_metadata(source)
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

    header = generate_header(
        repo,
        None,
        f"Importing {len(sources)} LAZ tiles as {ds_path}",
        repo.head_branch,
        repo.head_commit,
    )

    ds_inner_path = f"{ds_path}/.point-cloud-dataset.v1"

    def convert_tile_to_copc_and_reextract_metadata(source, dest):
        nonlocal source_to_metadata

        convert_tile_to_copc(source, dest)
        source_to_metadata[source] = extract_pc_tile_metadata(dest, extract_schema=True)

    with git_fast_import(repo, *FastImportSettings().as_args(), "--quiet") as proc:
        proc.stdin.write(header.encode("utf8"))

        for i, blob_path in write_blobs_to_stream(proc.stdin, extra_blobs):
            pass

        for source in sources:
            click.echo(f"Importing {source}...")
            source_metadata = source_to_metadata[source]

            tile_is_copc = source_metadata["format"]["optimization"] == "copc"
            conversion_func = None

            if convert_to_copc and not tile_is_copc:
                conversion_func = convert_tile_to_copc_and_reextract_metadata

            pointer_dict = copy_file_to_local_lfs_cache(repo, source, conversion_func)
            pointer_dict = format_tile_for_pointer_file(
                source_to_metadata[source]["tile"], pointer_dict
            )

            tilename = remove_las_extension(os.path.basename(source))
            tile_prefix = hexhash(tilename)[0:2]
            blob_path = f"{ds_inner_path}/tile/{tile_prefix}/{tilename}"
            write_blob_to_stream(
                proc.stdin, blob_path, dict_to_pointer_file_bytes(pointer_dict)
            )

        rewrite_metadata = (
            None if convert_to_copc else RewriteMetadata.DROP_OPTIMIZATION
        )
        merged_metadata = rewrite_and_merge_metadata(
            source_to_metadata.values(), rewrite_metadata
        )
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

    parts_to_create = [PartType.WORKDIR] if do_checkout else []
    # During imports we can keep old changes since they won't conflict with newly imported datasets.
    repo.working_copy.reset_to_head(
        repo_key_filter=RepoKeyFilter.datasets([ds_path]),
        create_parts_if_missing=parts_to_create,
    )
