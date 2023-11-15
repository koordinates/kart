import logging

import click

from kart.byod.importer import ByodTileImporter
from kart.cli_util import StringFromFile, MutexOption, KartCommand
from kart.point_cloud.import_ import PointCloudImporter
from kart.point_cloud.metadata_util import extract_pc_tile_metadata
from kart.s3_util import get_hash_and_size_of_s3_object, fetch_from_s3


L = logging.getLogger(__name__)


@click.command("byod-point-cloud-import", hidden=True, cls=KartCommand)
@click.pass_context
@click.option(
    "--convert-to-copc/--no-convert-to-copc",
    "--cloud-optimized/--no-cloud-optimized",
    "--cloud-optimised/--no-cloud-optimised",
    " /--preserve-format",
    is_flag=True,
    default=None,
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
    help=(
        "Whether to check out the dataset once the import is finished. If false, the dataset will be configured as "
        "not being checked out and will never be written to the working copy, until this decision is reversed by "
        "running `kart checkout --dataset=DATASET-PATH`."
    ),
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
    "--num-workers",
    "--num-processes",
    type=click.INT,
    help="How many import workers to run in parallel. Defaults to the number of available CPU cores.",
    default=None,
    hidden=True,
)
@click.option("--dataset-path", "--dataset", help="The dataset's path once imported")
@click.option(
    "--link",
    "do_link",
    is_flag=True,
    default=True,
    hidden=True,
    help=(
        "Link the created dataset to the original source location, so that the original source location is treated as "
        "the authoritative source for the given data and data is fetched from there if needed. Only supported for "
        "tile-based datasets."
    ),
)
@click.argument(
    "sources",
    nargs=-1,
    metavar="SOURCE [SOURCES...]",
)
def byod_point_cloud_import(
    ctx,
    convert_to_copc,
    message,
    do_checkout,
    replace_existing,
    update_existing,
    delete,
    amend,
    allow_empty,
    num_workers,
    dataset_path,
    do_link,
    sources,
):
    """
    Import a dataset of point-cloud tiles from S3. Doesn't fetch the tiles, does store the tiles original location.

    SOURCES should be one or more LAZ or LAS files (or wildcards that match multiple LAZ or LAS files).
    """
    if not do_link:
        # This is here for technical reasons - all the options are forwarded from one command to another, including --link.
        # In practise we don't expect the user to set --link at all if they are also manually calling this (hidden) command.
        raise click.UsageError("Can't do a linked-import with --link=false")
    if convert_to_copc:
        raise click.UsageError(
            "Sorry, converting a linked dataset to COPC is not supported - "
            "the data must remain in its original location and its original format as the authoritative source."
        )

    repo = ctx.obj.repo

    ByodPointCloudImporter(
        repo=repo,
        ctx=ctx,
        convert_to_cloud_optimized=False,
        dataset_path=dataset_path,
        message=message,
        do_checkout=do_checkout,
        replace_existing=replace_existing,
        update_existing=update_existing,
        delete=delete,
        amend=amend,
        allow_empty=allow_empty,
        num_workers=num_workers,
        sources=list(sources),
    ).import_tiles()


class ByodPointCloudImporter(ByodTileImporter, PointCloudImporter):
    def extract_tile_metadata(self, tile_location):
        oid_and_size = get_hash_and_size_of_s3_object(tile_location)
        return None, extract_pc_tile_metadata(tile_location, oid_and_size=oid_and_size)
