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
    "--num-workers",
    "--num-processes",
    type=click.INT,
    help="How many import workers to run in parallel. Defaults to the number of available CPU cores.",
    default=None,
    hidden=True,
)
@click.option("--dataset-path", "--dataset", help="The dataset's path once imported")
@click.argument(
    "sources",
    nargs=-1,
    metavar="SOURCE [SOURCES...]",
)
def byod_point_cloud_import(
    ctx,
    message,
    do_checkout,
    replace_existing,
    update_existing,
    delete,
    amend,
    allow_empty,
    num_workers,
    dataset_path,
    sources,
):
    """
    Experimental. Import a dataset of point-cloud tiles from S3. Doesn't fetch the tiles, does store the tiles original location.

    SOURCES should be one or more LAZ or LAS files (or wildcards that match multiple LAZ or LAS files).
    """
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
        # TODO - download only certain ranges of the file, and extract metadata from those.
        tmp_downloaded_tile = fetch_from_s3(tile_location)
        result = extract_pc_tile_metadata(
            tmp_downloaded_tile, oid_and_size=oid_and_size
        )
        tmp_downloaded_tile.unlink()
        # TODO - format still not definite, we might not put the whole URL in here.
        result["tile"]["url"] = tile_location
        return result
