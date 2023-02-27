import logging

import click

from kart.cli_util import StringFromFile, MutexOption, KartCommand
from kart.completion_shared import file_path_completer
from kart.parse_args import parse_import_sources_and_datasets
from kart.raster.metadata_util import rewrite_and_merge_metadata
from kart.raster.v1 import RasterV1
from kart.tile.importer import TileImporter

L = logging.getLogger(__name__)


@click.command("raster-import", hidden=True, cls=KartCommand)
@click.option(
    "--convert-to-cog/--no-convert-to-cog",
    " /--preserve-format",
    is_flag=True,
    default=False,
    help="Whether to convert all GeoTIFFs to COGs (Cloud Optimized GeoTIFFs), or to import all files in their native format.",
)
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
@click.option("--dataset-path", "--dataset", help="The dataset's path once imported")
@click.argument(
    "args",
    nargs=-1,
    metavar="SOURCE [SOURCES...]",
    shell_complete=file_path_completer,
)
def raster_import(
    ctx,
    convert_to_cog,
    dataset_path,
    message,
    do_checkout,
    replace_existing,
    update_existing,
    delete,
    amend,
    allow_empty,
    args,
):
    """
    Experimental command for importing a dataset of raster tiles.

    SOURCES should be one or more GeoTIFF files (or wildcards that match multiple GeoTIFF files).
    """
    repo = ctx.obj.repo

    if convert_to_cog:
        raise NotImplementedError("Sorry, --convert-to-cog is not yet implemented")

    sources, datasets = parse_import_sources_and_datasets(args)
    if datasets:
        problem = "    \n".join(datasets)
        raise click.UsageError(
            f"For raster import, every argument should be a GeoTIFF file:\n    {problem}"
        )

    RasterImporter(repo, ctx, convert_to_cog).import_tiles(
        dataset_path=dataset_path,
        message=message,
        do_checkout=do_checkout,
        replace_existing=replace_existing,
        update_existing=update_existing,
        delete=delete,
        amend=amend,
        allow_empty=allow_empty,
        sources=sources,
    )


class RasterImporter(TileImporter):

    DATASET_CLASS = RasterV1

    def __init__(self, repo, ctx, convert_to_copc):
        super().__init__(repo, ctx)
        self.convert_to_copc = convert_to_copc

    def get_default_message(self):
        return f"Importing {len(self.sources)} GeoTIFF tiles as {self.dataset_path}"

    def check_metadata_pre_convert(self):
        pass

    def check_metadata_post_convert(self):
        pass

    # These are all pretty simple since we don't do any conversions yet:

    def get_merged_source_metadata(self, all_metadata):
        return rewrite_and_merge_metadata(all_metadata)

    def get_predicted_merged_metadata(self, all_metadata):
        return rewrite_and_merge_metadata(all_metadata)

    def get_actual_merged_metadata(self, all_metadata):
        return rewrite_and_merge_metadata(all_metadata)

    def get_conversion_func(self, source_metadata):
        return None

    def existing_tile_matches_source(self, source_oid, existing_summary):
        """Check if the existing tile can be reused instead of reimporting."""
        if not source_oid.startswith("sha256:"):
            source_oid = "sha256:" + source_oid

        return existing_summary.get("oid") == source_oid
