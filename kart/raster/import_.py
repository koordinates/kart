import logging

import click

from kart.cli_util import StringFromFile, MutexOption, KartCommand
from kart.completion_shared import file_path_completer
from kart.exceptions import InvalidOperation, INVALID_FILE_FORMAT
from kart.parse_args import parse_import_sources_and_datasets
from kart.raster.gdal_convert import convert_tile_to_cog
from kart.raster.metadata_util import (
    rewrite_and_merge_metadata,
    is_cog,
    RewriteMetadata,
)
from kart.raster.v1 import RasterV1
from kart.tile.importer import TileImporter
from kart.tile.tilename_util import find_similar_files_case_insensitive, PAM_SUFFIX

L = logging.getLogger(__name__)


@click.command("raster-import", hidden=True, cls=KartCommand)
@click.option(
    "--convert-to-cog/--no-convert-to-cog",
    " /--preserve-format",
    is_flag=True,
    default=True,
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
    "args",
    nargs=-1,
    metavar="SOURCE [SOURCES...]",
    shell_complete=file_path_completer,
)
def raster_import(
    ctx,
    convert_to_cog,
    message,
    do_checkout,
    replace_existing,
    update_existing,
    delete,
    amend,
    allow_empty,
    num_workers,
    dataset_path,
    args,
):
    """
    Experimental command for importing a dataset of raster tiles.

    SOURCES should be one or more GeoTIFF files (or wildcards that match multiple GeoTIFF files).
    """
    repo = ctx.obj.repo

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
        num_workers=num_workers,
    )


class RasterImporter(TileImporter):

    DATASET_CLASS = RasterV1

    def __init__(self, repo, ctx, convert_to_cog):
        super().__init__(repo, ctx)
        self.convert_to_cog = convert_to_cog

    def get_default_message(self):
        return f"Importing {len(self.sources)} GeoTIFF tiles as {self.dataset_path}"

    def check_metadata_pre_convert(self):
        pass

    def check_metadata_post_convert(self):
        pass

    def get_merged_source_metadata(self, all_metadata):
        return rewrite_and_merge_metadata(all_metadata, RewriteMetadata.DROP_PROFILE)

    def get_predicted_merged_metadata(self, all_metadata):
        rewrite_metadata = (
            RewriteMetadata.AS_IF_CONVERTED_TO_COG
            if self.convert_to_cog
            else RewriteMetadata.DROP_PROFILE
        )
        return rewrite_and_merge_metadata(all_metadata, rewrite_metadata)

    def get_actual_merged_metadata(self, all_metadata):
        rewrite_metadata = None if self.convert_to_cog else RewriteMetadata.DROP_PROFILE
        return rewrite_and_merge_metadata(all_metadata, rewrite_metadata)

    def get_conversion_func(self, source_metadata):
        if self.convert_to_cog and not is_cog(source_metadata):
            return convert_tile_to_cog
        return None

    def existing_tile_matches_source(self, source_oid, existing_summary):
        """Check if the existing tile can be reused instead of reimporting."""
        if not source_oid.startswith("sha256:"):
            source_oid = "sha256:" + source_oid

        if existing_summary.get("oid") == source_oid:
            # The import source we were given has already been imported in its native format.
            # Return True if that's what we would do anyway.
            if self.convert_to_cog:
                return is_cog(existing_summary)
            else:
                return True

        # NOTE: this logic would be more complicated if we supported more than one type of conversion.
        if existing_summary.get("sourceOid") == source_oid:
            # The import source we were given has already been imported, but converted to COPC.
            # Return True if we were going to convert it to COPC too.
            return self.convert_to_cog and is_cog(existing_summary)

        return False

    def sidecar_files(self, source):
        source = str(source)
        if self.DATASET_CLASS.remove_tile_extension(source) == source:
            return

        pam_path = source + PAM_SUFFIX
        pams = find_similar_files_case_insensitive(pam_path)
        if len(pams) == 1:
            yield pams[0], PAM_SUFFIX
        if len(pams) > 1:
            detail = "\n".join(str(p) for p in pams)
            raise InvalidOperation(
                f"More than one PAM file found for {source}:\n{detail}",
                exit_code=INVALID_FILE_FORMAT,
            )
