import os
from pathlib import Path

import click
from osgeo import gdal

from kart import is_windows

from . import checkout
from .cli_util import JsonFromFile, MutexOption, StringFromFile, call_and_exit_flag
from .core import check_git_user
from .dataset_util import validate_dataset_paths
from .exceptions import InvalidOperation
from .fast_import import FastImportSettings, ReplaceExisting, fast_import_tables
from .repo import KartRepo, PotentialRepo
from .spatial_filter import SpatialFilterString, spatial_filter_help_text
from .tabular.import_source import TableImportSource
from .tabular.ogr_import_source import FORMAT_TO_OGR_MAP
from .tabular.pk_generation import PkGeneratingTableImportSource
from .working_copy import WorkingCopyStatus


def list_import_formats(ctx):
    """
    List the supported import formats
    """
    click.echo("Geopackage: PATH.gpkg")
    click.echo("PostgreSQL: postgresql://HOST/DBNAME[/DBSCHEMA]")
    click.echo("SQL Server: mssql://HOST/DBNAME[/DBSCHEMA]")
    click.echo("MySQL: mysql://HOST[/DBNAME]")

    ogr_types = set()
    for prefix, ogr_driver_name in FORMAT_TO_OGR_MAP.items():
        d = gdal.GetDriverByName(ogr_driver_name)
        if d:
            m = d.GetMetadata()
            # only vector formats which can read things.
            if m.get("DCAP_VECTOR") == "YES" and m.get("DCAP_OPEN") == "YES":
                ogr_types.add(prefix)

    if "SHP" in ogr_types:
        click.echo("Shapefile: PATH.shp")


def _add_datasets_to_working_copy(repo, *datasets, replace_existing=False):
    wc = repo.get_working_copy(allow_uncreated=True)
    if not wc:
        return

    commit = repo.head_commit
    if not (wc.status() & WorkingCopyStatus.INITIALISED):
        click.echo(f"Creating working copy at {wc} ...")
        wc.create_and_initialise()
    else:
        click.echo(f"Updating {wc} ...")

    if replace_existing:
        wc.rewrite_full(commit, *datasets, force=True)
    else:
        wc.write_full(commit, *datasets)


class GenerateIDsFromFile(StringFromFile):
    name = "ids"

    def convert(self, value, param, ctx):
        fp = super().convert(
            value,
            param,
            ctx,
            # Get the file object, so we don't have to read the whole thing
            as_file=True,
        )
        return (line.rstrip("\n") for line in fp)


def any_at_all(iterable):
    # Returns True if anything exists in iterable - even falsey values like None or 0.
    # Advances the iterable by one if non-empty, so is best used like so:
    # >> if any_at_all(iterable): raise Error("Iterable should be empty.")
    return any(True for _ in iterable)


@click.command("import")
@click.pass_context
@click.argument("source")
@click.argument(
    "tables",
    nargs=-1,
)
@click.option(
    "--all-tables",
    "-a",
    help="Import all tables from the source.",
    is_flag=True,
    cls=MutexOption,
    exclusive_with=["do_list", "tables"],
)
@click.option(
    "--message",
    "-m",
    type=StringFromFile(encoding="utf-8"),
    help="Commit message. By default this is auto-generated.",
)
@click.option(
    "--table-info",
    type=JsonFromFile(
        encoding="utf-8",
        schema={
            "type": "object",
            "$schema": "http://json-schema.org/draft-07/schema",
            "patternProperties": {
                ".*": {
                    "type": "object",
                    "properties": {
                        "title": {"type": "string"},
                        "description": {"type": "string"},
                        "xmlMetadata": {"type": "string"},
                    },
                }
            },
        },
    ),
    default="{}",
    help=(
        "Specifies overrides for imported tables, in a nested JSON object. \n"
        "Each key is a dataset name, and each value is an object. \n"
        "Valid overrides are 'title', 'description' and 'xmlMetadata'.\n"
        """e.g.:  --table-info='{"land_parcels": {"title": "Land Parcels 1:50k"}'\n"""
        "To import from a file, prefix with `@`, e.g. `--table-info=@filename.json`"
    ),
)
@click.option(
    "--list",
    "do_list",
    is_flag=True,
    help="List all tables present in the source path",
    cls=MutexOption,
    exclusive_with=["all_tables", "tables"],
)
@call_and_exit_flag(
    "--list-formats",
    callback=list_import_formats,
    help="List available import formats, and then exit",
)
@click.option(
    "--output-format",
    "-o",
    type=click.Choice(["text", "json"]),
    default="text",
)
@click.option(
    "--primary-key",
    help="Which field to use as the primary key. Must be unique. Auto-detected when possible.",
)
@click.option(
    "--replace-existing",
    is_flag=True,
    help="Replace existing dataset(s) of the same name.",
)
@click.option(
    "--replace-ids",
    type=GenerateIDsFromFile(encoding="utf-8"),
    help=(
        "Replace only features with the given IDs. IDs should be given one-per-line. "
        "Use file arguments (--replace-ids=@filename.txt). Implies --replace-existing. "
        "Requires the dataset to have a primary key, unless the value given is an empty "
        "string (replaces no features)"
    ),
)
@click.option(
    "--similarity-detection-limit",
    hidden=True,
    type=click.INT,
    default=10000,
    help=(
        "When replacing an existing dataset where primary keys are auto-generated: the maximum number of unmatched "
        "features to search through for similar features, so that primary keys can be reassigned for features that "
        "are similar but have had minor edits. Zero means that no similarity detection is performed. (Advanced users only)"
    ),
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
    "--max-delta-depth",
    hidden=True,
    default=0,
    type=click.INT,
    help="--depth option to git-fast-import (advanced users only)",
)
@click.option(
    "--checkout/--no-checkout",
    "do_checkout",
    is_flag=True,
    default=True,
    help="Whether to create a working copy once the import is finished, if no working copy exists yet.",
)
@click.option(
    "--num-processes",
    type=click.INT,
    help="How many git-fast-import processes to use. Defaults to the number of available CPU cores.",
)
def import_(
    ctx,
    all_tables,
    message,
    do_list,
    output_format,
    primary_key,
    source,
    tables,
    table_info,
    replace_existing,
    replace_ids,
    similarity_detection_limit,
    allow_empty,
    max_delta_depth,
    do_checkout,
    num_processes,
):
    """
    Import data into a repository.

    $ kart import SOURCE [TABLE_SPEC] [TABLE_SPEC]

    SOURCE: Import from dataset: "FORMAT:PATH" eg. "GPKG:my.gpkg"

    TABLE_SPEC: Import a particular table, optionally with a new name: "TABLE[:AS_NAME]"
    eg. "2019_08_06_median_waterlevel:waterlevel"

    $ kart import GPKG:my.gpkg [table1[:new_name1]] [table2[:new_name2]]

    To show available tables in the import data, use

    $ kart import --list GPKG:my.gpkg
    """

    if output_format == "json" and not do_list:
        raise click.UsageError(
            "Illegal usage: '--output-format=json' only supports --list"
        )
    if do_list:
        TableImportSource.open(source).print_table_list(do_json=output_format == "json")
        return

    repo = ctx.obj.repo
    check_git_user(repo)

    if not do_checkout and repo.working_copy is not None:
        click.echo(
            "Warning: '--no-checkout' has no effect as a working copy already exists",
            err=True,
        )
        do_checkout = True

    base_import_source = TableImportSource.open(source)
    if all_tables:
        tables = base_import_source.get_tables().keys()
    elif not tables:
        tables = [base_import_source.prompt_for_table("Select a table to import")]

    import_sources = []
    for table in tables:
        if ":" in table:
            table, dest_path = table.split(":", 1)
            if not dest_path:
                raise click.BadParameter("Invalid table name", param_hint="tables")
        else:
            dest_path = table

        meta_overrides = table_info.get(dest_path, {})
        if is_windows:
            dest_path = dest_path.replace("\\", "/")  # git paths use / as a delimiter

        if "xmlMetadata" in meta_overrides:
            meta_overrides["metadata.xml"] = meta_overrides.pop("xmlMetadata")
        import_source = base_import_source.clone_for_table(
            table,
            dest_path=dest_path,
            primary_key=primary_key,
            meta_overrides=meta_overrides,
        )

        if replace_ids is not None:
            if repo.table_dataset_version < 2:
                raise InvalidOperation(
                    f"--replace-ids is not supported for V{repo.table_dataset_version} datasets"
                )
            if not import_source.schema.pk_columns:
                # non-PK datasets can use this if it's only ever an empty list.
                if any_at_all(replace_ids):
                    raise InvalidOperation(
                        "--replace-ids requires an import source with a primary key"
                    )

            replace_existing = True

        if replace_existing:
            if repo.table_dataset_version < 2:
                raise InvalidOperation(
                    f"--replace-existing is not supported for V{repo.table_dataset_version} datasets"
                )
            try:
                existing_ds = repo.datasets()[dest_path]
            except KeyError:
                # no such existing dataset. no problems
                pass
            else:
                # Align the column IDs to the existing schema.
                # This is important, otherwise importing the same data twice
                # will result in a new schema object, and thus a new blob for every feature.
                # Note that alignment works better if we add the generated-pk-column first (when needed),
                # if one schema has this and the other lacks it they will be harder to align.
                import_source = PkGeneratingTableImportSource.wrap_source_if_needed(
                    import_source,
                    repo,
                    dest_path=dest_path,
                    similarity_detection_limit=similarity_detection_limit,
                )
                import_source.align_schema_to_existing_schema(existing_ds.schema)
                if (
                    import_source.schema.legend.pk_columns
                    != existing_ds.schema.legend.pk_columns
                    and replace_ids is not None
                ):
                    raise InvalidOperation(
                        "--replace-ids is not supported when the primary key column is being changed"
                    )
        import_sources.append(import_source)

    TableImportSource.check_valid(import_sources, param_hint="tables")

    all_ds_paths = [s.dest_path for s in import_sources]
    if not replace_existing:
        all_ds_paths[:0] = [ds.path for ds in repo.datasets()]
    validate_dataset_paths(all_ds_paths)

    replace_existing_enum = (
        ReplaceExisting.GIVEN if replace_existing else ReplaceExisting.DONT_REPLACE
    )
    fast_import_tables(
        repo,
        import_sources,
        settings=FastImportSettings(
            num_processes=num_processes, max_delta_depth=max_delta_depth
        ),
        verbosity=ctx.obj.verbosity + 1,
        message=message,
        replace_existing=replace_existing_enum,
        from_commit=repo.head_commit,
        replace_ids=replace_ids,
        allow_empty=allow_empty,
    )

    if do_checkout:
        _add_datasets_to_working_copy(
            repo,
            *[repo.datasets()[s.dest_path] for s in import_sources],
            replace_existing=replace_existing,
        )


@click.command()
@click.pass_context
@click.argument(
    "directory", type=click.Path(writable=True, file_okay=False), required=False
)
@click.option(
    "--import",
    "import_from",
    help='Import a database (all tables): "FORMAT:PATH" eg. "GPKG:my.gpkg"',
)
@click.option(
    "--checkout/--no-checkout",
    "do_checkout",
    is_flag=True,
    default=True,
    help="Whether to immediately create a working copy with the initial import. Has no effect if --import is not set.",
)
@click.option(
    "--message",
    "-m",
    type=StringFromFile(encoding="utf-8"),
    help="Commit message (when used with --import). By default this is auto-generated.",
)
@click.option(
    "--bare",
    is_flag=True,
    default=False,
    help='Whether the new repository should be "bare" and have no working copy',
)
@click.option(
    "-b",
    "--initial-branch",
    default=None,  # Not specified? We use git's `init.defaultBranch` config
    help=(
        "Use the specified name for the initial branch "
        "in the newly created repository."
    ),
)
@click.option(
    "--workingcopy-location",
    "--workingcopy-path",
    "--workingcopy",
    "wc_location",
    help="Location where the working copy should be created. This should be in one of the following formats:\n"
    "- PATH.gpkg\n"
    "- postgresql://HOST/DBNAME/DBSCHEMA\n"
    "- mssql://HOST/DBNAME/DBSCHEMA\n"
    "- mysql://HOST/DBNAME\n",
)
@click.option(
    "--max-delta-depth",
    hidden=True,
    type=click.INT,
    help="--depth option to git-fast-import (advanced users only)",
)
@click.option(
    "--num-processes",
    type=click.INT,
    help="How many git-fast-import processes to use. Defaults to the number of available CPU cores.",
)
@click.option(
    "--spatial-filter",
    "spatial_filter_spec",
    type=SpatialFilterString(encoding="utf-8", allow_reference=False),
    help=spatial_filter_help_text(allow_reference=False),
)
def init(
    ctx,
    message,
    directory,
    import_from,
    do_checkout,
    bare,
    initial_branch,
    wc_location,
    max_delta_depth,
    num_processes,
    spatial_filter_spec,
):
    """
    Initialise a new repository and optionally import data.
    DIRECTORY must be empty. Defaults to the current directory.
    """
    if directory is None:
        directory = os.curdir
    repo_path = Path(directory).resolve()

    if repo_path.exists() and any(repo_path.iterdir()):
        raise InvalidOperation(f'"{repo_path}" isn\'t empty', param_hint="directory")

    from kart.working_copy.base import BaseWorkingCopy

    BaseWorkingCopy.check_valid_creation_location(wc_location, PotentialRepo(repo_path))

    if not repo_path.exists():
        repo_path.mkdir(parents=True)

    if import_from:
        check_git_user(repo=None)
        base_source = TableImportSource.open(import_from)

        # Import all tables.
        # If you need finer grained control than this,
        # use `kart init` and *then* `kart import` as a separate command.
        tables = base_source.get_tables().keys()
        sources = [base_source.clone_for_table(t) for t in tables]

    # Create the repository
    repo = KartRepo.init_repository(
        repo_path,
        wc_location,
        bare,
        initial_branch=initial_branch,
        spatial_filter_spec=spatial_filter_spec,
    )

    if import_from:
        validate_dataset_paths([s.dest_path for s in sources])
        fast_import_tables(
            repo,
            sources,
            settings=FastImportSettings(
                num_processes=num_processes, max_delta_depth=max_delta_depth
            ),
            from_commit=None,
            message=message,
        )
        head_commit = repo.head_commit
        if do_checkout and not bare:
            checkout.reset_wc_if_needed(repo, head_commit)

    else:
        click.echo(
            f"Created an empty repository at {repo_path} — import some data with `kart import`"
        )

    # Experimental point-cloud datasets:
    if os.environ.get("X_KART_POINT_CLOUDS"):
        from kart.lfs_util import install_lfs_hooks

        lfs_override = os.environ.get("X_KART_SET_LFS_FOR_NEW_REPOS")
        if lfs_override:
            repo.config["lfs.url"] = lfs_override

        install_lfs_hooks(repo)
