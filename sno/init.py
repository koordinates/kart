import os
from pathlib import Path


import click
import pygit2
from osgeo import gdal

from sno import is_windows
from . import checkout
from .core import check_git_user
from .cli_util import call_and_exit_flag, MutexOption, StringFromFile, JsonFromFile
from .exceptions import InvalidOperation
from .import_source import ImportSource
from .ogr_import_source import OgrImportSource, FORMAT_TO_OGR_MAP
from .fast_import import fast_import_tables, ReplaceExisting
from .structure import RepositoryStructure
from .sno_repo import SnoRepo
from .repository_version import (
    REPO_VERSIONS_CHOICE,
    REPO_VERSIONS_DEFAULT_CHOICE,
)
from .working_copy import WorkingCopy


def list_import_formats(ctx, param, value):
    """
    List the supported import formats
    """
    names = set()
    for prefix, ogr_driver_name in FORMAT_TO_OGR_MAP.items():
        d = gdal.GetDriverByName(ogr_driver_name)
        if d:
            m = d.GetMetadata()
            # only vector formats which can read things.
            if m.get("DCAP_VECTOR") == "YES" and m.get("DCAP_OPEN") == "YES":
                names.add(prefix)
    for n in sorted(names):
        click.echo(n)


def _add_datasets_to_working_copy(repo, *datasets, replace_existing=False):
    wc = WorkingCopy.get(repo, allow_uncreated=True)
    if not wc:
        return

    commit = repo.head.peel(pygit2.Commit)
    if not wc.is_created():
        click.echo(f"Creating working copy at {wc.path} ...")
        wc.create_and_initialise()
    else:
        click.echo(f"Updating {wc.path} ...")

    if replace_existing:
        wc.drop_table(commit, *datasets)
    wc.write_full(commit, *datasets)


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
def import_table(
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
    allow_empty,
    max_delta_depth,
    do_checkout,
):
    """
    Import data into a repository.

    $ sno import SOURCE [TABLE_SPEC] [TABLE_SPEC]

    SOURCE: Import from dataset: "FORMAT:PATH" eg. "GPKG:my.gpkg"

    TABLE_SPEC: Import a particular table, optionally with a new name: "TABLE[:AS_NAME]"
    eg. "2019_08_06_median_waterlevel:waterlevel"

    $ sno import GPKG:my.gpkg [table1[:new_name1] [table2[:new_name2]]

    To show available tables in the import data, use

    $ sno import --list GPKG:my.gpkg
    """

    if output_format == "json" and not do_list:
        raise click.UsageError(
            "Illegal usage: '--output-format=json' only supports --list"
        )
    if do_list:
        OgrImportSource.open(source, None).print_table_list(
            do_json=output_format == "json"
        )
        return

    repo = ctx.obj.repo
    check_git_user(repo)

    if (
        not do_checkout
        and WorkingCopy.get(
            repo,
        )
        is not None
    ):
        click.echo(
            "Warning: '--no-checkout' has no effect as a working copy already exists",
            err=True,
        )
        do_checkout = True

    base_import_source = OgrImportSource.open(source, None)
    if all_tables:
        tables = base_import_source.get_tables().keys()
    elif not tables:
        tables = [base_import_source.prompt_for_table("Select a table to import")]

    import_sources = []
    for table in tables:
        (src_table, *rest) = table.split(":", 1)
        dest_path = rest[0] if rest else src_table
        if not dest_path:
            raise click.BadParameter("Invalid table name", param_hint="tables")
        if is_windows:
            dest_path = dest_path.replace("\\", "/")  # git paths use / as a delimiter

        info = table_info.get(dest_path, {})
        import_source = base_import_source.clone_for_table(
            src_table,
            primary_key=primary_key,
            title=info.get("title"),
            description=info.get("description"),
            xml_metadata=info.get("xmlMetadata"),
        )
        if replace_existing:
            rs = RepositoryStructure(repo)
            if rs.version < 2:
                raise InvalidOperation(
                    f"--replace-existing is not supported for V{rs.version} datasets"
                )
            try:
                existing_ds = rs[dest_path]
            except KeyError:
                # no such existing dataset. no problems
                pass
            else:
                # Align the column IDs to the existing schema.
                # This is important, otherwise importing the same data twice
                # will result in a new schema object, and thus a new blob for every feature.
                import_source.schema = existing_ds.schema.align_to_self(
                    import_source.schema
                )
        import_source.dest_path = dest_path
        import_sources.append(import_source)

    ImportSource.check_valid(import_sources, param_hint="tables")

    fast_import_tables(
        repo,
        import_sources,
        message=message,
        max_delta_depth=max_delta_depth,
        replace_existing=ReplaceExisting.GIVEN
        if replace_existing
        else ReplaceExisting.DONT_REPLACE,
        allow_empty=allow_empty,
    )

    rs = RepositoryStructure(repo)
    if do_checkout:
        _add_datasets_to_working_copy(
            repo,
            *[rs[s.dest_path] for s in import_sources],
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
    "--repo-version",
    type=REPO_VERSIONS_CHOICE,
    default=REPO_VERSIONS_DEFAULT_CHOICE,
    hidden=True,
)
@click.option(
    "--bare",
    is_flag=True,
    default=False,
    help='Whether the new repository should be "bare" and have no working copy',
)
@click.option(
    "--workingcopy-path",
    "wc_path",
    help="Path where the working copy should be created. "
    "This should be a GPKG file eg example.gpkg or a postgres URI including schema eg postgresql://[HOST]/DBNAME/SCHEMA",
)
@click.option(
    "--max-delta-depth",
    hidden=True,
    default=0,
    type=click.INT,
    help="--depth option to git-fast-import (advanced users only)",
)
def init(
    ctx,
    message,
    directory,
    repo_version,
    import_from,
    do_checkout,
    bare,
    wc_path,
    max_delta_depth,
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
    WorkingCopy.check_valid_creation_path(wc_path, repo_path)

    if not repo_path.exists():
        repo_path.mkdir(parents=True)

    if import_from:
        check_git_user(repo=None)
        base_source = OgrImportSource.open(import_from, None)

        # Import all tables.
        # If you need finer grained control than this,
        # use `sno init` and *then* `sno import` as a separate command.
        tables = base_source.get_tables().keys()
        sources = [base_source.clone_for_table(t) for t in tables]

    # Create the repository
    repo = SnoRepo.init_repository(repo_path, repo_version, wc_path, bare)

    if import_from:
        fast_import_tables(
            repo,
            sources,
            message=message,
            max_delta_depth=max_delta_depth,
        )
        head_commit = repo.head.peel(pygit2.Commit)
        if do_checkout and not bare:
            checkout.reset_wc_if_needed(repo, head_commit)

    else:
        click.echo(
            f"Created an empty repository at {repo_path} — import some data with `sno import`"
        )
