import contextlib
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
from .ogr_import_source import OgrImporter, FORMAT_TO_OGR_MAP
from .fast_import import fast_import_tables
from .structure import RepositoryStructure
from .repository_version import (
    write_repo_version_config,
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
            if m.get('DCAP_VECTOR') == 'YES' and m.get('DCAP_OPEN') == 'YES':
                names.add(prefix)
    for n in sorted(names):
        click.echo(n)


def _add_datasets_to_working_copy(repo, *datasets):
    wc = WorkingCopy.get(repo, create_if_missing=True)
    if not wc:
        return

    commit = repo.head.peel(pygit2.Commit)
    if not wc.is_created():
        click.echo(f'Creating working copy at {wc.path} ...')
        wc.create()
    else:
        click.echo(f'Updating {wc.path} ...')

    for dataset in datasets:
        wc.write_full(commit, dataset)


@contextlib.contextmanager
def temporary_branch(repo):
    """
    Contextmanager.
    Creates a branch for HEAD to point at, then deletes it again so HEAD is detached again.
    """
    TEMP_BRANCH_NAME = '__temp-fast-import-branch'
    if TEMP_BRANCH_NAME in repo.branches:
        del repo.branches[TEMP_BRANCH_NAME]

    temp_branch = repo.branches.local.create(
        TEMP_BRANCH_NAME, repo.head.peel(pygit2.Commit)
    )
    repo.set_head(temp_branch.name)
    try:
        yield temp_branch
    finally:
        repo.set_head(repo.head.target)
        repo.branches.delete(temp_branch.branch_name)


@click.command("import")
@click.pass_context
@click.argument("source")
@click.argument(
    "tables", nargs=-1,
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
    type=StringFromFile(encoding='utf-8'),
    help="Commit message. By default this is auto-generated.",
)
@click.option(
    "--table-info",
    type=JsonFromFile(
        encoding='utf-8',
        schema={
            "type": "object",
            "$schema": 'http://json-schema.org/draft-07/schema',
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
    default='{}',
    help=(
        "Specifies overrides for imported tables, in a nested JSON object. \n"
        "Each key is a dataset name, and each value is an object. \n"
        "Valid overrides are 'title', 'description' and 'xmlMetadata'.\n"
        '''e.g.:  --table-info='{"land_parcels": {"title": "Land Parcels 1:50k"}'\n'''
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
    "--output-format", "-o", type=click.Choice(["text", "json"]), default="text",
)
@click.option(
    "--primary-key",
    help="Which field to use as the primary key. Must be unique. Auto-detected when possible.",
)
@click.option(
    "--max-delta-depth",
    hidden=True,
    default=0,
    type=click.INT,
    help="--depth option to git-fast-import (advanced users only)",
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
    max_delta_depth,
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

    if output_format == 'json' and not do_list:
        raise click.UsageError(
            "Illegal usage: '--output-format=json' only supports --list"
        )

    use_repo_ctx = not do_list
    if use_repo_ctx:
        repo = ctx.obj.repo
        check_git_user(repo)

    source_loader = OgrImporter.open(source, None)
    if do_list:
        source_loader.print_table_list(do_json=output_format == 'json')
        return
    elif all_tables:
        tables = source_loader.get_tables().keys()
    else:
        if not tables:
            tables = [source_loader.prompt_for_table("Select a table to import")]

    loaders = {}
    for table in tables:
        (src_table, *rest) = table.split(':', 1)
        dst_table = rest[0] if rest else src_table
        if not dst_table:
            raise click.BadParameter("Invalid table name", param_hint="tables")
        if is_windows:
            dst_table = dst_table.replace("\\", "/")  # git paths use / as a delimiter

        if dst_table in loaders:
            raise click.UsageError(
                f'table "{dst_table}" was specified more than once', param_hint="tables"
            )
        info = table_info.get(dst_table, {})
        loaders[dst_table] = source_loader.clone_for_table(
            src_table,
            primary_key=primary_key,
            title=info.get('title'),
            description=info.get('description'),
            xml_metadata=info.get('xmlMetadata'),
        )

    # Workaround the fact that fast import doesn't work when head is detached.
    ctx = temporary_branch(repo) if repo.head_is_detached else contextlib.nullcontext()

    with ctx:
        fast_import_tables(
            repo, loaders, message=message, max_delta_depth=max_delta_depth,
        )

    rs = RepositoryStructure(repo)
    _add_datasets_to_working_copy(repo, *[rs[dst_table] for dst_table in loaders])


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
    "--message",
    "-m",
    type=StringFromFile(encoding='utf-8'),
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
    "--no-checkout/--checkout",
    is_flag=True,
    default=False,
    help='Whether the new repository should be "bare" and have no working copy',
)
@click.option(
    "--workingcopy-path",
    "wc_path",
    type=click.Path(dir_okay=False),
    help="Path where the working copy should be created",
)
@click.option(
    "--max-delta-depth",
    hidden=True,
    default=0,
    type=click.INT,
    help="--depth option to git-fast-import (advanced users only)",
)
def init(
    ctx, message, directory, repo_version, import_from, bare, wc_path, max_delta_depth,
):
    """
    Initialise a new repository and optionally import data.
    DIRECTORY must be empty. Defaults to the current directory.
    """

    if directory is None:
        directory = os.curdir
    elif not Path(directory).exists():
        Path(directory).mkdir(parents=True)

    repo_path = Path(directory).resolve()
    if any(repo_path.iterdir()):
        raise InvalidOperation(f'"{repo_path}" isn\'t empty', param_hint="directory")

    if import_from:
        check_git_user(repo=None)
        source_loader = OgrImporter.open(import_from, None)

        # Import all tables.
        # If you need finer grained control than this,
        # use `sno init` and *then* `sno import` as a separate command.
        tables = source_loader.get_tables().keys()
        loaders = {t: source_loader.clone_for_table(t) for t in tables}

    # Create the repository
    repo = pygit2.init_repository(str(repo_path), bare=True)
    write_repo_version_config(repo, repo_version)
    WorkingCopy.write_config(repo, wc_path, bare)

    if import_from:
        fast_import_tables(
            repo, loaders, message=message, max_delta_depth=max_delta_depth,
        )
        head_commit = repo.head.peel(pygit2.Commit)
        checkout.reset_wc_if_needed(repo, head_commit)

    else:
        click.echo(
            f"Created an empty repository at {repo_path} — import some data with `sno import`"
        )
