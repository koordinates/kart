import os
import warnings
from pathlib import Path

import click


from .cli_util import StringFromFile, RemovalInKart013Warning, KartCommand
from .core import check_git_user
from .dataset_util import validate_dataset_paths
from .exceptions import InvalidOperation
from .fast_import import FastImportSettings, fast_import_tables
from .repo import KartRepo, PotentialRepo
from .spatial_filter import SpatialFilterString, spatial_filter_help_text
from .tabular.import_source import TableImportSource
from .working_copy import PartType


@click.command(cls=KartCommand)
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
    help="Deprecated (no longer used)",
    default=None,
    hidden=True,
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
    if num_processes is not None:
        warnings.warn(
            "--num-processes is deprecated and will be removed in Kart 0.13.",
            RemovalInKart013Warning,
        )

    if directory is None:
        directory = os.curdir
    repo_path = Path(directory).resolve()

    if repo_path.exists() and any(repo_path.iterdir()):
        raise InvalidOperation(f'"{repo_path}" isn\'t empty', param_hint="directory")

    from kart.tabular.working_copy.base import TableWorkingCopy

    TableWorkingCopy.check_valid_creation_location(
        wc_location, PotentialRepo(repo_path)
    )

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
            settings=FastImportSettings(max_delta_depth=max_delta_depth),
            from_commit=None,
            message=message,
        )
        if do_checkout:
            repo.working_copy.reset_to_head(create_parts_if_missing=[PartType.TABULAR])

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
