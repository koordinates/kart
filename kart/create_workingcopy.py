import click

from kart.cli_util import KartCommand
from kart.exceptions import InvalidOperation, DbConnectionError
from kart.output_util import InputMode, get_input_mode
from kart.working_copy import PartType


_DISCARD_CHANGES_HELP_MESSAGE = (
    "Commit these changes first (`kart commit`) or"
    " just discard them by adding the option `--discard-changes`."
)


def create_tabular_workingcopy(repo, delete_existing, discard_changes, new_wc_loc):
    """Create or recreate the tabular working copy."""
    from kart.tabular.working_copy import TableWorkingCopyStatus
    from kart.tabular.working_copy.base import TableWorkingCopy

    # This function is relatively complex because it has a few different possibilities -
    # - old and new are the same place, old and new are different, old definitely doesn't exist,
    # old may exist but we don't know until we try to connect, old is dirty, old is corrupt,
    # user wants to delete old or not, user wants to discard changes or not...

    # And, we try to handle all these situations without trying to connect to the old database
    # too many times, or at all if we don't need to. The old database could be completely gone /
    # unavailable, or it could be corrupt, and trying to connect if we don't need to could cause
    # this command to fail unnecessarily, or could cause an unnecessary long wait before a timeout.

    old_wc_loc = repo.workingcopy_location
    if not new_wc_loc and old_wc_loc is not None:
        new_wc_loc = old_wc_loc
    elif not new_wc_loc:
        new_wc_loc = TableWorkingCopy.default_location(repo)

    if new_wc_loc != old_wc_loc:
        TableWorkingCopy.check_valid_creation_location(new_wc_loc, repo)

    if TableWorkingCopy.clearly_doesnt_exist(old_wc_loc, repo):
        old_wc_loc = None

    if old_wc_loc:
        old_wc = TableWorkingCopy.get_at_location(
            repo,
            old_wc_loc,
            allow_uncreated=True,
            allow_invalid_state=True,
            allow_unconnectable=True,
        )

        if delete_existing is None:
            if get_input_mode() is not InputMode.INTERACTIVE:
                if old_wc_loc == new_wc_loc:
                    help_message = (
                        "Specify --delete-existing to delete and recreate it."
                    )
                else:
                    help_message = "Either delete it with --delete-existing, or just abandon it with --no-delete-existing."
                raise click.UsageError(
                    f"A tabular working copy is already configured at {old_wc}\n{help_message}"
                )

            click.echo(f"A tabular working copy is already configured at {old_wc}")
            delete_existing = click.confirm(
                "Delete the existing working copy before creating a new one?",
                default=True,
            )

        check_if_dirty = not discard_changes

        if delete_existing is False:
            allow_unconnectable = old_wc_loc != new_wc_loc
            status = old_wc.status(
                allow_unconnectable=allow_unconnectable, check_if_dirty=check_if_dirty
            )
            if old_wc_loc == new_wc_loc and status & TableWorkingCopyStatus.WC_EXISTS:
                raise InvalidOperation(
                    f"Cannot recreate working copy at same location {old_wc} if --no-delete-existing is set."
                )

            if not discard_changes and (status & TableWorkingCopyStatus.DIRTY):
                raise InvalidOperation(
                    f"You have uncommitted changes at {old_wc}.\n"
                    + _DISCARD_CHANGES_HELP_MESSAGE
                )

        if delete_existing is True:
            try:
                status = old_wc.status(check_if_dirty=check_if_dirty)
            except DbConnectionError as e:
                click.echo(
                    f"Encountered an error while trying to delete existing working copy at {old_wc}"
                )
                click.echo(
                    "To simply abandon the existing working copy, use --no-delete-existing."
                )
                raise e

            if not discard_changes and (status & TableWorkingCopyStatus.DIRTY):
                raise InvalidOperation(
                    f"You have uncommitted changes at {old_wc}.\n"
                    + _DISCARD_CHANGES_HELP_MESSAGE
                )

            if status & TableWorkingCopyStatus.WC_EXISTS:
                click.echo(f"Deleting existing working copy at {old_wc}")
                keep_db_schema_if_possible = old_wc_loc == new_wc_loc
                old_wc.delete(keep_db_schema_if_possible=keep_db_schema_if_possible)

    TableWorkingCopy.write_config(repo, new_wc_loc)
    repo.working_copy.create_parts_if_missing(
        [PartType.TABULAR], reset_to=repo.head_commit
    )


def create_workdir(repo, delete_existing, discard_changes):
    """Create or recreate the file-system working copy."""

    from kart.workdir import FileSystemWorkingCopy, FileSystemWorkingCopyStatus

    # This one is much simpler since old and new are always the same place, and,
    # we can cheaply check if the files exist or not - we don't need to connect to anything.

    old_wc = FileSystemWorkingCopy.get(
        repo, allow_uncreated=True, allow_invalid_state=True
    )
    status = old_wc.status()

    if status != FileSystemWorkingCopyStatus.UNCREATED:
        if delete_existing is None:
            if get_input_mode() is not InputMode.INTERACTIVE:
                raise click.UsageError(
                    f"A file-system working copy already exists. Specify --delete-existing to delete and recreate it."
                )

            click.echo(f"A file-system working copy already exists.")
            delete_existing = click.confirm(
                "Delete the existing working copy before creating a new one?",
                default=True,
            )

        if delete_existing is False:
            raise InvalidOperation(
                "Cannot recreate file-system working copy if --no-delete-existing is set."
            )

    if (
        status == FileSystemWorkingCopyStatus.CREATED
        and not discard_changes
        and old_wc.is_dirty()
    ):
        raise InvalidOperation(
            f"You have uncommitted changes in the file-system working copy.\n"
            + _DISCARD_CHANGES_HELP_MESSAGE
        )

    if status != FileSystemWorkingCopyStatus.UNCREATED:
        assert delete_existing
        old_wc.delete()

    repo.working_copy.create_parts_if_missing(
        [PartType.WORKDIR], reset_to=repo.head_commit
    )


@click.command("create-workingcopy", cls=KartCommand)
@click.pass_context
@click.option(
    "--discard-changes",
    "--force",
    "-f",
    is_flag=True,
    help="Discard local changes in working copy if necessary",
)
@click.option(
    "--delete-existing/--no-delete-existing",
    help="Whether to delete the existing working copy",
    required=False,
    default=None,
)
@click.option(
    "--parts",
    type=click.Choice(["tabular", "file-system", "auto"]),
    default="auto",
    help=(
        "Which parts of the working copy to create / recreate. "
        'The default "auto" creates those parts that are required to store the contents of the Kart repo.'
    ),
)
@click.argument("tabular_location", nargs=1, required=False)
def create_workingcopy(ctx, parts, delete_existing, discard_changes, tabular_location):
    """
    Create or recreate a new working copy (or just certain parts of the working copy).
    If the required working copy parts already exist, they will be deleted before being recreated.
    The parts of the working copy that are required depends on the contents of your Kart repository -
    if your repository contains tabular datasets, you need a GPKG file or database in which to view and edit them.

    Usage: kart create-workingcopy [TABULAR-LOCATION]
    TABULAR-LOCATION specifies where the tabular part of the working copy should be created:
    - PATH.gpkg for a GPKG file.
    - postgresql://HOST/DBNAME/DBSCHEMA for a PostGIS database.
    - mssql://HOST/DBNAME/DBSCHEMA for a SQL Server database.
    - mysql://HOST/DBNAME for a MySQL database.
    If no such location is supplied, but a tabular part of the working copy is required, the location from the repo
    config at "kart.workingcopy.location" will be used. If no such location is configured, a GPKG working copy will be
    created with a default name based on the repository name.
    """
    repo = ctx.obj.repo
    if repo.head_is_unborn:
        raise InvalidOperation(
            "Can't create a working copy for an empty repository — first import some data with `kart import`"
        )

    if parts == "auto":
        parts = repo.datasets().working_copy_part_types()
        if tabular_location:
            parts.add(PartType.TABULAR)
    else:
        if tabular_location and parts != "tabular":
            raise click.UsageError(
                "The tabular-location should only be specified when creating a tabular working copy (--parts=tabular)"
            )
        parts = set(
            [{"tabular": PartType.TABULAR, "file-system": PartType.WORKDIR}[parts]]
        )

    if PartType.TABULAR in parts:
        create_tabular_workingcopy(
            repo, delete_existing, discard_changes, tabular_location
        )
    if PartType.WORKDIR in parts:
        create_workdir(repo, delete_existing, discard_changes)

    # This command is used in tests and by other commands, so we have to be extra careful to
    # tidy up properly - otherwise, tests can fail (on Windows especially) due to PermissionError.
    repo.free()
    del repo
