import click
import pygit2

from .exceptions import (
    InvalidOperation,
    NotFound,
    NO_BRANCH,
    NO_COMMIT,
    NO_WORKING_COPY,
)

from .exceptions import DbConnectionError
from .structs import CommitWithReference
from .working_copy import WorkingCopyStatus
from .output_util import InputMode, get_input_mode

_DISCARD_CHANGES_HELP_MESSAGE = (
    "Commit these changes first (`kart commit`) or"
    " just discard them by adding the option `--discard-changes`."
)


def reset_wc_if_needed(repo, target_tree_or_commit, *, discard_changes=False):
    """Resets the working copy to the target if it does not already match, or if discard_changes is True."""
    if repo.is_bare:
        return

    working_copy = repo.get_working_copy(allow_uncreated=True, allow_invalid_state=True)
    if working_copy is None:
        click.echo(
            "(Working copy isn't created yet. To create a working copy, use `kart create-workingcopy`)"
        )
        return

    if not (working_copy.status() & WorkingCopyStatus.INITIALISED):
        click.echo(f"Creating working copy at {working_copy} ...")
        working_copy.create_and_initialise()
        datasets = list(repo.datasets())
        working_copy.write_full(target_tree_or_commit, *datasets, safe=False)

    db_tree_matches = (
        working_copy.get_db_tree() == target_tree_or_commit.peel(pygit2.Tree).hex
    )

    if discard_changes or not db_tree_matches:
        click.echo(f"Updating {working_copy} ...")
        working_copy.reset(target_tree_or_commit, force=discard_changes)


@click.command()
@click.pass_context
@click.option("new_branch", "-b", help="Name for new branch")
@click.option(
    "--force",
    "-f",
    is_flag=True,
    help="Discard local changes in working copy if necessary",
)
@click.option(
    "--discard-changes", is_flag=True, help="Discard local changes in working copy"
)
@click.option(
    "--guess/--no-guess",
    "do_guess",
    is_flag=True,
    default=True,
    help="If a local branch of given name doesn't exist, but a remote does, "
    "this option guesses that the user wants to create a local to track the remote",
)
@click.argument("refish", default=None, required=False)
def checkout(ctx, new_branch, force, discard_changes, do_guess, refish):
    """ Switch branches or restore working tree files """
    repo = ctx.obj.repo

    # refish could be:
    # - branch name
    # - tag name
    # - remote branch
    # - HEAD
    # - HEAD~1/etc
    # - 'c0ffee' commit ref
    # - 'refs/tags/1.2.3' some other refspec

    try:
        if refish:
            resolved = CommitWithReference.resolve(repo, refish)
        else:
            resolved = CommitWithReference.resolve(repo, "HEAD")
    except NotFound:
        # Guess: that the user wants create a new local branch to track a remote
        remote_branch = (
            _find_remote_branch_by_name(repo, refish) if do_guess and refish else None
        )
        if remote_branch:
            new_branch = refish
            refish = remote_branch.shorthand
            resolved = CommitWithReference.resolve(repo, refish)
        else:
            raise

    commit = resolved.commit
    head_ref = resolved.reference.name if resolved.reference else commit.id
    same_commit = repo.head_commit == commit

    force = force or discard_changes
    if not same_commit and not force:
        ctx.obj.check_not_dirty(help_message=_DISCARD_CHANGES_HELP_MESSAGE)

    if new_branch:
        if new_branch in repo.branches:
            raise click.BadParameter(
                f"A branch named '{new_branch}' already exists.", param_hint="branch"
            )

        if refish and refish in repo.branches.remote:
            click.echo(f"Creating new branch '{new_branch}' to track '{refish}'...")
            new_branch = repo.create_branch(new_branch, commit, force)
            new_branch.upstream = repo.branches.remote[refish]
        elif refish:
            click.echo(f"Creating new branch '{new_branch}' from '{refish}'...")
            new_branch = repo.create_branch(new_branch, commit, force)
        else:
            click.echo(f"Creating new branch '{new_branch}'...")
            new_branch = repo.create_branch(new_branch, commit, force)

        head_ref = new_branch.name

    from sno.working_copy.base import BaseWorkingCopy

    BaseWorkingCopy.ensure_config_exists(repo)
    reset_wc_if_needed(repo, commit, discard_changes=discard_changes)

    repo.set_head(head_ref)


@click.command()
@click.pass_context
@click.option("--create", "-c", help="Create a new branch")
@click.option(
    "--force-create",
    "-C",
    help="Similar to --create except that if <new-branch> already exists, it will be reset to <start-point>",
)
@click.option("--discard-changes", is_flag=True, help="Discard local changes")
@click.option(
    "--guess/--no-guess",
    "do_guess",
    is_flag=True,
    default=True,
    help="If a local branch of given name doesn't exist, but a remote does, "
    "this option guesses that the user wants to create a local to track the remote",
)
@click.argument("refish", default=None, required=False)
def switch(ctx, create, force_create, discard_changes, do_guess, refish):
    """
    Switch branches

    Switch to a specified branch. The working copy and the index are updated
    to match the branch. All new commits will be added to the tip of this
    branch.

    Optionally a new branch could be created with either -c, -C, automatically
    from a remote branch of same name.

    REFISH is either the branch name to switch to, or start-point of new branch for -c/--create.
    """
    repo = ctx.obj.repo

    if create and force_create:
        raise click.BadParameter("-c/--create and -C/--force-create are incompatible")

    if create or force_create:
        # New Branch
        new_branch = force_create or create
        is_force = bool(force_create)

        # refish could be:
        # - '' -> HEAD
        # - branch name eg 'main'
        # - tag name
        # - remote branch eg 'origin/main'
        # - HEAD
        # - HEAD~1/etc
        # - 'c0ffee' commit ref
        # - 'refs/tags/1.2.3' some other refspec
        start_point = refish
        if start_point:
            resolved = CommitWithReference.resolve(repo, start_point)
        else:
            resolved = CommitWithReference.resolve(repo, "HEAD")
        commit = resolved.commit

        same_commit = repo.head_commit == commit
        if not discard_changes and not same_commit:
            ctx.obj.check_not_dirty(_DISCARD_CHANGES_HELP_MESSAGE)

        if new_branch in repo.branches and not force_create:
            raise click.BadParameter(
                f"A branch named '{new_branch}' already exists.", param_hint="create"
            )

        if start_point and start_point in repo.branches.remote:
            click.echo(
                f"Creating new branch '{new_branch}' to track '{start_point}'..."
            )
            b_new = repo.create_branch(new_branch, commit, is_force)
            b_new.upstream = repo.branches.remote[start_point]
        elif start_point and start_point in repo.branches:
            click.echo(f"Creating new branch '{new_branch}' from '{start_point}'...")
            b_new = repo.create_branch(new_branch, commit, is_force)
        else:
            click.echo(f"Creating new branch '{new_branch}'...")
            b_new = repo.create_branch(new_branch, commit, is_force)

        head_ref = b_new.name

    else:
        # Switch to existing branch
        #
        # refish could be:
        # - local branch name (eg 'main')
        # - local branch name (eg 'main') that as yet only exists on remote (if do_guess is True)
        #   (But not a remote branch eg 'origin/main')
        if not refish:
            raise click.UsageError("Missing argument: REFISH")

        if refish in repo.branches.remote:
            # User specified something like "origin/main"
            raise click.BadParameter(
                f"A branch is expected, got remote branch {refish}",
                param_hint="refish",
            )

        existing_branch = None
        if refish in repo.branches.local:
            existing_branch = repo.branches[refish]
        elif do_guess:
            # Guess: that the user wants create a new local branch to track a remote
            existing_branch = _find_remote_branch_by_name(repo, refish)

        if not existing_branch:
            raise NotFound(f"Branch '{refish}' not found.", NO_BRANCH)

        commit = existing_branch.peel(pygit2.Commit)
        same_commit = repo.head_commit == commit
        if not discard_changes and not same_commit:
            ctx.obj.check_not_dirty(_DISCARD_CHANGES_HELP_MESSAGE)

        if existing_branch.shorthand in repo.branches.local:
            branch = existing_branch
        else:
            # Create new local branch to track remote
            click.echo(
                f"Creating new branch '{refish}' to track '{existing_branch.shorthand}'..."
            )
            branch = repo.create_branch(refish, commit)
            branch.upstream = existing_branch

        head_ref = branch.name

    reset_wc_if_needed(repo, commit, discard_changes=discard_changes)

    repo.set_head(head_ref)


def _find_remote_branch_by_name(repo, name):
    """
    Returns the only remote branch with the given name eg "main".
    Returns None if there is no remote branch with that unique name.
    """
    results = []
    remotes = repo.branches.remote
    for b in remotes:
        parts = b.split("/", 1)
        if len(parts) == 2 and parts[1] == name:
            results.append(remotes[b])
    return results[0] if len(results) == 1 else None


@click.command()
@click.pass_context
@click.option(
    "--source",
    "-s",
    help=(
        "Restore the working copy with the content from the given tree. "
        "It is common to specify the source tree by naming a commit, branch or "
        "tag associated with it. "
    ),
    default="HEAD",
)
@click.argument("pathspec", nargs=-1)
def restore(ctx, source, pathspec):
    """
    Restore specified paths in the working copy with some contents from a restore source.
    """
    repo = ctx.obj.repo

    working_copy = repo.working_copy
    if not working_copy:
        raise NotFound("You don't have a working copy", exit_code=NO_WORKING_COPY)

    try:
        commit_or_tree, ref = repo.resolve_refish(source)
        commit_or_tree.peel(pygit2.Tree)
    except (KeyError, pygit2.InvalidSpecError):
        raise NotFound(f"{source} is not a commit or tree", exit_code=NO_COMMIT)

    working_copy.reset(
        commit_or_tree,
        force=True,
        track_changes_as_dirty=True,
        paths=pathspec,
    )


@click.command()
@click.pass_context
@click.option(
    "--discard-changes",
    "--force",
    "-f",
    is_flag=True,
    help="Discard local changes in working copy if necessary",
)
@click.argument("refish")
def reset(ctx, discard_changes, refish):
    """
    Reset the branch head to point to a different commit.
    """
    repo = ctx.obj.repo

    try:
        commit_or_tree, ref = repo.resolve_refish(refish)
        commit = commit_or_tree.peel(pygit2.Commit)
    except (KeyError, pygit2.InvalidSpecError):
        raise NotFound(f"{refish} is not a commit", exit_code=NO_COMMIT)

    same_commit = repo.head_commit == commit
    if not discard_changes and not same_commit:
        ctx.obj.check_not_dirty(_DISCARD_CHANGES_HELP_MESSAGE)

    head_branch = repo.head_branch
    if head_branch is not None:
        repo.references[head_branch].set_target(commit.id)
    else:
        repo.set_head(commit.id)

    reset_wc_if_needed(repo, repo.head_commit, discard_changes=discard_changes)


@click.command("create-workingcopy")
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
@click.argument("new_wc_loc", nargs=1, required=False)
def create_workingcopy(ctx, delete_existing, discard_changes, new_wc_loc):
    """
    Create a new working copy - if one already exists it will be deleted.
    Usage: kart create-workingcopy [LOCATION]
    LOCATION should be one of the following:
    - PATH.gpkg for a GPKG file.
    - postgresql://[HOST]/DBNAME/DBSCHEMA for a PostGIS database.
    - mssql://[HOST]/DBNAME/DBSCHEMA for a SQL Server database.
    If no location is supplied, the location from the repo config at "kart.workingcopy.location" will be used.
    If no location is configured, a GPKG working copy will be created with a default name based on the repository name.
    """
    from sno.working_copy.base import BaseWorkingCopy

    repo = ctx.obj.repo
    if repo.head_is_unborn:
        raise InvalidOperation(
            "Can't create a working copy for an empty repository — first import some data with `kart import`"
        )

    old_wc_loc = repo.workingcopy_location
    if not new_wc_loc and old_wc_loc is not None:
        new_wc_loc = old_wc_loc
    elif not new_wc_loc:
        new_wc_loc = BaseWorkingCopy.default_location(repo)

    if new_wc_loc != old_wc_loc:
        BaseWorkingCopy.check_valid_creation_location(new_wc_loc, repo)

    if old_wc_loc:
        old_wc = BaseWorkingCopy.get_at_location(
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
                    f"A working copy is already configured at {old_wc}\n{help_message}"
                )

            click.echo(f"A working copy is already configured at {old_wc}")
            delete_existing = click.confirm(
                "Delete the existing working copy before creating a new one?",
                default=True,
            )

        if delete_existing is False:
            allow_unconnectable = old_wc_loc != new_wc_loc
            status = old_wc.status(
                allow_unconnectable=allow_unconnectable, check_if_dirty=True
            )
            if old_wc_loc == new_wc_loc and status & WorkingCopyStatus.WC_EXISTS:
                raise InvalidOperation(
                    f"Cannot recreate working copy at same location {old_wc} if --no-delete-existing is set."
                )

            if not discard_changes and (status & WorkingCopyStatus.DIRTY):
                raise InvalidOperation(
                    f"You have uncommitted changes at {old_wc}.\n"
                    + _DISCARD_CHANGES_HELP_MESSAGE
                )

        if delete_existing is True:
            try:
                status = old_wc.status(check_if_dirty=True)
            except DbConnectionError as e:
                click.echo(
                    f"Encountered an error while trying to delete existing working copy at {old_wc}"
                )
                click.echo(
                    "To simply abandon the existing working copy, use --no-delete-existing."
                )
                raise e

            if not discard_changes and (status & WorkingCopyStatus.DIRTY):
                raise InvalidOperation(
                    f"You have uncommitted changes at {old_wc}.\n"
                    + _DISCARD_CHANGES_HELP_MESSAGE
                )

            if status & WorkingCopyStatus.WC_EXISTS:
                click.echo(f"Deleting existing working copy at {old_wc}")
                keep_db_schema_if_possible = old_wc_loc == new_wc_loc
                old_wc.delete(keep_db_schema_if_possible=keep_db_schema_if_possible)

    BaseWorkingCopy.write_config(repo, new_wc_loc)
    reset_wc_if_needed(repo, repo.head_commit)

    # This command is used in tests and by other commands, so we have to be extra careful to
    # tidy up properly - otherwise, tests can fail (on Windows especially) due to PermissionError.
    repo.free()
    del repo
