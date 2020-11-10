import click
import pygit2

from .exceptions import (
    InvalidOperation,
    NotFound,
    NO_BRANCH,
    NO_COMMIT,
    NO_WORKING_COPY,
)

from .structure import RepositoryStructure
from .structs import CommitWithReference
from .working_copy import WorkingCopy

_DISCARD_CHANGES_HELP_MESSAGE = (
    "Commit these changes first (`sno commit`) or"
    " just discard them by adding the option `--discard-changes`."
)


def reset_wc_if_needed(repo, target_tree_or_commit, *, discard_changes=False):
    """Resets the working copy to the target if it does not already match, or if discard_changes is True."""
    working_copy = WorkingCopy.get(repo, allow_uncreated=True)
    if working_copy is None:
        click.echo(
            "(Bare sno repository - to create a working copy, use `sno create-workingcopy`)"
        )
        return

    if not working_copy.is_initialised():
        click.echo(f"Creating working copy at {working_copy.path} ...")
        working_copy.create_and_initialise()
        datasets = list(RepositoryStructure(repo))
        working_copy.write_full(target_tree_or_commit, *datasets, safe=False)

    db_tree_matches = (
        working_copy.get_db_tree() == target_tree_or_commit.peel(pygit2.Tree).hex
    )

    if discard_changes or not db_tree_matches:
        click.echo(f"Updating {working_copy.path} ...")
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
    same_commit = repo.head.peel(pygit2.Commit) == commit

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

    WorkingCopy.ensure_config_exists(repo)
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
        # - branch name eg 'master'
        # - tag name
        # - remote branch eg 'origin/master'
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

        same_commit = repo.head.peel(pygit2.Commit) == commit
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
        # - local branch name (eg 'master')
        # - local branch name (eg 'master') that as yet only exists on remote (if do_guess is True)
        #   (But not a remote branch eg 'origin/master')
        if not refish:
            raise click.UsageError("Missing argument: REFISH")

        if refish in repo.branches.remote:
            # User specified something like "origin/master"
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
        same_commit = repo.head.peel(pygit2.Commit) == commit
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
    Returns the only remote branch with the given name eg "master".
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
        "Restore the working tree files with the content from the given tree. "
        "It is common to specify the source tree by naming a commit, branch or "
        "tag associated with it."
    ),
    default="HEAD",
)
@click.argument("pathspec", nargs=-1)
def restore(ctx, source, pathspec):
    """
    Restore specified paths in the working tree with some contents from a restore source.
    """
    repo = ctx.obj.repo

    working_copy = WorkingCopy.get(repo)
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


@click.command("create-workingcopy")
@click.pass_context
@click.option(
    "--discard-changes",
    "--force",
    "-f",
    is_flag=True,
    help="Discard local changes in working copy if necessary",
)
@click.argument("wc_path", nargs=1, required=False)
def create_workingcopy(ctx, discard_changes, wc_path):
    """
    Create a new working copy - if one already exists it will be deleted.
    Usage: sno create-workingcopy [PATH]
    PATH should be a GPKG file eg example.gpkg or a postgres URI including schema eg postgresql://[HOST]/DBNAME/SCHEMA
    If no path is supplied, the path from the repo config at "sno.workingcopy.path" will be used.
    If no path is configured, a GPKG working copy will be created with a default name based on the repository name.
    """
    repo = ctx.obj.repo
    if repo.head_is_unborn:
        raise InvalidOperation(
            "Can't create a working copy for an empty repository — first import some data with `sno import`"
        )

    old_wc = WorkingCopy.get(repo, allow_invalid_state=True)
    old_wc_path = old_wc.path if old_wc else None

    if not discard_changes and old_wc and old_wc.is_initialised():
        old_wc.check_not_dirty(_DISCARD_CHANGES_HELP_MESSAGE)

    if not wc_path and WorkingCopy.SNO_WORKINGCOPY_PATH in repo.config:
        wc_path = repo.config[WorkingCopy.SNO_WORKINGCOPY_PATH]
    if not wc_path:
        wc_path = WorkingCopy.default_path(repo)

    if wc_path != old_wc_path:
        WorkingCopy.check_valid_creation_path(wc_path, repo.workdir_path)

    # Finished sanity checks - start work:
    if old_wc and wc_path != old_wc_path:
        click.echo(f"Deleting working copy at {old_wc.path} ...")
        old_wc.delete()

    WorkingCopy.write_config(repo, wc_path)

    new_wc = WorkingCopy.get(repo, allow_uncreated=True, allow_invalid_state=True)

    # Delete anything the already exists in the new target location also, and start fresh.
    if new_wc.is_created():
        click.echo(f"Deleting working copy at {new_wc.path} ...")
        # There's a possibility we lack permission to recreate the working copy container (eg a postgis schema),
        # so if it already exists, we keep that part.
        new_wc.delete(keep_container_if_possible=True)

    head_commit = repo.head.peel(pygit2.Commit)
    reset_wc_if_needed(repo, head_commit)
