import os
from pathlib import Path

import click
import pygit2

from .exceptions import (
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
    " just discard them by adding the option `--discard_changes`."
)


def reset_wc_if_needed(repo, target_tree_or_commit, *, discard_changes=False):
    """Resets the working copy to the target if it does not already match, or if discard_changes is True."""
    wc = WorkingCopy.get(repo, create_if_missing=True)
    if not wc:
        click.echo(
            "(Bare sno repository - to create a working copy, use `sno create-workingcopy`)"
        )
        return

    if not wc.is_created():
        click.echo(f'Creating working copy at {wc.path} ...')
        wc.create()
        for dataset in list(RepositoryStructure(repo)):
            wc.write_full(target_tree_or_commit, dataset, safe=False)
        return

    db_tree_matches = wc.get_db_tree() == target_tree_or_commit.peel(pygit2.Tree).hex

    if discard_changes or not db_tree_matches:
        click.echo(f'Updating {wc.path} ...')
        wc.reset(target_tree_or_commit, force=discard_changes)


@click.command()
@click.pass_context
@click.option("branch", "-b", help="Name for new branch")
@click.option(
    "--force",
    "-f",
    is_flag=True,
    help="Discard local changes in working copy if necessary",
)
@click.option(
    "--discard-changes", is_flag=True, help="Discard local changes in working copy"
)
@click.argument("refish", default=None, required=False)
def checkout(ctx, branch, force, discard_changes, refish):
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

    if refish:
        resolved = CommitWithReference.resolve(repo, refish)
    else:
        resolved = CommitWithReference.resolve(repo, "HEAD")

    commit = resolved.commit
    head_ref = resolved.reference.name if resolved.reference else commit.id
    same_commit = repo.head.peel(pygit2.Commit) == commit

    force = force or discard_changes
    if not same_commit and not force:
        ctx.obj.check_not_dirty(help_message=_DISCARD_CHANGES_HELP_MESSAGE)

    if branch:
        if branch in repo.branches:
            raise click.BadParameter(
                f"A branch named '{branch}' already exists.", param_hint="branch"
            )

        if refish and refish in repo.branches.remote:
            click.echo(f"Creating new branch '{branch}' to track '{refish}'...")
            new_branch = repo.create_branch(branch, commit, force)
            new_branch.upstream = repo.branches.remote[refish]
        elif refish:
            click.echo(f"Creating new branch '{branch}' from '{refish}'...")
            new_branch = repo.create_branch(branch, commit, force)
        else:
            click.echo(f"Creating new branch '{branch}'...")
            new_branch = repo.create_branch(branch, commit, force)

        head_ref = new_branch.name

    reset_wc_if_needed(repo, commit, discard_changes=discard_changes)

    repo.set_head(head_ref)
    repo.reset(commit.oid, pygit2.GIT_RESET_SOFT)


@click.command()
@click.pass_context
@click.option("--create", "-c", help="Create a new branch")
@click.option(
    "--force-create",
    "-C",
    help="Similar to --create except that if <new-branch> already exists, it will be reset to <start-point>",
)
@click.option("--discard-changes", is_flag=True, help="Discard local changes")
@click.argument("refish", default=None, required=False)
def switch(ctx, create, force_create, discard_changes, refish):
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
        # - branch name
        # - tag name
        # - remote branch
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
            print(f"Creating new branch '{new_branch}' to track '{start_point}'...")
            b_new = repo.create_branch(new_branch, commit, is_force)
            b_new.upstream = repo.branches.remote[start_point]
        elif start_point and start_point in repo.branches:
            print(f"Creating new branch '{new_branch}' from '{start_point}'...")
            b_new = repo.create_branch(new_branch, commit, is_force)
        else:
            print(f"Creating new branch '{new_branch}'...")
            b_new = repo.create_branch(new_branch, commit, is_force)

        head_ref = b_new.name

    else:
        # Switch to existing branch
        #
        # refish could be:
        # - branch name
        try:
            branch = repo.branches[refish]
        except KeyError:
            raise NotFound(f"Branch '{refish}' not found.", NO_BRANCH)
        commit = branch.peel(pygit2.Commit)

        same_commit = repo.head.peel(pygit2.Commit) == commit
        if not discard_changes and not same_commit:
            ctx.obj.check_not_dirty(_DISCARD_CHANGES_HELP_MESSAGE)

        head_ref = branch.name

    reset_wc_if_needed(repo, commit, discard_changes=discard_changes)

    repo.set_head(head_ref)
    repo.reset(commit.oid, pygit2.GIT_RESET_SOFT)


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

    head_commit = repo.head.peel(pygit2.Commit)
    try:
        commit_or_tree, ref = repo.resolve_refish(source)
        commit_or_tree.peel(pygit2.Tree)
    except (KeyError, pygit2.InvalidSpecError):
        raise NotFound(f"{source} is not a commit or tree", exit_code=NO_COMMIT)

    working_copy.reset(
        commit_or_tree,
        force=True,
        update_meta=(head_commit.id == commit_or_tree.id),
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
@click.argument("path", nargs=1, type=click.Path(dir_okay=False), required=False)
@click.argument("version", nargs=1, type=int, required=False)
def create_workingcopy(ctx, discard_changes, path, version):
    """ Create a new working copy - if one already exists it will be deleted """
    if not discard_changes:
        ctx.obj.check_not_dirty(_DISCARD_CHANGES_HELP_MESSAGE)

    if path is not None:
        path = Path(path)
        if not path.is_absolute():
            # Note: This is basically path = normpath(path)
            repo_path = ctx.obj.repo_path
            path = os.path.relpath(os.path.join(repo_path, path), repo_path)

    repo = ctx.obj.repo
    wc = WorkingCopy.get(repo)
    if wc:
        wc.delete()

    WorkingCopy.write_config(repo, path, version)
    head_commit = repo.head.peel(pygit2.Commit)
    reset_wc_if_needed(repo, head_commit)
