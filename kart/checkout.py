import subprocess

import click
import pygit2
from .completion_shared import ref_completer

from .cli_util import tool_environment
from .exceptions import (
    NO_BRANCH,
    NO_COMMIT,
    InvalidOperation,
    NotFound,
)
from .key_filters import RepoKeyFilter
from .promisor_utils import get_partial_clone_envelope
from .spatial_filter import SpatialFilterString, spatial_filter_help_text
from .structs import CommitWithReference
from kart.cli_util import KartCommand

_DISCARD_CHANGES_HELP_MESSAGE = (
    "Commit these changes first (`kart commit`) or"
    " just discard them by adding the option `--discard-changes`."
)


@click.command(cls=KartCommand)
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
@click.option(
    "--spatial-filter",
    "spatial_filter_spec",
    type=SpatialFilterString(encoding="utf-8"),
    help=spatial_filter_help_text(),
)
@click.argument("refish", default=None, required=False, shell_complete=ref_completer)
def checkout(
    ctx,
    new_branch,
    force,
    discard_changes,
    do_guess,
    spatial_filter_spec,
    refish,
):
    """Switch branches or restore working tree files"""
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
        # Allow a pointless "kart checkout main" on empty repos already on branch main.
        if refish is not None and refish in (
            repo.head_branch,
            repo.head_branch_shorthand,
        ):
            if new_branch or spatial_filter_spec:
                raise  # But don't allow them to do anything more complicated.
            return

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
    do_switch_commit = repo.head_commit != commit

    do_switch_spatial_filter = False
    do_refetch = False
    promisor_remote = None
    if spatial_filter_spec is not None:
        resolved_spatial_filter_spec = spatial_filter_spec.resolve(repo)
        do_switch_spatial_filter = (
            not resolved_spatial_filter_spec.matches_working_copy(repo)
        )
        fetched_envelope = get_partial_clone_envelope(repo)
        do_refetch = (
            fetched_envelope
            and not resolved_spatial_filter_spec.is_within_envelope(fetched_envelope)
        )
    else:
        # We also allow switching of spatial filter by just writing it to the config and then running
        # `kart checkout`. Updating the spatial filter by running an explicit command is preferred,
        # since then we can do the necessary checks and make the change all at once, but since we
        # store the spatial filter in the config, we need to handle it if the user has changed it.
        do_switch_spatial_filter = not repo.spatial_filter.matches_working_copy(repo)
        if do_switch_spatial_filter:
            click.echo(
                "The spatial filter has been updated in the config and no longer matches the working copy."
            )

    discard_changes = discard_changes or force
    if (do_switch_commit or do_switch_spatial_filter) and not discard_changes:
        ctx.obj.check_not_dirty(help_message=_DISCARD_CHANGES_HELP_MESSAGE)

    if new_branch and new_branch in repo.branches:
        raise click.BadParameter(
            f"A branch named '{new_branch}' already exists.", param_hint="branch"
        )

    # Finished pre-flight checks - start action:

    if do_refetch:
        from .promisor_utils import get_promisor_remote

        spec = resolved_spatial_filter_spec.partial_clone_filter_spec()
        spec_desc = (
            f"git spatial filter extension {spec}" if "spatial" in spec else spec
        )

        click.echo(
            f"Fetching missing but required features for new spatial filter using {spec_desc}"
        )
        promisor_remote = get_promisor_remote(repo)
        git_refetch(repo, promisor_remote, spec)

    if new_branch:
        if _is_in_branches(refish, repo.branches.remote):
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

    from kart.tabular.working_copy.base import TableWorkingCopy

    if spatial_filter_spec is not None:
        spatial_filter_spec.write_config(repo, update_remote=promisor_remote)

    TableWorkingCopy.ensure_config_exists(repo)
    repo.set_head(head_ref)

    parts_to_create = (
        repo.datasets().working_copy_part_types() if not repo.head_is_unborn else ()
    )

    if do_switch_commit or do_switch_spatial_filter or discard_changes:
        repo.working_copy.reset_to_head(
            rewrite_full=do_switch_spatial_filter,
            create_parts_if_missing=parts_to_create,
        )
    elif parts_to_create:
        # Possibly we needn't auto-create any working copy here at all, but lots of tests currently depend on it.
        repo.working_copy.create_parts_if_missing(
            parts_to_create, reset_to=repo.head_commit
        )


def _git_fetch_supports_flag(repo, flag):
    r = subprocess.run(
        ["git", "fetch", "?", f"--{flag}"],
        env=tool_environment(),
        cwd=repo.workdir_path,
        capture_output=True,
        text=True,
    )
    return f"unknown option `{flag}'" not in r.stderr


def git_refetch(repo, promisor_remote, spec):
    # This flag was renamed. It's not too hard to avoid any assumptions and check which one is supported.
    if _git_fetch_supports_flag(repo, "refetch"):
        repo.invoke_git("fetch", promisor_remote, "--refetch", spec)
    elif _git_fetch_supports_flag(repo, "repair"):
        repo.invoke_git("fetch", promisor_remote, "--repair", spec)
    else:
        raise RuntimeError(
            "Cannot fetch missing but required features - Git is missing --refetch functionality"
        )


@click.command(cls=KartCommand)
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
@click.argument("refish", default=None, required=False, shell_complete=ref_completer)
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

        do_switch_commit = repo.head_commit != commit
        if do_switch_commit and not discard_changes:
            ctx.obj.check_not_dirty(_DISCARD_CHANGES_HELP_MESSAGE)

        if new_branch in repo.branches and not force_create:
            raise click.BadParameter(
                f"A branch named '{new_branch}' already exists.", param_hint="create"
            )

        if _is_in_branches(start_point, repo.branches.remote):
            click.echo(
                f"Creating new branch '{new_branch}' to track '{start_point}'..."
            )
            b_new = repo.create_branch(new_branch, commit, is_force)
            b_new.upstream = repo.branches.remote[start_point]
        elif _is_in_branches(start_point, repo.branches):
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

        if _is_in_branches(refish, repo.branches.remote):
            # User specified something like "origin/main"
            raise click.BadParameter(
                f"A branch is expected, got remote branch {refish}",
                param_hint="refish",
            )

        existing_branch = None
        if _is_in_branches(refish, repo.branches.local):
            existing_branch = repo.branches[refish]
        elif do_guess:
            # Guess: that the user wants create a new local branch to track a remote
            existing_branch = _find_remote_branch_by_name(repo, refish)

        if not existing_branch:
            # Allow a pointless "kart switch main" on empty repos already on branch main.
            if refish is not None and refish in (
                repo.head_branch,
                repo.head_branch_shorthand,
            ):
                return
            raise NotFound(f"Branch '{refish}' not found.", exit_code=NO_BRANCH)

        commit = existing_branch.peel(pygit2.Commit)
        do_switch_commit = repo.head_commit != commit
        if do_switch_commit and not discard_changes:
            ctx.obj.check_not_dirty(_DISCARD_CHANGES_HELP_MESSAGE)

        if _is_in_branches(existing_branch.shorthand, repo.branches.local):
            branch = existing_branch
        else:
            # Create new local branch to track remote
            click.echo(
                f"Creating new branch '{refish}' to track '{existing_branch.shorthand}'..."
            )
            branch = repo.create_branch(refish, commit)
            branch.upstream = existing_branch

        head_ref = branch.name

    repo.set_head(head_ref)

    if do_switch_commit or discard_changes:
        repo.working_copy.reset_to_head()


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


def _is_in_branches(branch_name, branches):
    if not branch_name:
        return False
    try:
        return branch_name in branches
    except pygit2.InvalidSpecError:
        return False


@click.command(cls=KartCommand)
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
    shell_complete=ref_completer,
)
@click.argument("filters", nargs=-1)
def restore(ctx, source, filters):
    """
    Restore specified paths in the working copy with some contents from the given restore source.
    By default, restores the entire working copy to the commit at HEAD (so, discards all uncommitted changes).
    """
    repo = ctx.obj.repo

    repo.working_copy.assert_exists()
    repo.working_copy.assert_matches_head_tree()

    try:
        commit_or_tree, ref = repo.resolve_refish(source)
        commit_or_tree.peel(pygit2.Tree)
    except (KeyError, pygit2.InvalidSpecError):
        raise NotFound(f"{source} is not a commit or tree", exit_code=NO_COMMIT)

    repo_key_filter = RepoKeyFilter.build_from_user_patterns(filters)

    repo.working_copy.reset(
        commit_or_tree,
        track_changes_as_dirty=True,
        repo_key_filter=repo_key_filter,
    )


@click.command(cls=KartCommand)
@click.pass_context
@click.option(
    "--discard-changes",
    "--force",
    "-f",
    is_flag=True,
    help="Discard local changes in working copy if necessary",
)
@click.argument("refish", default="HEAD", shell_complete=ref_completer)
def reset(ctx, discard_changes, refish):
    """
    Reset the branch head to point to a particular commit.
    Defaults to HEAD, which has no effect unless --discard-changes is also specified.
    """
    repo = ctx.obj.repo

    if refish == "HEAD" and not discard_changes:
        raise InvalidOperation(
            "Resetting the current branch to HEAD has no effect, unless you also discard changes.\n"
            "Do you mean `kart reset --discard-changes?`"
        )

    try:
        commit_or_tree, ref = repo.resolve_refish(refish)
        commit = commit_or_tree.peel(pygit2.Commit)
    except (KeyError, pygit2.InvalidSpecError):
        raise NotFound(f"{refish} is not a commit", exit_code=NO_COMMIT)

    do_switch_commit = repo.head_commit != commit
    if do_switch_commit and not discard_changes:
        ctx.obj.check_not_dirty(_DISCARD_CHANGES_HELP_MESSAGE)

    head_branch = repo.head_branch
    if head_branch is not None:
        repo.references[head_branch].set_target(commit.id)
    else:
        repo.set_head(commit.id)

    repo.working_copy.reset_to_head()
