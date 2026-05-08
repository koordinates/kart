import functools

import click
import pygit2

from kart.cli_util import KartCommand
from kart.completion_shared import ref_completer
from kart.exceptions import (
    NO_BRANCH,
    NO_COMMIT,
    InvalidOperation,
    NotFound,
    SubprocessError,
)
from kart.key_filters import RepoKeyFilter
from kart.promisor_utils import get_partial_clone_envelope
from kart.spatial_filter import SpatialFilterString, spatial_filter_help_text
from kart.structs import CommitWithReference
from kart import subprocess_util as subprocess


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
@click.option(
    "--dataset",
    "do_checkout_spec",
    multiple=True,
    help="Request that a particular dataset be checked out (one which is currently configured to not be checked out)",
)
@click.option(
    "--not-dataset",
    "non_checkout_spec",
    multiple=True,
    help="Request that a particular dataset *not* be checked out (one which is currently configured to be checked out)",
)
@click.argument(
    "refish",
    default=None,
    required=False,
    shell_complete=ref_completer,  # type: ignore[call-arg]
)
def checkout(
    ctx,
    new_branch,
    force,
    discard_changes,
    do_guess,
    spatial_filter_spec,
    do_checkout_spec,
    non_checkout_spec,
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

    non_checkout_datasets = repo.non_checkout_datasets
    if do_checkout_spec or non_checkout_spec:
        do_checkout_spec = set(do_checkout_spec)
        non_checkout_spec = set(non_checkout_spec)
        _verify_checkout_datasets_spec(
            repo,
            commit,
            refish,
            do_checkout_spec,
            non_checkout_spec,
            non_checkout_datasets,
        )
        non_checkout_datasets = (
            non_checkout_datasets | non_checkout_spec
        ) - do_checkout_spec

    do_switch_checkout_datasets = not repo.working_copy.matches_non_checkout_datasets(
        non_checkout_datasets
    )

    # Again, we also allow switching of set of checked out / non-checked out datasets just by
    # writing it directly to the config and then running `kart checkout`, but using
    # `kart checkout --dataset=foo --not-dataset=bar` is preferred.
    if do_switch_checkout_datasets and not (do_checkout_spec or non_checkout_spec):
        click.echo(
            "The set of datasets to be checked out has been updated in the config and no longer matches the working copy."
        )

    discard_changes = discard_changes or force
    if (
        do_switch_commit or do_switch_spatial_filter or do_switch_checkout_datasets
    ) and not discard_changes:
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

    if do_checkout_spec or non_checkout_spec:
        repo.configure_do_checkout_datasets(do_checkout_spec, True)
        repo.configure_do_checkout_datasets(non_checkout_spec, False)

    TableWorkingCopy.ensure_config_exists(repo)
    repo.set_head(head_ref)

    repo_key_filter = (
        RepoKeyFilter.exclude_datasets(non_checkout_datasets)
        if non_checkout_datasets
        else RepoKeyFilter.MATCH_ALL
    )
    parts_to_create = (
        repo.datasets(repo_key_filter=repo_key_filter).working_copy_part_types()
        if not repo.head_is_unborn
        else ()
    )

    old_tree = repo.head_tree if not repo.head_is_unborn else None
    if do_switch_commit or do_switch_spatial_filter or discard_changes:
        # Changing commit, changing spatial filter, or discarding changes mean we need to update every dataset:
        repo.working_copy.reset_to_head(
            rewrite_full=do_switch_spatial_filter,
            create_parts_if_missing=parts_to_create,
            non_checkout_datasets=non_checkout_datasets,
        )
        if do_switch_commit or discard_changes:
            _restore_attachments_to_head(repo, old_tree=old_tree if do_switch_commit else None)
    elif do_switch_checkout_datasets:
        # Not doing any of the above - just need to change those datasets newly added / removed from the non_checkout_list.
        repo.working_copy.reset_to_head(
            non_checkout_datasets=non_checkout_datasets,
            only_update_checkout_datasets=True,
            create_parts_if_missing=parts_to_create,
        )
    elif parts_to_create:
        # Possibly we needn't auto-create any working copy here at all, but lots of tests currently depend on it.
        repo.working_copy.create_parts_if_missing(
            parts_to_create,
            reset_to=repo.head_commit,
            non_checkout_datasets=non_checkout_datasets,
        )


def _verify_checkout_datasets_spec(
    repo, commit, refish, do_checkout_spec, non_checkout_spec, non_checkout_datasets
):
    # Check the set of datasets that the user wants to check out / not check out, to make sure we've heard of them.
    # (avoid the bad experience where the user disables check out of non-existing dataset "foo-bar" instead of "foo_bar").
    if do_checkout_spec & non_checkout_spec:
        bad_ds = next(iter(do_checkout_spec & non_checkout_spec))
        raise click.BadParameter(
            f"Dataset {bad_ds} should not be present in both --dataset and --not-dataset",
            param_hint="dataset",
        )
    # Only datasets that are not already in the config are checked - if the user managed to mark it as non-checkout before,
    # they can mark it as checkout now, even if we can't find it any more.
    new_spec = (do_checkout_spec | non_checkout_spec) - non_checkout_datasets
    if not new_spec:
        return
    datasets_at_commit = repo.datasets(commit)
    for ds_path in new_spec:
        if ds_path not in datasets_at_commit:
            raise click.BadParameter(
                f"No dataset {ds_path} at commit {refish or 'HEAD'}",
                param_hint="dataset" if ds_path in do_checkout_spec else "not-dataset",
            )


@functools.lru_cache()
def _git_fetch_supports_flag(repo, flag):
    r = subprocess.run(
        ["git", "fetch", "?", f"--{flag}"],
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
@click.argument(
    "refish",
    default=None,
    required=False,
    shell_complete=ref_completer,  # type: ignore[call-arg]
)
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

    old_tree = repo.head_tree if not repo.head_is_unborn else None
    repo.set_head(head_ref)

    if do_switch_commit or discard_changes:
        repo.working_copy.reset_to_head()
        _restore_attachments_to_head(repo, old_tree=old_tree if do_switch_commit else None)


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
    shell_complete=ref_completer,  # type: ignore[call-arg]
)
@click.argument("filters", nargs=-1)
def restore(ctx, source, filters):
    """
    Restore specified paths in the working copy with some contents from the given restore source.
    By default, restores the entire working copy to the commit at HEAD (so, discards all uncommitted changes).

    FILTERS may name datasets, individual features, or attachment files. Each filter is tried
    against both datasets and attachment files — a filter matching a dataset restores all its
    features, a filter matching a file path restores that file, and a filter can match both if
    the same name exists as a dataset on one branch and a file on another.
    """
    repo = ctx.obj.repo

    repo.working_copy.assert_exists()
    repo.working_copy.assert_matches_head_tree()

    try:
        commit_or_tree, ref = repo.resolve_refish(source)
        source_tree = commit_or_tree.peel(pygit2.Tree)
    except (KeyError, pygit2.InvalidSpecError):
        raise NotFound(f"{source} is not a commit or tree", exit_code=NO_COMMIT)

    repo.working_copy.reset(
        commit_or_tree,
        track_changes_as_dirty=True,
        repo_key_filter=RepoKeyFilter.build_from_user_patterns(filters),
    )

    # Restore attachment files. With no user-supplied filters we restore every tracked file in the
    # source tree (matching the all-datasets restore above). Otherwise we pass the same filters to
    # the attachment restore: each filter is tried as both a dataset filter (above) and an
    # attachment-file path (here), so the correct side silently wins regardless of which branch a
    # dataset happens to exist on.
    if not filters:
        _restore_all_attachment_files(repo, source_tree)
    else:
        _restore_attachment_files(repo, source_tree, filters)


def _restore_attachment_files(repo, source_tree, rel_paths):
    """Restores each rel_path under the working directory from source_tree via `git checkout`.

    Paths that do not exist as files in source_tree are silently skipped, so callers can safely
    pass the same filter list that was used for dataset restore (the reviewer's preferred approach:
    each filter is tried on both sides; the side that matches wins).
    """
    workdir = str(repo.workdir_path)
    for rel_path in rel_paths:
        try:
            subprocess.check_call(
                ["git", "-C", workdir, "checkout", source_tree.id.hex, "--", rel_path],
                stderr=subprocess.DEVNULL,
            )
        except subprocess.CalledProcessError:
            # Path doesn't exist as a file in source_tree (e.g. it's a dataset name or
            # feature key) — silently skip so the caller can pass unfiltered user args.
            pass


def _restore_all_attachment_files(repo, source_tree):
    """Restores every attachment file present in source_tree to the working directory."""
    from kart.diff_util import ls_tree_attachments

    workdir = str(repo.workdir_path)
    tracked = ls_tree_attachments(workdir, source_tree.id.hex)
    if not tracked:
        return
    _restore_attachment_files(repo, source_tree, sorted(tracked))


def _remove_deleted_attachment_files(repo, old_tree, new_tree):
    """
    Deletes any attachment files that existed in old_tree but are absent from new_tree.
    Called after a HEAD switch so that files removed in the new commit disappear from the
    working directory rather than lingering as untracked files.
    """
    from kart.diff_util import ls_tree_attachments
    from pathlib import Path

    workdir = str(repo.workdir_path)
    old_files = set(ls_tree_attachments(workdir, old_tree.id.hex))
    new_files = set(ls_tree_attachments(workdir, new_tree.id.hex))
    for rel_path in sorted(old_files - new_files):
        full_path = Path(workdir) / rel_path
        if full_path.is_file():
            full_path.unlink()


def _restore_attachments_to_head(repo, old_tree=None):
    """
    Restore tracked attachment files in the working directory to the state at HEAD.

    Kart's working-copy reset only covers dataset contents (the GeoPackage / workdir parts).
    Attachment files (LICENSE.txt, README.md, project files, etc. tracked alongside datasets)
    are plain Git objects and need to be re-extracted explicitly when HEAD changes.

    If old_tree is provided (the tree before the HEAD switch), any attachment files that
    existed in old_tree but are absent from the new HEAD are deleted from the working directory.
    """
    if repo.head_is_unborn:
        return
    new_tree = repo.head_tree
    _restore_all_attachment_files(repo, new_tree)
    if old_tree is not None:
        _remove_deleted_attachment_files(repo, old_tree, new_tree)




@click.command(cls=KartCommand)
@click.pass_context
@click.option(
    "--discard-changes",
    "--force",
    "-f",
    is_flag=True,
    help="Discard local changes in working copy if necessary",
)
@click.argument(
    "refish",
    default="HEAD",
    shell_complete=ref_completer,  # type: ignore[call-arg]
)
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

    old_tree = repo.head_tree if not repo.head_is_unborn else None
    head_branch = repo.head_branch
    if head_branch is not None:
        repo.references[head_branch].set_target(commit.id)
    else:
        repo.set_head(commit.id)

    repo.working_copy.reset_to_head()
    _restore_attachments_to_head(repo, old_tree=old_tree if do_switch_commit else None)
