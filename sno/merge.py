import logging
import sys

import click

from .cli_util import do_json_option, call_and_exit_flag
from .conflicts import (
    list_conflicts,
    conflicts_json_as_text,
)
from .exceptions import InvalidOperation
from .merge_util import AncestorOursTheirs, MergeIndex, MergeContext
from .output_util import dump_json_output
from .repo_files import (
    MERGE_HEAD,
    MERGE_MSG,
    write_repo_file,
    read_repo_file,
    remove_all_merge_repo_files,
    repo_file_exists,
    RepoState,
)

from .structs import CommitWithReference
from .structure import RepositoryStructure


L = logging.getLogger("sno.merge")


def do_merge(repo, ff, ff_only, dry_run, commit):
    """Does a merge, but doesn't update the working copy."""
    if ff_only and not ff:
        raise click.BadParameter(
            "Conflicting parameters: --no-ff & --ff-only", param_hint="--ff-only"
        )

    # accept ref-ish things (refspec, branch, commit)
    theirs = CommitWithReference.resolve(repo, commit)
    ours = CommitWithReference.resolve(repo, "HEAD")
    ancestor_id = repo.merge_base(theirs.id, ours.id)

    if not ancestor_id:
        raise InvalidOperation(f"Commits {theirs.id} and {ours.id} aren't related.")

    ancestor = CommitWithReference.resolve(repo, ancestor_id)
    commit_with_ref3 = AncestorOursTheirs(ancestor, ours, theirs)
    merge_context = MergeContext.from_commit_with_refs(commit_with_ref3, repo)
    merge_message = merge_context.get_message()

    merge_jdict = {
        "commit": ours.id.hex,
        "branch": ours.branch_shorthand,
        "merging": merge_context.as_json(),
        "message": merge_message,
        "conflicts": None,
    }

    # We're up-to-date if we're trying to merge our own common ancestor.
    if ancestor_id == theirs.id:
        merge_jdict["noOp"] = True
        return merge_jdict

    # "dryRun": True means we didn't actually do this
    # "dryRun": False means we *did* actually do this
    merge_jdict["dryRun"] = dry_run

    # We're fastforwardable if we're our own common ancestor.
    can_ff = ancestor_id == ours.id

    if ff_only and not can_ff:
        raise InvalidOperation(
            "Can't resolve as a fast-forward merge and --ff-only specified"
        )

    if can_ff and ff:
        # do fast-forward merge
        L.debug(f"Fast forward: {theirs.id.hex}")
        merge_jdict["commit"] = theirs.id.hex
        merge_jdict["fastForward"] = True
        if not dry_run:
            repo.head.set_target(theirs.id, f"{merge_message}: Fast-forward")
        return merge_jdict

    tree3 = commit_with_ref3.map(lambda c: c.tree)
    index = repo.merge_trees(**tree3.as_dict())

    if index.conflicts:
        merge_index = MergeIndex.from_pygit2_index(index)

        merge_jdict["conflicts"] = list_conflicts(
            merge_index, merge_context, "json", summarise=2
        )
        merge_jdict["state"] = "merging"
        if not dry_run:
            move_repo_to_merging_state(
                repo, merge_index, merge_context, merge_message,
            )
        return merge_jdict

    if dry_run:
        merge_jdict["commit"] = "(dryRun)"
        return merge_jdict

    merge_tree_id = index.write_tree(repo)
    L.debug(f"Merge tree: {merge_tree_id}")

    user = repo.default_signature
    # TODO - let user edit merge message.
    merge_commit_id = repo.create_commit(
        repo.head.name, user, user, merge_message, merge_tree_id, [ours.id, theirs.id],
    )

    L.debug(f"Merge commit: {merge_commit_id}")
    merge_jdict["commit"] = merge_commit_id.hex

    return merge_jdict


def move_repo_to_merging_state(repo, merge_index, merge_context, merge_message):
    """
    Move the sno repository into a "merging" state in which conflicts
    can be resolved one by one.
    repo - the pygit2.Repository.
    merge_index - the MergeIndex containing the conflicts found.
    merge_context - the MergeContext object for the merge.
    merge_message - the commit message for when the merge is completed.
    """
    assert RepoState.get_state(repo) != RepoState.MERGING
    merge_index.write_to_repo(repo)
    merge_context.write_to_repo(repo)
    write_repo_file(repo, MERGE_MSG, merge_message)
    assert RepoState.get_state(repo) == RepoState.MERGING


def abort_merging_state(ctx):
    """
    Put things back how they were before the merge began.
    Tries to be robust against failure, in case the user has messed up the repo's state.
    """
    repo = ctx.obj.get_repo(allowed_states=RepoState.ALL_STATES)
    is_ongoing_merge = repo_file_exists(repo, MERGE_HEAD)
    # If we are in a merge, we now need to delete all the MERGE_* files.
    # If we are not in a merge, we should clean them up anyway.
    remove_all_merge_repo_files(repo)
    assert RepoState.get_state(repo) != RepoState.MERGING

    if not is_ongoing_merge:
        message = RepoState.bad_state_message(
            RepoState.NORMAL, [RepoState.MERGING], command_extra="--abort"
        )
        raise InvalidOperation(message)


def complete_merging_state(ctx):
    """
    Completes a merge that had conflicts - commits the result of the merge, and
    moves the repo from merging state back into the normal state, with the branch
    HEAD now at the merge commit. Only works if all conflicts have been resolved.
    """
    repo = ctx.obj.get_repo(
        allowed_states=[RepoState.MERGING], command_extra="--continue",
    )
    merge_index = MergeIndex.read_from_repo(repo)
    if merge_index.unresolved_conflicts:
        raise InvalidOperation(
            "Merge cannot be completed until all conflicts are resolved - see `sno conflicts`."
        )

    merge_context = MergeContext.read_from_repo(repo)
    commit_ids = merge_context.versions.map(lambda v: v.repo_structure.id)

    # TODO - let user edit merge message.
    merge_message = read_repo_file(repo, MERGE_MSG, missing_ok=True)
    if not merge_message:
        merge_message = merge_context.get_message()

    merge_tree_id = merge_index.write_resolved_tree(repo)
    L.debug(f"Merge tree: {merge_tree_id}")

    user = repo.default_signature
    merge_commit_id = repo.create_commit(
        repo.head.name,
        user,
        user,
        merge_message,
        merge_tree_id,
        [commit_ids.ours, commit_ids.theirs],
    )

    L.debug(f"Merge commit: {merge_commit_id}")

    head = CommitWithReference.resolve(repo, "HEAD")
    merge_jdict = {
        "branch": head.branch_shorthand,
        "commit": merge_commit_id,
        "merging": merge_context.as_json(),
        "message": merge_message,
    }

    repo_structure = RepositoryStructure(repo)
    wc = repo_structure.working_copy
    if wc:
        L.debug(f"Updating {wc.path} ...")
        merge_commit = repo[merge_commit_id]
        wc.reset(merge_commit, repo_structure)

    # TODO - support json output
    output_merge_json_as_text(merge_jdict)


def output_merge_json_as_text(jdict):
    theirs = jdict["merging"]["theirs"]
    ours = jdict["merging"]["ours"]
    theirs_branch = theirs.get("branch", None)
    theirs_desc = (
        f'branch "{theirs_branch}"' if theirs_branch else theirs["abbrevCommit"]
    )
    ours_desc = ours.get("branch", None) or ours["abbrevCommit"]
    click.echo(f"Merging {theirs_desc} into {ours_desc}")

    if jdict.get("noOp", False):
        click.echo("Already up to date")
        return

    dry_run = jdict.get("dryRun", False)
    commit = jdict.get("commit", None)

    if jdict.get("fastForward", False):
        if dry_run:
            click.echo(
                f"Can fast-forward to {commit}\n"
                "(Not actually fast-forwarding due to --dry-run)",
            )
        else:
            click.echo(f"Fast-forwarded to {commit}")
        return

    conflicts = jdict.get("conflicts", None)
    if not conflicts:
        if dry_run:
            click.echo(
                "No conflicts: merge will succeed!\n"
                "(Not actually merging due to --dry-run)"
            )
        else:
            click.echo(f"No conflicts!\nMerge commited as {commit}")
        return

    click.echo("Conflicts found:\n")
    click.echo(conflicts_json_as_text(conflicts))

    if dry_run:
        click.echo("(Not actually merging due to --dry-run)")
    else:
        # TODO: explain how to resolve conflicts, when this is possible
        click.echo('Repository is now in "merging" state.')
        click.echo(
            "View conflicts with `sno conflicts` and resolve them with `sno resolve`."
        )
        click.echo(
            "Once no conflicts remain, complete this merge with `sno merge --continue`."
        )
        click.echo("Or use `sno merge --abort` to return to the previous state.")


@click.command()
@click.option(
    "--ff/--no-ff",
    default=True,
    help=(
        "When the merge resolves as a fast-forward, only update the branch pointer, without creating a merge commit. "
        "With --no-ff create a merge commit even when the merge resolves as a fast-forward."
    ),
)
@click.option(
    "--ff-only",
    default=False,
    is_flag=True,
    help=(
        "Refuse to merge and exit with a non-zero status unless the current HEAD is already up to date "
        "or the merge can be resolved as a fast-forward."
    ),
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Don't perform a merge - just show what would be done",
)
@call_and_exit_flag(
    '--abort',
    callback=abort_merging_state,
    help="Abandon an ongoing merge, revert repository to the state before the merge began",
)
@call_and_exit_flag(
    '--continue',
    callback=complete_merging_state,
    help="Completes and commits a merge once all conflicts are resolved and leaves the merging state",
)
@click.argument("commit", required=True, metavar="COMMIT")
@do_json_option
@click.pass_context
def merge(ctx, ff, ff_only, dry_run, do_json, commit):
    """ Incorporates changes from the named commits (usually other branch heads) into the current branch. """

    repo = ctx.obj.get_repo(
        allowed_states=[RepoState.NORMAL],
        bad_state_message="A merge is already ongoing - see `sno merge --abort` or `sno merge --continue`",
    )

    merge_jdict = do_merge(repo, ff, ff_only, dry_run, commit)
    no_op = merge_jdict.get("noOp", False) or merge_jdict.get("dryRun", False)
    conflicts = merge_jdict.get("conflicts", None)

    if not no_op and not conflicts:
        # Update working copy.
        # TODO - maybe lock the working copy during a merge?
        repo_structure = RepositoryStructure(repo)
        wc = repo_structure.working_copy
        if wc:
            L.debug(f"Updating {wc.path} ...")
            merge_commit = repo[merge_jdict["commit"]]
            wc.reset(merge_commit, repo_structure)

    if do_json:
        jdict = {"sno.merge/v1": merge_jdict}
        dump_json_output(jdict, sys.stdout)
    else:
        output_merge_json_as_text(merge_jdict)
