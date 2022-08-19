import logging
import sys

import click

from . import commit
from .cli_util import StringFromFile, call_and_exit_flag, KartCommand
from .conflicts_writer import BaseConflictsWriter
from .diff_util import get_repo_diff
from .exceptions import InvalidOperation
from .merge_util import (
    ALL_MERGE_FILES,
    AncestorOursTheirs,
    MergeContext,
    MergedIndex,
    WorkingCopyMerger,
    merge_status_to_text,
    write_merged_index_flags,
)
from .output_util import dump_json_output
from .pack_util import write_to_packfile
from .repo import KartRepoFiles, KartRepoState
from .structs import CommitWithReference

L = logging.getLogger("kart.merge")


def get_commit_message(
    merge_context, merge_tree_id, repo, read_msg_file=False, quiet=False
):
    merge_message = None
    if read_msg_file:
        merge_message = repo.read_gitdir_file(KartRepoFiles.MERGE_MSG, missing_ok=True)
    if not merge_message:
        merge_message = merge_context.get_message()
    head = repo.structure("HEAD")
    merged = repo.structure(merge_tree_id)
    diff = get_repo_diff(head, merged)
    merge_message = commit.get_commit_message(
        repo, diff, draft_message=merge_message, quiet=quiet
    )
    if not merge_message:
        raise click.UsageError("Aborting commit due to empty commit message.")
    return merge_message


def do_merge(repo, ff, ff_only, dry_run, commit, commit_message, quiet=False):
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
    merge_message = commit_message or merge_context.get_message()

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
    index = repo.merge_trees(**tree3.as_dict(), flags={"find_renames": False})

    if index.conflicts:
        merged_index = MergedIndex.from_pygit2_index(index)
        conflicts_writer_class = BaseConflictsWriter.get_conflicts_writer_class("json")
        conflicts_writer = conflicts_writer_class(
            repo, summarise=2, merged_index=merged_index, merge_context=merge_context
        )
        merge_jdict["conflicts"] = conflicts_writer.list_conflicts()
        merge_jdict["state"] = "merging"
        if not dry_run:
            move_repo_to_merging_state(
                repo,
                merged_index,
                merge_context,
                merge_message,
            )
        return merge_jdict

    if dry_run:
        merge_jdict["commit"] = "(dryRun)"
        return merge_jdict

    with write_to_packfile(repo):
        merge_tree_id = index.write_tree(repo, write_merged_index_flags(repo))
        L.debug(f"Merge tree: {merge_tree_id}")

        user = repo.default_signature
        if not commit_message:
            commit_message = get_commit_message(
                merge_context, merge_tree_id, repo, quiet=quiet
            )
        merge_commit_id = repo.create_commit(
            repo.head.name,
            user,
            user,
            commit_message,
            merge_tree_id,
            [ours.id, theirs.id],
        )

    L.debug(f"Merge commit: {merge_commit_id}")
    merge_jdict["commit"] = merge_commit_id.hex

    return merge_jdict


def move_repo_to_merging_state(
    repo,
    merged_index,
    merge_context,
    merge_message,
):
    """
    Move the Kart repository into a "merging" state in which conflicts
    can be resolved one by one.
    repo - the KartRepo
    merged_index - the MergedIndex containing the conflicts found.
    merge_context - the MergeContext object for the merge.
    merge_message - the commit message for when the merge is completed.
    """
    assert repo.state != KartRepoState.MERGING
    merged_index.write_to_repo(repo)
    merge_context.write_to_repo(repo)
    repo.write_gitdir_file(KartRepoFiles.MERGE_MSG, merge_message)

    working_copy_merger = WorkingCopyMerger(repo, merge_context)
    # The merged_tree is used mostly for updating the working copy, but is also used for
    # serialising feature resolves, so we write it even if there's no WC.
    merged_tree = working_copy_merger.write_merged_tree(merged_index)
    if repo.working_copy.exists():
        working_copy_merger.update_working_copy(merged_index, merged_tree)

    assert repo.state == KartRepoState.MERGING


def abort_merging_state(ctx):
    """
    Put things back how they were before the merge began.
    Tries to be robust against failure, in case the user has messed up the repo's state.
    """
    repo = ctx.obj.get_repo(allowed_states=KartRepoState.ALL_STATES)
    is_ongoing_merge = repo.gitdir_file(KartRepoFiles.MERGE_HEAD).exists()

    # If we are in a merge, we now need to delete all the MERGE_* files.
    # If we are not in a merge, we should clean them up anyway.
    for filename in ALL_MERGE_FILES:
        repo.remove_gitdir_file(filename)

    assert repo.state != KartRepoState.MERGING

    if not is_ongoing_merge:
        message = KartRepoState.bad_state_message(
            KartRepoState.NORMAL, [KartRepoState.MERGING], command_extra="--abort"
        )
        raise InvalidOperation(message)

    # In case the user has modified the working copy during the merging state.
    repo.working_copy.reset_to_head()


def complete_merging_state(ctx):
    """
    Completes a merge that had conflicts - commits the result of the merge, and
    moves the repo from merging state back into the normal state, with the branch
    HEAD now at the merge commit. Only works if all conflicts have been resolved.
    """
    repo = ctx.obj.get_repo(
        allowed_states=KartRepoState.MERGING,
        command_extra="--continue",
    )
    merged_index = MergedIndex.read_from_repo(repo)
    if merged_index.unresolved_conflicts:
        raise InvalidOperation(
            "Merge cannot be completed until all conflicts are resolved - see `kart conflicts`."
        )

    merge_context = MergeContext.read_from_repo(repo)
    commit_ids = merge_context.versions.map(lambda v: v.commit_id)

    with write_to_packfile(repo):
        merge_tree_id = merged_index.write_resolved_tree(repo)
        L.debug(f"Merge tree: {merge_tree_id}")

        merge_message = ctx.params.get("message")
        if not merge_message:
            merge_message = get_commit_message(
                merge_context, merge_tree_id, repo, read_msg_file=True
            )

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

    for filename in ALL_MERGE_FILES:
        repo.remove_gitdir_file(filename)

    assert repo.state != KartRepoState.MERGING

    # TODO - support json output
    click.echo(merge_status_to_text(merge_jdict, fresh=True))
    repo.gc("--auto")

    # TODO - this blows away any uncommitted WC changes the user has, but unfortunately,
    # we don't have any way of preserving them right now.
    repo.working_copy.reset_to_head()


@click.command(cls=KartCommand)
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
@click.option(
    "--output-format",
    "-o",
    type=click.Choice(["text", "json"]),
    default="text",
)
@click.option(
    "--message",
    "-m",
    type=StringFromFile(encoding="utf-8"),
    help="Use the given message as the commit message.",
    is_eager=True,  # -m is eager and --continue is non-eager so we can access -m from complete_merging_state callback.
)
@call_and_exit_flag(
    "--continue",
    callback=complete_merging_state,
    help="Completes and commits a merge once all conflicts are resolved and leaves the merging state",
    is_eager=False,
)
@call_and_exit_flag(
    "--abort",
    callback=abort_merging_state,
    help="Abandon an ongoing merge, revert repository to the state before the merge began",
)
@click.argument("commit", required=True, metavar="COMMIT")
@click.pass_context
def merge(ctx, ff, ff_only, dry_run, message, output_format, commit):
    """Incorporates changes from the named commits (usually other branch heads) into the current branch."""

    repo = ctx.obj.get_repo(
        allowed_states=KartRepoState.NORMAL,
        bad_state_message="A merge is already ongoing - see `kart merge --abort` or `kart merge --continue`",
    )
    ctx.obj.check_not_dirty()

    do_json = output_format == "json"

    jdict = do_merge(repo, ff, ff_only, dry_run, commit, message, quiet=do_json)
    no_op = jdict.get("noOp", False) or jdict.get("dryRun", False)
    conflicts = jdict.get("conflicts", None)

    if do_json:
        dump_json_output({"kart.merge/v1": jdict}, sys.stdout)
    else:
        click.echo(merge_status_to_text(jdict, fresh=True))
    if not no_op and not conflicts:
        repo.gc("--auto")
        repo.working_copy.reset_to_head(quiet=do_json)
