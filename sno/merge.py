import logging
import sys

import click

from .cli_util import do_json_option
from .conflicts import (
    ConflictIndex,
    move_repo_to_merging_state,
    summarise_conflicts_json,
    output_json_conflicts_as_text,
)
from .exceptions import InvalidOperation, NotYetImplemented
from .output_util import dump_json_output
from .structs import CommitWithReference
from .structure import RepositoryStructure


L = logging.getLogger("sno.merge")


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
@click.argument("commit", required=True, metavar="COMMIT")
@do_json_option
@click.pass_context
def merge(ctx, ff, ff_only, dry_run, do_json, commit):
    """ Incorporates changes from the named commits (usually other branch heads) into the current branch. """
    repo = ctx.obj.repo
    merge_jdict = do_merge(repo, ff, ff_only, dry_run, commit)
    no_op = merge_jdict.get("noOp", False) or merge_jdict.get("dryRun", False)

    if not no_op:
        # Update working copy.
        repo_structure = RepositoryStructure(repo)
        wc = repo_structure.working_copy
        if wc:
            L.debug(f"Updating {wc.path} ...")
            merge_commit = repo[merge_jdict["mergeCommit"]]
            wc.reset(merge_commit, repo_structure)

    if do_json:
        jdict = {"sno.merge/v1": merge_jdict}
        dump_json_output(jdict, sys.stdout)
    else:
        output_merge_json_as_text(merge_jdict)


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

    merge_message = f"Merge {theirs.shorthand_with_type} into {ours.shorthand}"
    merge_jdict = {
        "ancestor": {"commit": ancestor_id.hex},
        "ours": ours.as_json(),
        "theirs": theirs.as_json(),
        "message": merge_message,
        "conflicts": None,
    }

    # We're up-to-date if we're trying to merge our own common ancestor.
    if ancestor_id == theirs.id:
        merge_jdict["mergeCommit"] = ours.id.hex
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
        merge_jdict["mergeCommit"] = theirs.id.hex
        merge_jdict["fastForward"] = True
        if not dry_run:
            repo.head.set_target(theirs.id, f"{merge_message}: Fast-forward")
        return merge_jdict

    c_ancestor = repo[ancestor_id]
    merge_index = repo.merge_trees(
        ancestor=c_ancestor.tree, ours=ours.tree, theirs=theirs.tree
    )

    if merge_index.conflicts:
        conflict_index = ConflictIndex(merge_index)
        merge_jdict["conflicts"] = summarise_conflicts_json(repo, conflict_index)
        if not dry_run:
            move_repo_to_merging_state(
                repo, conflict_index, ancestor=c_ancestor, ours=ours, theirs=theirs
            )
        return merge_jdict

    if dry_run:
        merge_jdict["mergeCommit"] = "(dryRun)"
        return merge_jdict

    merge_tree_id = merge_index.write_tree(repo)
    L.debug(f"Merge tree: {merge_tree_id}")

    user = repo.default_signature
    merge_commit_id = repo.create_commit(
        repo.head.name, user, user, merge_message, merge_tree_id, [ours.id, theirs.id],
    )

    L.debug(f"Merge commit: {merge_commit_id}")
    merge_jdict["mergeCommit"] = merge_commit_id.hex

    return merge_jdict


def output_merge_json_as_text(jdict):
    click.echo(jdict["message"].replace("Merge", "Merging", 1))

    if jdict.get("noOp", False):
        click.echo("Already up to date")
        return

    dry_run = jdict.get("dryRun", False)
    merge_commit = jdict.get("mergeCommit", None)

    if jdict.get("fastForward", False):
        if dry_run:
            click.echo(
                f"Can fast-forward to {merge_commit}\n"
                "(Not actually fast-forwarding due to --dry-run)",
            )
        else:
            click.echo(f"Fast-forwarded to {merge_commit}")
        return

    conflicts = jdict.get("conflicts", None)
    if not conflicts:
        if dry_run:
            click.echo(
                "No conflicts: merge will succeed!\n"
                "(Not actually merging due to --dry-run)"
            )
        else:
            click.echo(f"No conflicts!\nMerge commited as {merge_commit}")
        return

    click.echo("Conflicts found:\n")
    output_json_conflicts_as_text(conflicts)

    if dry_run:
        click.echo("(Not actually merging due to --dry-run)")
    else:
        # TODO: explain how to resolve conflicts, when this is possible
        raise NotYetImplemented("Sorry, resolving conflicts is not yet supported")
