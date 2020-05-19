import logging
import sys

import click

from .cli_util import do_json_option
from .conflicts import (
    ConflictIndex,
    abort_merging_state,
    move_repo_to_merging_state,
    list_conflicts,
    output_conflicts_as_text,
)
from .exceptions import InvalidOperation
from .output_util import dump_json_output
from .repo_files import is_ongoing_merge
from .structs import AncestorOursTheirs, CommitWithReference
from .structure import RepositoryStructure


L = logging.getLogger("sno.merge")


def merge_abort(ctx, param, value):
    if value:
        repo = ctx.obj.repo
        abort_merging_state(repo)
        ctx.exit()


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
@click.option(
    '--abort',
    is_flag=True,
    callback=merge_abort,
    expose_value=False,
    is_eager=True,
    help="Abandon an ongoing merge, revert repository to the state before the merge began",
)
@click.argument("commit", required=True, metavar="COMMIT")
@do_json_option
@click.pass_context
def merge(ctx, ff, ff_only, dry_run, do_json, commit):
    """ Incorporates changes from the named commits (usually other branch heads) into the current branch. """

    repo = ctx.obj.repo
    if is_ongoing_merge(repo):
        raise InvalidOperation("A merge is already ongoing")

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

    ancestor = CommitWithReference.resolve(repo, ancestor_id)

    merge_message = f"Merge {theirs.shorthand_with_type} into {ours.shorthand}"
    merge_jdict = {
        "branch": ours.shorthand,
        "ancestor": ancestor.id.hex,
        "ours": ours.id.hex,
        "theirs": theirs.id.hex,
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

    commit_with_ref3 = AncestorOursTheirs(ancestor, ours, theirs)
    tree3 = commit_with_ref3.map(lambda c: c.tree)
    merge_index = repo.merge_trees(**tree3.as_dict())

    if merge_index.conflicts:
        conflict_index = ConflictIndex(merge_index)
        repo_structures3 = commit_with_ref3.map(
            lambda c: RepositoryStructure(repo, c.commit)
        )
        merge_jdict["conflicts"] = list_conflicts(conflict_index, repo_structures3)
        if not dry_run:
            move_repo_to_merging_state(
                repo,
                conflict_index,
                merge_message,
                ancestor=ancestor,
                ours=ours,
                theirs=theirs,
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
    output_conflicts_as_text(conflicts)

    if dry_run:
        click.echo("(Not actually merging due to --dry-run)")
    else:
        # TODO: explain how to resolve conflicts, when this is possible
        click.echo("Sorry, resolving merge conflicts is not yet supported", err=True)
        click.echo("Use `sno merge --abort` to abort this merge", err=True)
