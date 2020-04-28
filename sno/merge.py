import logging

import click

from .conflicts import resolve_merge_conflicts
from .exceptions import InvalidOperation
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
@click.pass_context
def merge(ctx, ff, ff_only, dry_run, commit):
    """ Incorporates changes from the named commits (usually other branch heads) into the current branch. """
    repo = ctx.obj.repo

    if ff_only and not ff:
        raise click.BadParameter(
            "Conflicting parameters: --no-ff & --ff-only", param_hint="--ff-only"
        )

    # accept ref-ish things (refspec, branch, commit)
    theirs = CommitWithReference.resolve_refish(repo, commit)
    ours = CommitWithReference.resolve_refish(repo, "HEAD")

    click.echo(f"Merging {theirs} to {ours} ...")
    ancestor_id = repo.merge_base(theirs.commit.id, ours.commit.id)
    click.echo(f"Found common ancestor: {ancestor_id}")

    if not ancestor_id:
        raise InvalidOperation(
            f"Commits {theirs.commit.id} and {ours.commit.id} aren't related."
        )

    # We're up-to-date if we're trying to merge our own common ancestor.
    if ancestor_id == theirs.commit.id:
        click.echo("Already merged!")
        return

    # We're fastforwardable if we're our own common ancestor.
    can_ff = ancestor_id == ours.commit.id

    if ff_only and not can_ff:
        raise InvalidOperation(
            "Can't resolve as a fast-forward merge and --ff-only specified"
        )

    if can_ff and ff:
        # do fast-forward merge
        click.echo(f"Can fast-forward to {theirs.commit.id}")
        if not dry_run:
            repo.head.set_target(theirs.commit.id, "merge: Fast-forward")
        commit_id = theirs.commit.id
    else:
        c_ancestor = repo[ancestor_id]
        merge_index = repo.merge_trees(
            ancestor=c_ancestor.tree, ours=ours.commit.tree, theirs=theirs.commit.tree
        )
        if merge_index.conflicts:
            commit_id = resolve_merge_conflicts(
                repo,
                merge_index,
                ancestor=c_ancestor,
                ours=ours,
                theirs=theirs,
                dry_run=dry_run,
            )
        else:
            click.echo("No conflicts!")
            merge_tree_id = merge_index.write_tree(repo)
            L.debug(f"Merge tree: {merge_tree_id}")

            user = repo.default_signature
            merge_message = f"Merge '{theirs.shorthand}'"
            if not dry_run:
                commit_id = repo.create_commit(
                    repo.head.name,
                    user,
                    user,
                    merge_message,
                    merge_tree_id,
                    [ours.id, theirs.id],
                )
                click.echo(f"Merge committed as: {commit_id}")

    if not dry_run:
        # update our working copy
        repo_structure = RepositoryStructure(repo)
        wc = repo_structure.working_copy
        L.info(f"Updating {wc.path} ...")
        commit = repo[commit_id]
        wc.reset(commit, repo_structure)
