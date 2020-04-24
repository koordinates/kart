import click

from .conflicts import resolve_merge_conflicts, CommitWithReference
from .exceptions import InvalidOperation
from .structure import RepositoryStructure


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
    c_theirs, r_theirs = repo.resolve_refish(commit)
    c_ours, r_ours = repo.resolve_refish("HEAD")

    click.echo(f"Merging {c_theirs.id} to {c_ours.id} ...")
    ancestor_id = repo.merge_base(c_theirs.id, c_ours.id)
    click.echo(f"Found common ancestor: {ancestor_id}")

    if not ancestor_id:
        raise InvalidOperation(f"Commits {c_theirs.id} and {c_ours.id} aren't related.")

    # We're up-to-date if we're trying to merge our own common ancestor.
    if ancestor_id == c_theirs.id:
        click.echo("Already merged!")
        return

    # We're fastforwardable if we're our own common ancestor.
    can_ff = ancestor_id == c_ours.id

    if ff_only and not can_ff:
        raise InvalidOperation(
            "Can't resolve as a fast-forward merge and --ff-only specified"
        )

    if can_ff and ff:
        # do fast-forward merge
        click.echo(f"Can fast-forward to {c_theirs.id}")
        if not dry_run:
            repo.head.set_target(c_theirs.id, "merge: Fast-forward")
        commit_id = c_theirs.id
    else:
        c_ancestor = repo[ancestor_id]
        merge_index = repo.merge_trees(
            ancestor=c_ancestor.tree, ours=c_ours.tree, theirs=c_theirs.tree
        )
        if merge_index.conflicts:
            commit_id = resolve_merge_conflicts(
                repo,
                merge_index,
                ancestor=c_ancestor,
                ours=CommitWithReference(c_ours, r_ours),
                theirs=CommitWithReference(c_theirs, r_theirs),
                dry_run=dry_run,
            )
        else:
            click.echo("No conflicts!")
            merge_tree_id = merge_index.write_tree(repo)
            click.echo(f"Merge tree: {merge_tree_id}")

            user = repo.default_signature
            merge_message = "Merge '{}'".format(
                r_theirs.shorthand if r_theirs else c_theirs.id
            )
            if not dry_run:
                commit_id = repo.create_commit(
                    repo.head.name,
                    user,
                    user,
                    merge_message,
                    merge_tree_id,
                    [c_ours.id, c_theirs.id],
                )
                click.echo(f"Merge commit: {commit_id}")

    if not dry_run:
        # update our working copy
        repo_structure = RepositoryStructure(repo)
        wc = repo_structure.working_copy
        click.echo(f"Updating {wc.path} ...")
        commit = repo[commit_id]
        wc.reset(commit, repo_structure)
