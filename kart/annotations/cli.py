import click
import pygit2

from .db import annotations_session, is_db_writable

from kart.diff_estimation import estimate_diff_feature_counts
from kart.exceptions import InvalidOperation

EMPTY_TREE_SHA = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"


def gen_reachable_commits(repo):
    """
    Generator. Yields commits that are reachable from any ref.
    """
    walker = repo.walk(repo.head.target)
    refs = repo.references
    for ref in refs:
        walker.push(refs[ref].target)
    yield from walker


@click.command(name="build-annotations")
@click.pass_context
@click.option(
    "--all-reachable", is_flag=True, help="Build annotations for all reachable commits"
)
@click.argument(
    "refishes",
    nargs=-1,
)
def build_annotations(ctx, refishes, all_reachable):
    """
    Builds annotations against commits; stores the annotations in a sqlite database.
    """
    repo = ctx.obj.repo
    if all_reachable:
        if refishes:
            raise click.UsageError(
                "--all-reachable and refishes are mutually exclusive"
            )
        click.echo("Enumerating reachable commits...")
        commits = list(gen_reachable_commits(repo))
    else:
        if not refishes:
            refishes = ["HEAD"]
        commits = [repo.revparse_single(r).peel(pygit2.Commit) for r in refishes]
    if commits:
        with annotations_session(repo) as session:
            if not is_db_writable(session):
                # not much point in this command if it can't write to the db
                raise InvalidOperation(
                    "Annotations database is readonly; can't continue"
                )
            click.echo("Building feature change counts...")
            for i, commit in enumerate(commits):
                click.echo(
                    f"({i+1}/{len(commits)}): {commit.short_id} {commit.message.splitlines()[0]}"
                )
                estimate_diff_feature_counts(
                    repo.structure(
                        commit.parent_ids[0] if commit.parent_ids else EMPTY_TREE_SHA
                    ),
                    repo.structure(commit),
                    accuracy="exact",
                )
    click.echo("done.")
