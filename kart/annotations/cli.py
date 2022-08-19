import sys

import click
import pygit2

from kart.diff_estimation import estimate_diff_feature_counts
from kart.exceptions import InvalidOperation
from kart.cli_util import KartCommand

from .db import annotations_session, is_db_writable

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


@click.command(cls=KartCommand, name="build-annotations")
@click.pass_context
@click.option(
    "--all-reachable",
    is_flag=True,
    help="Build annotations for reachable commits on all refs",
)
def build_annotations(ctx, all_reachable):
    """
    Builds annotations against commits; stores the annotations in a sqlite database.

    If --all-reachable is not specified, commits hashes or refs should be supplied on stdin.
    """
    repo = ctx.obj.repo
    if all_reachable:
        click.echo("Enumerating reachable commits...")
        commits = list(gen_reachable_commits(repo))
    else:
        if sys.stdin.isatty():
            # don't just hang silently if a user typed this in an interactive shell without piping stdin
            click.echo("Reading commit hashes from stdin...")
        commits = list(
            repo.revparse_single(line.strip()).peel(pygit2.Commit)
            for line in sys.stdin
            if line.strip()
        )
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
                    repo,
                    commit.parents[0] if commit.parents else repo.empty_tree,
                    commit,
                    accuracy="exact",
                )
    click.echo("done.")
