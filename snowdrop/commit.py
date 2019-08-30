import json
import uuid

import click
import pygit2

from . import core, diff, gpkg
from .working_copy import WorkingCopy
from .structure import RepositoryStructure


@click.command()
@click.pass_context
@click.option("--message", "-m", help="Use the given message as the commit message.")
def commit(ctx, message):
    """ Record changes to the repository """
    repo_dir = ctx.obj["repo_dir"]
    repo = pygit2.Repository(repo_dir)
    if not repo:
        raise click.BadParameter("Not an existing repository", param_hint="--repo")

    commit = repo.head.peel(pygit2.Commit)
    tree = commit.tree

    working_copy = WorkingCopy.open(repo)
    if not working_copy:
        raise click.UsageError("No working copy, use 'checkout'")

    working_copy.assert_db_tree_match(tree)

    rs = RepositoryStructure(repo)
    for i, dataset in enumerate(rs):
        if i > 0:
            raise NotImplementedError("multi-dataset repositories")

        wcdiff = working_copy.diff_db_to_tree(dataset)
        if not any(wcdiff.values()):
            raise click.ClickException("No changes to commit")

    new_commit = working_copy.commit(tree, wcdiff, message)
