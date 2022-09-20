import sys

import click
import pygit2

from .exceptions import InvalidOperation
from .exec import run_and_wait
from .output_util import dump_json_output
from kart.cli_util import KartCommand


@click.command(cls=KartCommand, context_settings=dict(ignore_unknown_options=True))
@click.pass_context
@click.option(
    "--output-format",
    "-o",
    type=click.Choice(["text", "json"]),
    default="text",
)
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def branch(ctx, output_format, args):
    """List, create, or delete branches"""
    repo = ctx.obj.repo

    sargs = set(args)
    if output_format == "json":
        valid_args = {"--list"}  # "kart branch -o json" or "kart branch --list -o json"
        invalid_args = sargs - valid_args
        if invalid_args:
            raise click.UsageError(
                "Illegal usage: 'kart branch --output-format=json' only supports listing branches."
            )
        dump_json_output(list_branches_json(repo), sys.stdout)
        return

    # git's branch protection behaviour doesn't apply if it's a bare repository
    # attempt to apply it here.
    if sargs & {"-d", "--delete", "-D"}:
        branch = repo.head.shorthand
        if branch in sargs:
            raise InvalidOperation(
                f"Cannot delete the branch '{branch}' which you are currently on."
            )

    run_and_wait("git", ["git", "-C", repo.path, "branch"] + list(args))


def list_branches_json(repo):
    output = {"current": None, "branches": {}}

    if not repo.head_is_detached:
        if not repo.head_is_unborn:
            output["current"] = repo.head.shorthand
        else:
            target = repo.references.get("HEAD").target
            if target.startswith("refs/heads/"):
                target_shorthand = target[len("refs/heads/") :]
                output["current"] = target_shorthand

    branches = {}
    for branch_name in repo.listall_branches():
        branches[branch_name] = branch_obj_to_json(repo, repo.branches[branch_name])
    output["branches"] = branches

    return {"kart.branch/v1": output}


def branch_obj_to_json(repo, branch):
    output = {"commit": None, "abbrevCommit": None, "branch": None, "upstream": None}
    output["branch"] = branch.shorthand

    commit = branch.peel(pygit2.Commit)
    output["commit"] = commit.id.hex
    output["abbrevCommit"] = commit.short_id

    upstream = branch.upstream
    if upstream:
        upstream_head = upstream.peel(pygit2.Commit)
        n_ahead, n_behind = repo.ahead_behind(commit.id, upstream_head.id)
        output["upstream"] = {
            "branch": upstream.shorthand,
            "ahead": n_ahead,
            "behind": n_behind,
        }
    return output
