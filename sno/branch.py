import json
import sys

import click
import pygit2

from .cli_util import MutexOption
from .exec import execvp


@click.command(context_settings=dict(ignore_unknown_options=True))
@click.pass_context
@click.option(
    "--text",
    "is_output_json",
    flag_value=False,
    default=True,
    help="Get the status in text format",
    cls=MutexOption,
    exclusive_with=["json"],
)
@click.option(
    "--json",
    "is_output_json",
    flag_value=True,
    help="Get the status in JSON format",
    cls=MutexOption,
    exclusive_with=["text"],
)
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def branch(ctx, is_output_json, args):
    """ List, create, or delete branches """
    repo_path = ctx.obj.repo_path
    repo = ctx.obj.repo

    sargs = set(args)
    if is_output_json:
        if sargs - {"--list"}:  # sno branch --list --json is allowed.
            raise click.UsageError(
                "Illegal usage: 'sno branch --json' only supports listing branches."
            )
        json.dump(list_branches_json(repo), sys.stdout, indent=2)
        return

    # git's branch protection behaviour doesn't apply if it's a bare repository
    # attempt to apply it here.
    if sargs & {"-d", "--delete", "-D"}:
        branch = repo.head.shorthand
        if branch in sargs:
            raise click.ClickException(
                f"Cannot delete the branch '{branch}' which you are currently on."
            )

    execvp("git", ["git", "-C", str(repo_path), "branch"] + list(args))


def list_branches_json(repo):
    output = {"current": None, "branches": {}}
    if not repo.is_empty and not repo.head_is_detached:
        output["current"] = repo.head.shorthand
    branches = {}
    for branch_name in repo.listall_branches():
        branches[branch_name] = branch_obj_to_json(repo, repo.branches[branch_name])
    output["branches"] = branches

    return {"sno.branch/v1": output}


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
