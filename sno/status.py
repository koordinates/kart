import json
import sys

import click
import pygit2

from .cli_util import MutexOption
from .structure import RepositoryStructure


EMPTY_REPO_JSON = frozenset({
    "commit": None,
    "branch": None,
    "upstream": None,
    "workingCopy": None,
}.items())


@click.command()
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
def status(ctx, is_output_json):
    """ Show the working copy status """
    repo = ctx.obj.repo
    jdict = get_status_json(repo)
    if is_output_json:
        json.dump(jdict, sys.stdout, indent=2)
    else:
        click.echo(status_to_text(jdict))


def get_status_json(repo):
    output = dict(EMPTY_REPO_JSON)
    if not repo.is_empty:
        output.update(get_branch_status_json(repo))
        output.update(get_working_copy_status_json(repo))
    return {"sno.status/v1": output}


def get_branch_status_json(repo):
    commit = repo.head.peel(pygit2.Commit)
    output = {"commit": commit.short_id}

    if repo.head_is_detached:
        return output

    branch = repo.branches[repo.head.shorthand]
    output["branch"] = branch.shorthand

    upstream = branch.upstream
    if upstream:
        upstream_head = upstream.peel(pygit2.Commit)
        n_ahead, n_behind = repo.ahead_behind(commit.id, upstream_head.id)
        output["upstream"] = {
            "branch": upstream.shorthand,
            "ahead": n_ahead,
            "behind": n_behind
        }
    return output


def get_working_copy_status_json(repo):
    rs = RepositoryStructure(repo)
    working_copy = rs.working_copy
    if not rs.working_copy:
        return {}

    wc_changes = {}
    for dataset in rs:
        status = working_copy.status(dataset)
        if any(status.values()):
            wc_changes[dataset.path] = status

    if wc_changes:
        return {"workingCopy": get_diff_status_json(wc_changes)}
    else:
        return {"workingCopy": {}}


def get_diff_status_json(wc_changes):
    result = {}
    for dataset_path, status in wc_changes.items():
        if sum(status.values()):
            result[dataset_path] = {
                "schemaChanges": {} if status["META"] else None,
                "featureChanges": {
                    "modified": status["U"],
                    "new": status["I"],
                    "deleted": status["D"],
                }
            }
    return result


def status_to_text(jdict):
    branch_status = branch_status_to_text(jdict["sno.status/v1"])
    wc_status = working_copy_status_to_text(jdict["sno.status/v1"]["workingCopy"])

    is_empty = not jdict["sno.status/v1"]["commit"]
    if is_empty:
        return branch_status

    return "\n".join([branch_status, wc_status])


def branch_status_to_text(jdict):
    commit = jdict["commit"]
    if not commit:
        return 'Empty repository.\n  (use "sno import" to add some data)'
    branch = jdict["branch"]
    if not branch:
        return f"{click.style('HEAD detached at', fg='red')} {commit}\n"
    output = f"On branch {branch}\n"

    upstream = jdict["upstream"]
    if upstream:
        output += upstream_status_to_text(upstream)
    return output


def upstream_status_to_text(jdict):
    upstream_branch = jdict["branch"]
    n_ahead = jdict["ahead"]
    n_behind = jdict["behind"]

    if n_ahead == n_behind == 0:
        return f"Your branch is up to date with '{upstream_branch}'.\n"
    elif n_ahead > 0 and n_behind > 0:
        return (
            f"Your branch and '{upstream_branch}' have diverged,\n"
            f"and have {n_ahead} and {n_behind} different commits each, respectively.\n"
            '  (use "sno pull" to merge the remote branch into yours)\n'
        )
    elif n_ahead > 0:
        return (
            f"Your branch is ahead of '{upstream_branch}' by {n_ahead} {_pc(n_ahead)}.\n"
            '  (use "sno push" to publish your local commits)\n'
        )
    elif n_behind > 0:
        return (
            f"Your branch is behind '{upstream_branch}' by {n_behind} {_pc(n_behind)}, "
            "and can be fast-forwarded.\n"
            '  (use "sno pull" to update your local branch)\n'
        )


def working_copy_status_to_text(jdict):
    if jdict is None:
        return 'No working copy\n  (use "sno checkout" to create a working copy)\n'

    if not jdict:
        return "Nothing to commit, working copy clean"

    return ("Changes in working copy:\n"
            '  (use "sno commit" to commit)\n'
            '  (use "sno reset" to discard changes)\n\n'
            + diff_status_to_text(jdict))


def diff_status_to_text(jdict):
    message = []
    for dataset_path, all_changes in jdict.items():
        message.append(f"  {dataset_path}/")
        if all_changes["schemaChanges"] is not None:
            message.append(f"    meta")

        feature_changes = all_changes["featureChanges"]
        feature_change_message(message, feature_changes, "modified")
        feature_change_message(message, feature_changes, "new")
        feature_change_message(message, feature_changes, "deleted")
    return "\n".join(message)


def feature_change_message(message, feature_changes, key):
    n = feature_changes[key]
    label = f"    {key}:"
    col_width = 15
    if n:
        message.append(f"{label: <{col_width}}{n} {_pf(n)}")


def get_branch_status_message(repo):
    return branch_status_to_text(get_branch_status_json(repo))


def get_diff_status_message(wc_changes):
    return diff_status_to_text(get_diff_status_json(wc_changes))


def _pf(count):
    """ Simple pluraliser for feature/features """
    if count == 1:
        return "feature"
    else:
        return "features"


def _pc(count):
    """ Simple pluraliser for commit/commits """
    if count == 1:
        return "commit"
    else:
        return "commits"
