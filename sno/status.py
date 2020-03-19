import click
import pygit2

from .cli_util import MutexOption
from .structure import RepositoryStructure
from .output_util import merge_outputs, print_output


JSON_DEFAULT_ATTRS = {
    "commit": None,
    "branch": None,
    "upstream": None,
    "workingCopy": None,
}


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
    rs = RepositoryStructure(repo)

    if repo.is_empty:
        if is_output_json:
            output = JSON_DEFAULT_ATTRS
        else:
            output = 'Empty repository.\n  (use "sno import" to add some data)'
    else:
        output = merge_outputs(
            [
                get_branch_status(repo, is_output_json),
                get_working_copy_status(rs, is_output_json)
            ],
            is_output_json,
            json_default_attrs=JSON_DEFAULT_ATTRS,
            text_join_str="\n")

    print_output(output, is_output_json, json_version_tag="sno.status/v1")


def get_branch_status_message(repo):
    return "\n".join(get_branch_status(repo))


def get_branch_status(repo, is_output_json):
    commit = repo.head.peel(pygit2.Commit)

    if repo.head_is_detached:
        if is_output_json:
            return {"commit": commit.short_id}
        else:
            return f"{click.style('HEAD detached at', fg='red')} {commit.short_id}\n"

    branch = repo.branches[repo.head.shorthand]

    if is_output_json:
        return {
            "commit": commit.short_id,
            "branch": branch.shorthand,
            **get_upstream_status(repo, commit, branch, is_output_json)
        }
    else:
        return (
            f"On branch {branch.shorthand}\n" +
            get_upstream_status(repo, commit, branch, is_output_json)
        )


def get_upstream_status(repo, commit, branch, is_output_json):
    upstream = branch.upstream
    if not upstream:
        if is_output_json:
            return {"upstream": None}
        else:
            return ""

    upstream_head = upstream.peel(pygit2.Commit)
    n_ahead, n_behind = repo.ahead_behind(commit.id, upstream_head.id)

    if is_output_json:
        return {"upstream": upstream.shorthand, "ahead": n_ahead, "behind": n_behind}

    if n_ahead == n_behind == 0:
        return [f"Your branch is up to date with '{upstream.shorthand}'."]
    elif n_ahead > 0 and n_behind > 0:
        return [
            f"Your branch and '{upstream.shorthand}' have diverged,",
            f"and have {n_ahead} and {n_behind} different commits each, respectively.",
            '  (use "sno pull" to merge the remote branch into yours)',
        ]
    elif n_ahead > 0:
        return [
            f"Your branch is ahead of '{upstream.shorthand}' by {n_ahead} {_pc(n_ahead)}.",
            '  (use "sno push" to publish your local commits)',
        ]
    elif n_behind > 0:
        return [
            f"Your branch is behind '{upstream.shorthand}' by {n_behind} {_pc(n_behind)}, "
            "and can be fast-forwarded.",
            '  (use "sno pull" to update your local branch)',
        ]


def get_working_copy_status(rs, is_output_json):
    working_copy = rs.working_copy
    if not rs.working_copy:
        if is_output_json:
            return {}
        else:
            return 'No working copy\n  (use "sno checkout" to create a working copy)\n'


    wc_changes = {}
    for dataset in rs:
        status = working_copy.status(dataset)
        if any(status.values()):
            wc_changes[dataset.path] = status

    if not wc_changes and not is_output_json:
        return "Nothing to commit, working copy clean\n"

    if is_output_json:
        return {"workingCopy": get_diff_status_json(wc_changes)}
    else:
        return ("Changes in working copy:\n"
                '  (use "sno commit" to commit)\n'
                '  (use "sno reset" to discard changes)\n\n'
                + get_diff_status_message(wc_changes))


def get_diff_status_json(wc_changes):
    if not wc_changes:
        return {}
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


def get_diff_status_message(wc_changes):
    message = []
    for dataset_path, status in wc_changes.items():
        if sum(status.values()):
            message.append(f"  {dataset_path}/")
            if status["META"]:
                message.append(f"    meta")
            if status["U"]:
                message.append(f"    modified:  {status['U']} {_pf(status['U'])}")
            if status["I"]:
                message.append(f"    new:       {status['I']} {_pf(status['I'])}")
            if status["D"]:
                message.append(f"    deleted:   {status['D']} {_pf(status['D'])}")
    return "\n".join(message)


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
