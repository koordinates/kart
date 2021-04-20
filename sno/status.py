import sys

import click
import pygit2

from .conflicts import list_conflicts
from .output_util import dump_json_output
from .merge_util import MergeContext, MergeIndex, merge_status_to_text
from .repo import SnoRepoState


@click.command()
@click.pass_context
@click.option(
    "--output-format",
    "-o",
    type=click.Choice(["text", "json"]),
    default="text",
)
def status(ctx, output_format):
    """ Show the working copy status """
    repo = ctx.obj.get_repo(allowed_states=SnoRepoState.ALL_STATES)
    jdict = get_branch_status_json(repo)

    if repo.state == SnoRepoState.MERGING:
        merge_index = MergeIndex.read_from_repo(repo)
        merge_context = MergeContext.read_from_repo(repo)
        jdict["merging"] = merge_context.as_json()
        jdict["conflicts"] = list_conflicts(
            merge_index, merge_context, output_format, summarise=2
        )
        jdict["state"] = "merging"
    else:
        jdict["workingCopy"] = get_working_copy_status_json(repo)

    if output_format == "json":
        dump_json_output({"kart.status/v1": jdict}, sys.stdout)
    else:
        click.echo(status_to_text(jdict))


def get_branch_status_json(repo):
    output = {"commit": None, "abbrevCommit": None, "branch": None, "upstream": None}

    commit = repo.head_commit
    if commit:
        output["commit"] = commit.id.hex
        output["abbrevCommit"] = commit.short_id

    output["branch"] = repo.head_branch_shorthand
    if not repo.head_is_unborn and not repo.head_is_detached:
        branch = repo.branches[repo.head_branch_shorthand]
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


def get_working_copy_status_json(repo):
    if repo.is_empty:
        return None

    working_copy = repo.working_copy
    if not working_copy:
        return None

    output = {"path": working_copy.clean_location, "changes": None}

    wc_diff = working_copy.diff_to_tree()
    if wc_diff:
        output["changes"] = get_diff_status_json(wc_diff)

    return output


def get_diff_status_json(diff):
    """Given a diff.Diff object, returns a JSON object describing the diff status."""
    return diff.type_counts()


def status_to_text(jdict):
    branch_status = branch_status_to_text(jdict)
    is_empty = not jdict["commit"]
    is_merging = jdict.get("state", None) == SnoRepoState.MERGING.value

    if is_merging:
        merge_status = merge_status_to_text(jdict, fresh=False)
        return "\n\n".join([branch_status, merge_status])

    if not is_empty:
        wc_status = working_copy_status_to_text(jdict["workingCopy"])
        return "\n\n".join([branch_status, wc_status])

    return branch_status


def branch_status_to_text(jdict):
    commit = jdict["abbrevCommit"]
    if not commit:
        return 'Empty repository.\n  (use "sno import" to add some data)'
    branch = jdict["branch"]
    if not branch:
        return f"{click.style('HEAD detached at', fg='red')} {commit}"
    output = f"On branch {branch}"

    upstream = jdict["upstream"]
    if upstream:
        output = "\n".join([output, upstream_status_to_text(upstream)])
    return output


def upstream_status_to_text(jdict):
    upstream_branch = jdict["branch"]
    n_ahead = jdict["ahead"]
    n_behind = jdict["behind"]

    if n_ahead == n_behind == 0:
        return f"Your branch is up to date with '{upstream_branch}'."
    elif n_ahead > 0 and n_behind > 0:
        return (
            f"Your branch and '{upstream_branch}' have diverged,\n"
            f"and have {n_ahead} and {n_behind} different commits each, respectively.\n"
            '  (use "sno pull" to merge the remote branch into yours)'
        )
    elif n_ahead > 0:
        return (
            f"Your branch is ahead of '{upstream_branch}' by {n_ahead} {_pc(n_ahead)}.\n"
            '  (use "sno push" to publish your local commits)'
        )
    elif n_behind > 0:
        return (
            f"Your branch is behind '{upstream_branch}' by {n_behind} {_pc(n_behind)}, "
            "and can be fast-forwarded.\n"
            '  (use "sno pull" to update your local branch)'
        )


def working_copy_status_to_text(jdict):
    if jdict is None:
        return 'No working copy\n  (use "sno checkout" to create a working copy)\n'

    if jdict["changes"] is None:
        return "Nothing to commit, working copy clean"

    return (
        "Changes in working copy:\n"
        '  (use "sno commit" to commit)\n'
        '  (use "sno reset" to discard changes)\n\n'
        + diff_status_to_text(jdict["changes"])
    )


def diff_status_to_text(jdict):
    message = []
    for dataset_path, dataset_changes in jdict.items():
        message.append(f"  {dataset_path}:")
        for dataset_part in ("meta", "feature"):
            if dataset_part not in dataset_changes:
                continue
            message.append(f"    {dataset_part}:")
            dataset_part_changes = dataset_changes[dataset_part]
            for change_type in ("inserts", "updates", "deletes"):
                if change_type not in dataset_part_changes:
                    continue
                change_type_count = dataset_part_changes[change_type]
                message.append(f"      {change_type_count} {change_type}")

    return "\n".join(message)


def feature_change_message(message, feature_changes, key):
    n = feature_changes.get(key)
    label = f"    {key}:"
    col_width = 15
    if n:
        message.append(f"{label: <{col_width}}{n} {_pf(n)}")


def get_branch_status_message(repo):
    return branch_status_to_text(get_branch_status_json(repo))


def get_diff_status_message(diff):
    """Given a diff.Diff, return a status message describing it."""
    return diff_status_to_text(get_diff_status_json(diff))


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
