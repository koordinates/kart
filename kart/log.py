from datetime import datetime, timezone, timedelta
import subprocess
import sys
import warnings

import click

from .cli_util import tool_environment
from .exec import execvp
from .exceptions import SubprocessError
from .output_util import dump_json_output
from .repo import KartRepoState
from .timestamps import datetime_to_iso8601_utc, timedelta_to_iso8601_tz
from . import diff_estimation


class PreserveDoubleDash(click.Command):
    """
    Preserves the double-dash ("--") arg from user input.

    Click normally swallows this arg, but using this command class preserves it.
    """

    def parse_args(self, ctx, args):
        from kart.cli import get_version_tuple

        args = list(args)
        for i in range(len(args)):
            arg = args[i]
            if arg == "--":
                if "--" in args[i + 1 :] and get_version_tuple() <= ("0", "12"):
                    # Before we added this shim, we had users using a workaround (adding the `--` twice themselves),
                    # which ideally we'd like them to stop doing.
                    warnings.warn(
                        "Using '--' twice is no longer needed, and will behave differently or fail in Kart 0.12",
                        UserWarning,
                    )
                else:
                    # Insert a second `--` arg.
                    # One of the `--` gets consumed by Click during super() below.
                    # Then the second one gets left alone and we can pass it to git.
                    args.insert(i + 1, "--")
                break

        return super(PreserveDoubleDash, self).parse_args(ctx, args)


@click.command(
    context_settings=dict(
        ignore_unknown_options=True,
    ),
    cls=PreserveDoubleDash,
)
@click.pass_context
@click.option(
    "--output-format",
    "-o",
    type=click.Choice(["text", "json", "json-lines"]),
    default="text",
)
@click.option(
    "--json-style",
    type=click.Choice(["extracompact", "compact", "pretty"]),
    default="pretty",
    help="How to format the output. Only used with --output-format=json",
)
@click.option(
    "--dataset-changes",
    "do_dataset_changes",
    is_flag=True,
    help="Shows which datasets were changed at each commit. Only works with --output-format-json",
    hidden=True,
)
@click.option(
    "--with-feature-count",
    default=None,
    type=click.Choice(diff_estimation.ACCURACY_CHOICES),
    help=(
        "Adds a 'feature_count' (the number of features modified in this diff) to JSON output."
        "If the value is 'exact', the feature count is exact (this may be slow.) "
        "Otherwise, the feature count will be approximated with varying levels of accuracy."
    ),
)
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def log(ctx, output_format, json_style, do_dataset_changes, with_feature_count, args):
    """ Show commit logs """
    if output_format == "text":
        execvp("git", ["git", "-C", ctx.obj.repo.path, "log"] + list(args))

    elif output_format in ("json", "json-lines"):
        repo = ctx.obj.get_repo(allowed_states=KartRepoState.ALL_STATES)
        try:
            cmd = [
                "git",
                "-C",
                repo.path,
                "log",
                "--pretty=format:%H,%D",
            ] + list(args)
            r = subprocess.run(
                cmd,
                encoding="utf8",
                check=True,
                capture_output=True,
                env=tool_environment(),
            )
        except subprocess.CalledProcessError as e:
            raise SubprocessError(
                f"There was a problem with git log: {e}", called_process_error=e
            )

        commit_ids_and_refs_log = _parse_git_log_output(r.stdout.splitlines())
        dataset_change_cache = {}

        commit_log = (
            commit_obj_to_json(
                repo[commit_id],
                repo,
                refs,
                do_dataset_changes,
                dataset_change_cache,
                with_feature_count,
            )
            for (commit_id, refs) in commit_ids_and_refs_log
        )
        if output_format == "json-lines":
            for item in commit_log:
                # hardcoded style here; each item must be on one line.
                dump_json_output(item, sys.stdout, "compact")

        else:
            dump_json_output(commit_log, sys.stdout, json_style)


def _parse_git_log_output(lines):
    for line in lines:
        commit_id, *refs = line.split(",")
        if not any(refs):
            refs = []
        yield commit_id, [r.strip() for r in refs]


def commit_obj_to_json(
    commit,
    repo=None,
    refs=None,
    do_dataset_changes=False,
    dataset_change_cache={},
    with_feature_count=None,
):
    """Given a commit object, returns a dict ready for dumping as JSON."""
    author = commit.author
    committer = commit.committer
    author_time = datetime.fromtimestamp(author.time, timezone.utc)
    author_time_offset = timedelta(minutes=author.offset)
    commit_time = datetime.fromtimestamp(commit.commit_time, timezone.utc)
    commit_time_offset = timedelta(minutes=commit.commit_time_offset)

    try:
        abbrev_parents = [p.short_id for p in commit.parents]
    except KeyError:
        # This happens for shallow clones where parent commits may not exist.
        # There's no way to get valid short IDs in this situation, so we just
        # fallback to full IDs
        abbrev_parents = [oid.hex for oid in commit.parent_ids]
    result = {
        "commit": commit.id.hex,
        "abbrevCommit": commit.short_id,
        "message": commit.message,
        "authorName": author.name,
        "authorEmail": author.email,
        "authorTime": datetime_to_iso8601_utc(author_time),
        "authorTimeOffset": timedelta_to_iso8601_tz(author_time_offset),
        "committerEmail": committer.email,
        "committerName": committer.name,
        "commitTime": datetime_to_iso8601_utc(commit_time),
        "commitTimeOffset": timedelta_to_iso8601_tz(commit_time_offset),
        "parents": [oid.hex for oid in commit.parent_ids],
        "abbrevParents": abbrev_parents,
    }
    if refs is not None:
        result["refs"] = refs
    if do_dataset_changes:
        result["datasetChanges"] = get_dataset_changes(
            repo, commit, dataset_change_cache
        )
    if with_feature_count:
        if (not do_dataset_changes) or result["datasetChanges"]:
            try:
                parent_commit = commit.parents[0]
            except (KeyError, IndexError):
                # shallow clone (parent not present) or initial commit (no parents)
                base = repo.empty_tree
            else:
                base = parent_commit

            result["featureChanges"] = diff_estimation.estimate_diff_feature_counts(
                repo,
                base=base,
                target=commit,
                accuracy=with_feature_count,
            )
        else:
            result["featureChanges"] = {}
    return result


def get_dataset_changes(repo, commit, dataset_change_cache):
    """Given a commit, returns a list of datasets changed by that commit."""
    cur_datasets = _get_dataset_tree_ids(repo, commit, dataset_change_cache)
    prev_datasets = None
    try:
        if not commit.parents:
            return sorted(list(cur_datasets.keys()))

        parent = commit.parents[0]
        prev_datasets = _get_dataset_tree_ids(repo, parent, dataset_change_cache)
        changes = prev_datasets.items() ^ cur_datasets.items()
        return sorted(list(set(ds for ds, tree in changes)))

    except KeyError:
        return sorted(list(cur_datasets.keys()))


def _get_dataset_tree_ids(repo, commit, dataset_change_cache):
    """
    Given a commit, returns a dict of dataset SHAs at that commit eg:
    {
        "nz_building_outlines": "8f7dbff287b9d40a772a1315c47e208124028645",
        ...
    }
    """
    commit_id = commit.id.hex
    if commit_id not in dataset_change_cache:
        result = {}
        for dataset in repo.datasets(commit):
            result[dataset.path] = dataset.tree.id.hex
        dataset_change_cache[commit_id] = result

    return dataset_change_cache[commit_id]
