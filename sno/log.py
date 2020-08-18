from datetime import datetime, timezone, timedelta
import itertools
import subprocess
import sys

import click

from .exec import execvp
from .exceptions import SubprocessError
from .output_util import dump_json_output
from .timestamps import datetime_to_iso8601_utc, timedelta_to_iso8601_tz
from .structure import RepositoryStructure


@click.command(context_settings=dict(ignore_unknown_options=True,))
@click.pass_context
@click.option(
    "--output-format", "-o", type=click.Choice(["text", "json"]), default="text",
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
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def log(ctx, output_format, json_style, do_dataset_changes, args):
    """ Show commit logs """
    if output_format == "text":
        execvp("git", ["git", "-C", ctx.obj.repo.path, "log"] + list(args))

    elif output_format == "json":
        repo = ctx.obj.repo
        try:
            cmd = ["git", "-C", repo.path, "log", "--pretty=format:%H,%D",] + list(args)
            r = subprocess.run(cmd, encoding="utf8", check=True, capture_output=True)
        except subprocess.CalledProcessError as e:
            raise SubprocessError(
                f"There was a problem with git log: {e}", called_process_error=e
            )

        commit_ids_and_refs_log = _parse_git_log_output(r.stdout.splitlines())
        if do_dataset_changes:
            dataset_changes_log = get_dataset_changes_log(repo, args)
        else:
            dataset_changes_log = itertools.cycle([None])

        commit_log = [
            commit_obj_to_json(repo[commit_id], refs, dataset_changes)
            for (commit_id, refs), dataset_changes in zip(
                commit_ids_and_refs_log, dataset_changes_log
            )
        ]
        dump_json_output(commit_log, sys.stdout, json_style)


def _parse_git_log_output(lines):
    for line in lines:
        commit_id, *refs = line.split(",")
        if not any(refs):
            refs = []
        yield commit_id, refs


def commit_obj_to_json(commit, refs, dataset_changes=None):
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
        "refs": refs,
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
    if dataset_changes is not None:
        result["datasetChanges"] = dataset_changes
    return result


def get_dataset_changes_log(repo, args):
    # TODO - git log isn't really designed to efficiently tell us which datasets changed.
    # So this code is a bit more complex that would be ideal, and a bit less efficient.
    dataset_dirname = f"/{RepositoryStructure(repo).dataset_dirname}/"
    for percentage in (90, 10, 1):
        directory_changes_log = _get_directory_changes_log(repo, percentage, args)
        if all(_enough_detail(d, dataset_dirname) for d in directory_changes_log):
            break

    return [_get_datasets(d, dataset_dirname) for d in directory_changes_log]


def _enough_detail(directories, dataset_dirname):

    return all(dataset_dirname in d for d in directories)


def _get_datasets(directories, dataset_dirname):
    datasets = set()
    for d in directories:
        parts = d.split(dataset_dirname, 1)
        if len(parts) == 2:
            datasets.add(parts[0])
    return list(datasets)


_SEPARATOR = "=" * 20


def _get_directory_changes_log(repo, percentage, args):
    try:
        cmd = [
            "git",
            "-C",
            repo.path,
            "log",
            f"--pretty=format:{_SEPARATOR}",
            "--shortstat",
            f"--dirstat=files,cumulative,{percentage}",
        ] + list(args)
        r = subprocess.run(cmd, encoding="utf8", check=True, capture_output=True)
    except subprocess.CalledProcessError as e:
        raise SubprocessError(
            f"There was a problem with git log: {e}", called_process_error=e
        )
    raw_log = r.stdout.split(f"{_SEPARATOR}\n")[1:]
    return [_get_directories(r) for r in raw_log]


def _get_directories(raw_output):
    raw_output = raw_output.strip()
    if not raw_output:
        return []  # Empty change.

    directories = set()
    for line in raw_output.splitlines()[1:]:
        line = line.strip()
        if not line:
            continue
        directory = line.split("% ", 1)[1]
        directories.add(directory)

    if not directories:
        # Non-empty change, but dirstat didn't return any particular directory.
        directories.add("/")

    return directories
