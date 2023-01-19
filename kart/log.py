import subprocess
import sys
import logging
from datetime import datetime, timedelta, timezone

import click

from kart import diff_estimation
from kart.cli_util import (
    OutputFormatType,
    parse_output_format,
    tool_environment,
)
from kart.completion_shared import ref_or_repo_path_completer
from kart.exceptions import NotYetImplemented, SubprocessError
from kart.subprocess_util import run
from kart.key_filters import RepoKeyFilter
from kart.output_util import dump_json_output
from kart.parse_args import PreserveDoubleDash, parse_revisions_and_filters
from kart.repo import KartRepoState
from kart.timestamps import datetime_to_iso8601_utc, timedelta_to_iso8601_tz

L = logging.getLogger("kart.log")


def find_dataset(ds_path, repo, commits):
    """Finds a dataset by name, so long as it is found somewhere in the given commits / refs / ranges."""
    if ds_path in repo.datasets():
        return repo.datasets()[ds_path]
    cmd = [
        "git",
        "-C",
        repo.path,
        "log",
        "--max-count=1",
        "--format=%H",
        *commits,
        "--",
        ds_path,
    ]
    try:
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
    commit = r.stdout.strip()
    if not commit:
        # Nothing ever existed at the given dataset path.
        return None

    try:
        return repo.datasets(commit)[ds_path]
    except KeyError:
        # This happens if the dataset was deleted at the commit we found - we'll try the parent:
        try:
            return repo.datasets(f"{commit}^")[ds_path]
        except KeyError:
            # We failed find the dataset. Most likely reason is that it doesn't exist.
            return None


def convert_user_patterns_to_raw_paths(paths, repo, commits):
    """
    Given some user-supplied filter patterns like "path/to/dataset:feature:123" or its equivalent "path/to/dataset:123",
    finds the path encoding for the dataset they apply to and converts them to the feature's path, eg:
    path/to/dataset/.table-dataset/feature/F/9/o/6/kc4F9o6L
    """
    DATASET_DIRNAME = repo.dataset_class.DATASET_DIRNAME
    # Specially handle raw paths, because we can and it's nice for Kart developers
    result = [p for p in paths if f"/{DATASET_DIRNAME}/" in p]
    normal_paths = [p for p in paths if f"/{DATASET_DIRNAME}/" not in p]
    repo_filter = RepoKeyFilter.build_from_user_patterns(normal_paths)
    if repo_filter.match_all:
        return result
    for ds_path, ds_filter in repo_filter.items():
        if ds_filter.match_all:
            result.append(ds_path)
            continue

        for char in "?[]":
            # git pathspecs actually treat '*?[]' specially but we only want to support '*' for now
            ds_path = ds_path.replace(char, f"[{char}]")

        # NOTE: git's interpretation of '*' is pretty loose.
        # It matches all characters in a path *including slashes*, so '*abc' will match 'foo/bar/abc'
        # This is pretty much what we want though 👍
        if ds_filter.match_all:
            result.append(f"{ds_path}/*")
        else:
            for ds_part, part_filter in ds_filter.items():
                if part_filter.match_all:
                    result.append(f"{ds_path}/{DATASET_DIRNAME}/{ds_part}/*")
                    continue

                for item_key in part_filter:
                    if ds_part == "feature":
                        if "*" in ds_path:
                            raise NotYetImplemented(
                                "`kart log` doesn't currently support filters with both wildcards and feature IDs"
                            )
                        else:
                            ds = find_dataset(ds_path, repo, commits)
                            if not ds:
                                result.append(ds_path)
                                continue
                            result.append(
                                ds.encode_pks_to_path(ds.schema.sanitise_pks(item_key))
                            )
                    else:
                        result.append(
                            f"{ds_path}/{DATASET_DIRNAME}/{ds_part}/{item_key}"
                        )
    return result


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
    type=OutputFormatType(
        output_types=["text", "json", "json-lines"],
        allow_text_formatstring=True,
    ),
    default="text",
)
@click.option(
    "--dataset-changes",
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
        "Otherwise, the feature count will be approximated with varying levels of accuracy. "
        "For non-tabular datasets, the feature count is always exact, and refers to the number of tiles."
    ),
)
# Some standard git options
@click.option(
    "-n",
    "--max-count",
    type=int,
    nargs=1,
    help="Limit the number of commits to output.",
)
@click.option(
    "--skip",
    type=int,
    nargs=1,
    metavar="INTEGER",
    help="Skip INTEGER commits before starting to show the commit output.",
)
@click.option(
    "--since",
    "--after",
    nargs=1,
    metavar="DATE",
    help="Show commits more recent than a specific date.",
)
@click.option(
    "--until",
    "--before",
    nargs=1,
    metavar="DATE",
    help="Show commits older than a specific date.",
)
@click.option(
    "--author",
    nargs=1,
    metavar="PATTERN",
    multiple=True,
    help="Limit the commits output to ones with author matching the specified pattern (regular expression)",
)
@click.option(
    "--committer",
    nargs=1,
    metavar="PATTERN",
    multiple=True,
    help="Limit the commits output to ones with committer matching the specified pattern (regular expression)",
)
@click.option(
    "--grep",
    nargs=1,
    metavar="PATTERN",
    multiple=True,
    help="Limit the commits output to ones with log message matching the specified pattern (regular expression)",
)
@click.option(
    "--decorate",
    nargs=1,
    metavar="[=short|full|auto|no]",
    type=click.Choice(["short", "full", "auto", "no"]),
    help="""
    Print out the ref names of any commits that are shown. If short is specified, the ref name prefixes refs/heads/, refs/tags/ and refs/remotes/ will not be printed. If full is specified, the full ref name (including prefix) will be printed. If auto is specified, then if the output is going to a terminal, the ref names are shown as if short were given, otherwise no ref names are shown. The option --decorate is short-hand for --decorate=short. Default to configuration value of log.decorate if configured, otherwise, auto.
    """,
)
@click.option(
    "--no-decorate",
    is_flag=True,
    show_default=True,
    default=False,
    help="Doesn't print out the ref names of any commits that are shown. The option --no-decorate is short-hand for --decorate=no.",
)
@click.option(
    "--decorate-refs",
    multiple=True,
    help=(
        "Overrides the default list of refs to decorate commits with.\n"
        "By default, references are used as decoration if they match "
        "HEAD, refs/heads/, refs/remotes/, refs/stash/, or refs/tags/. "
        "Can be used multiple times."
    ),
)
@click.argument(
    "args",
    metavar="[REVISIONS] [--] [FILTERS]",
    nargs=-1,
    shell_complete=ref_or_repo_path_completer,
)
def log(
    ctx,
    output_format,
    dataset_changes,
    with_feature_count,
    args,
    **kwargs,
):
    """
    Show commit logs.
    The REVISIONS can be a commit, a set of commits, or references to commits. A log containing those commits
    and all their ancestors will be output. The log of a particular range of commits can also be requested
    using the format <commit1>..<commit2> - for more details, see https://git-scm.com/docs/git-log.
    If FILTERS are specified, then only commits where the datasets or features specified were changed will be output.
    Entire datasets can be specified by name, or individual features can be specified using the format
    <dataset-name>:<feature-primary-key>.
    """
    repo = ctx.obj.get_repo(allowed_states=KartRepoState.ALL_STATES)
    options, commits, filters = parse_revisions_and_filters(
        repo, args, kwargs, allow_options=True
    )

    paths = convert_user_patterns_to_raw_paths(filters, repo, commits)
    output_type, fmt = output_format

    # TODO: should we check paths exist here? git doesn't!
    if output_type == "text":
        if fmt:
            options.append(f"--format={fmt}")
        git_args = ["-C", repo.path, "log", *options, *commits, "--", *paths]
        run("git", git_args)

    elif output_type in ("json", "json-lines"):
        try:
            cmd = [
                "git",
                "-C",
                repo.path,
                "log",
                "--format=%H,%D",
                *options,
                *commits,
                "--",
                *paths,
            ]
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
                dataset_changes,
                dataset_change_cache,
                with_feature_count,
            )
            for (commit_id, refs) in commit_ids_and_refs_log
        )
        if output_type == "json-lines":
            for item in commit_log:
                # hardcoded style here; each item must be on one line.
                dump_json_output(item, sys.stdout, "compact")

        else:
            dump_json_output(commit_log, sys.stdout, fmt)


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
    dataset_changes=False,
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
    if dataset_changes:
        result["datasetChanges"] = get_dataset_changes(
            repo, commit, dataset_change_cache
        )
    if with_feature_count:
        if (not dataset_changes) or result["datasetChanges"]:
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
