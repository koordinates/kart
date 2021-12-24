from datetime import datetime, timezone, timedelta
from enum import Enum, auto
import re
import subprocess
import sys
import warnings

import click
import pygit2

from .cli_util import tool_environment
from .exec import execvp
from .exceptions import SubprocessError
from .key_filters import RepoKeyFilter
from .output_util import dump_json_output
from .repo import KartRepoState
from .timestamps import datetime_to_iso8601_utc, timedelta_to_iso8601_tz
from . import diff_estimation


class RemovalInKart012Warning(UserWarning):
    pass


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
                        RemovalInKart012Warning,
                    )
                else:
                    # Insert a second `--` arg.
                    # One of the `--` gets consumed by Click during super() below.
                    # Then the second one gets left alone and we can pass it to git.
                    args.insert(i + 1, "--")
                break

        return super(PreserveDoubleDash, self).parse_args(ctx, args)


class LogArgType(Enum):
    # What to log - a commit, ref, range, etc:
    COMMIT = auto()
    # How to log it.
    OPTION = auto()
    # Which path(s) the user is interested in. Paths must come last.
    PATH = auto()


def get_arg_type(repo, arg, allow_paths=True):
    """Decides what some user-supplied argument to kart log is supposed to do."""
    if arg.startswith("-"):
        # It's not explicitly stated by https://git-scm.com/docs/git-check-ref-format
        # but this isn't a valid commit-ish.
        #    $ git branch -c -- -x
        #    fatal: '-x' is not a valid branch name.
        # So we can assume it's a CLI flag, presumably for git rather than kart.
        # It *could* be a path, but in that case the user should add a `--` before this option
        # to disambiguate, and they haven't done so here.
        issue_link = "https://github.com/koordinates/kart/issues/508"
        warnings.warn(
            f"{arg!r} is unknown to Kart and will be passed directly to git. "
            f"This will be removed in Kart 0.12! Please comment on {issue_link} if you need to use this option.",
            RemovalInKart012Warning,
        )
        return LogArgType.OPTION

    range_parts = re.split(r"\.\.\.?", arg)
    if len(range_parts) <= 2:
        try:
            for part in range_parts:
                repo.resolve_refish(part or "HEAD")
            return LogArgType.COMMIT
        except (KeyError, pygit2.InvalidSpecError):
            pass

    if allow_paths:
        return LogArgType.PATH

    raise click.UsageError(
        f"Argument not recognised as a valid commit, ref, or range: {arg}"
    )


def parse_extra_args(
    repo,
    args,
    *,
    max_count,
    skip,
    since,
    until,
    author,
    committer,
    grep,
):
    """
    Interprets positional `kart log` args, including "--", commits/refs, and paths.
    Returns a two-tuple: (other_args, paths)
    """
    # As soon as we encounter a path, we assume all remaining args are also paths.
    # i.e. the paths must be given *last*.
    # If it's ambiguous whether something is a path or not, we assume it's a commit-ish.
    # If you want to be unambiguous, provide the `--` arg to separate the list of commit-ish-es and paths.
    # This behaviour should be consistent with git's behaviour.

    if "--" in args:
        dash_index = args.index("--")
        paths = list(args[dash_index + 1 :])
        args = args[:dash_index]
    else:
        dash_index = None
        paths = []

    options = []
    commits = []
    allow_paths = dash_index is None
    for arg in args:
        arg_type = get_arg_type(repo, arg, allow_paths=allow_paths)
        {
            LogArgType.OPTION: options,
            LogArgType.COMMIT: commits,
            LogArgType.PATH: paths,
        }[arg_type].append(arg)

    if max_count is not None:
        options.append(f"--max-count={max_count}")
    if skip is not None:
        options.append(f"--skip={skip}")
    if since is not None:
        options.append(f"--since={since}")
    if until is not None:
        options.append(f"--until={until}")
    # These ones can be specified more than once
    if author:
        options.extend(f"--author={a}" for a in author)
    if committer:
        options.extend(f"--committer={c}" for c in committer)
    if grep:
        options.extend(f"--grep={g}" for g in grep)
    return options, commits, paths


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
    # Specially handle raw paths, because we can and it's nice for Kart developers
    result = [p for p in paths if "/.table-dataset/" in p]
    normal_paths = [p for p in paths if "/.table-dataset/" not in p]
    repo_filter = RepoKeyFilter.build_from_user_patterns(normal_paths)
    if repo_filter.match_all:
        return result
    for ds_path, ds_filter in repo_filter.items():
        if ds_filter.match_all:
            result.append(ds_path)
            continue
        ds = find_dataset(ds_path, repo, commits)
        if not ds:
            result.append(ds_path)
            continue
        for ds_part, part_filter in ds_filter.items():
            if part_filter.match_all:
                result.append(f"{ds.inner_path}/{ds_part}")
                continue

            for item_key in part_filter:
                if ds_part == "feature":
                    result.append(
                        ds.encode_pks_to_path(ds.schema.sanitise_pks(item_key))
                    )
                else:
                    result.append(f"{ds.inner_path}/{ds_part}/{item_key}")
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
@click.argument(
    "args", metavar="[REVISION RANGE] [--] [FEATURES]", nargs=-1, type=click.UNPROCESSED
)
def log(
    ctx,
    output_format,
    json_style,
    do_dataset_changes,
    with_feature_count,
    args,
    **kwargs,
):
    """
    Show commit logs.
    The REVISION RANGE can be a commit, a set of commits, or references to commits. A log containing those commits
    and all their ancestors will be output. The log of a particular range of commits can also be requested
    using the format <commit1>..<commit2> - for more details, see https://git-scm.com/docs/git-log.
    If FEATURES are specified, then only commits where those features were changed will be output. Entire
    datasets can be specified by name, or individual features can be specified using the format
    <dataset-name>:<feature-primary-key>.
    """
    repo = ctx.obj.get_repo(allowed_states=KartRepoState.ALL_STATES)

    options, commits, paths = parse_extra_args(repo, args, **kwargs)
    paths = convert_user_patterns_to_raw_paths(paths, repo, commits)

    # TODO: should we check paths exist here? git doesn't!
    if output_format == "text":
        git_args = ["git", "-C", repo.path, "log", *options, *commits, "--", *paths]
        execvp("git", git_args)

    elif output_format in ("json", "json-lines"):
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
