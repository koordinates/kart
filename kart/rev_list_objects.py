# Utility for scanning through all the objects in the commit graph, or in a particular part of the commit graph.
# For example, reachable from commits A, B, C, but not from D, E, F (which have already been taken care of).

import re
import subprocess

import pygit2

from kart.cli_util import tool_environment
from kart.exceptions import SubprocessError


def _rev_list_objects_command(repo):
    return [
        "git",
        "-C",
        repo.path,
        "rev-list",
        "--objects",
        # We should really use --missing=allow-promisor, but that option currently exposes a race condition in Git.
        # missing=allow-any is the next best option.
        "--missing=allow-any",
        # We don't necessarily always need commit-order, but only using it sometimes just makes the code more complex.
        "--in-commit-order",
    ]


DS_PATH_PATTERN = r"(.+)/\.(sno|table)-dataset/"


def rev_list_object_oids(repo, start_commits, stop_commits):
    """
    Yield all the objects referenced between the start and stop commits as tuples (commit_id, path, object_id).
    Each object will only be yielded once, so not necessarily at all paths and commits where it can be found.
    """
    cmd = [
        *_rev_list_objects_command(repo),
        *start_commits,
        "--not",
        *stop_commits,
    ]
    try:
        p = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            encoding="utf8",
            env=tool_environment(),
        )
        yield from _parse_revlist_output(repo, p.stdout)
    except subprocess.CalledProcessError as e:
        raise SubprocessError(
            f"There was a problem with git rev-list: {e}", called_process_error=e
        )


def rev_list_blobs(repo, start_commits, stop_commits):
    """
    Yield all the blobs referenced between the start and stop commits as tuples (commit_id, path, blob).
    Each blob will only be yielded once, so not necessarily at all paths and commits where it can be found.
    """
    for (commit_id, path, oid) in rev_list_object_oids(
        repo, start_commits, stop_commits
    ):
        obj = repo[oid]
        if obj.type == pygit2.GIT_OBJ_BLOB:
            yield commit_id, path, obj


def rev_list_matching_blobs(repo, start_commits, stop_commits, path_pattern):
    """
    Yield all the blobs with a path matching the given pattern referenced between the start and stop commits as tuples
    (commit_id, match_result, blob). To get the entire path, use match_result.group(0).
    """
    for (commit_id, path, oid) in rev_list_object_oids(
        repo, start_commits, stop_commits
    ):
        m = path_pattern.fullmatch(path)
        if m:
            obj = repo[oid]
            if obj.type == pygit2.GIT_OBJ_BLOB:
                yield commit_id, m, obj


FEATURE_BLOBS_PATTERN = re.compile(r"(.+)/\.(?:sno|table)-dataset[^/]*/feature/.+")


def rev_list_feature_blobs(repo, start_commits, stop_commits):
    """
    Yield all the blobs with a path identifying them as features (or rows) of a "table-dataset".
    (commit_id, match_result, blob).
    To get the entire path, use match_result.group(0) - this can be decoded if necessary.
    To get the dataset-path, use match_result.group(1)
    """
    return rev_list_matching_blobs(
        repo, start_commits, stop_commits, FEATURE_BLOBS_PATTERN
    )


TILE_POINTER_FILES_PATTERN = re.compile(r"(.+)/\.point-cloud-dataset[^/]*/tile/.+")


def rev_list_tile_pointer_files(repo, start_commits, stop_commits):
    """
    Yield all the blobs with a path identifying them as features (or rows) of a "table-dataset".
    (commit_id, match_result, blob).
    To get the entire path, use match_result.group(0) - this can be decoded if necessary.
    To get the dataset-path, use match_result.group(1)
    """
    return rev_list_matching_blobs(
        repo, start_commits, stop_commits, TILE_POINTER_FILES_PATTERN
    )


def _parse_revlist_output(repo, line_iter):
    commit_id = None
    for line in line_iter:
        parts = line.split(" ", maxsplit=1)
        if len(parts) == 1:
            oid = parts[0].strip()
            if repo[oid].type == pygit2.GIT_OBJ_COMMIT:
                commit_id = oid
            continue

        oid, path = parts
        yield commit_id, path.strip(), oid.strip()
