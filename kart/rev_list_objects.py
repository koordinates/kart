# Utility for scanning through all the objects in the commit graph, or in a particular part of the commit graph.
# For example, reachable from commits A, B, C, but not from D, E, F (which have already been taken care of).

import os
import re
import subprocess

import pygit2

from kart.core import all_trees_with_paths_in_tree
from kart.cli_util import tool_environment
from kart.exceptions import SubprocessError


def _rev_list_commits_command(repo):
    return ["git", "-C", repo.path, "rev-list"]


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


def rev_list_object_oids(repo, start_commits, stop_commits, pathspecs):
    """
    Yield all the objects referenced between the start and stop commits as tuples (commit_id, path, object_id),
    and which match at least one of the given pathspecs.
    Each object will only be yielded once, so not necessarily at all paths and commits where it can be found.
    """
    if not pathspecs:
        return

    cmd = [
        *_rev_list_objects_command(repo),
        *start_commits,
        "--not",
        *stop_commits,
        "--stdin",
    ]
    try:
        p = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            encoding="utf8",
            env=tool_environment(),
        )
        p.stdin.write(os.linesep.join(["--", *pathspecs]))
        p.stdin.close()
        yield from _parse_revlist_output(repo, p.stdout)
    except subprocess.CalledProcessError as e:
        raise SubprocessError(
            f"There was a problem with git rev-list: {e}", called_process_error=e
        )


def get_dataset_pathspecs(repo, start_commits, stop_commits, dirname_filter):
    """
    Get the list of dataset paths we need to search in for the objects we are insterested in.
    Without this, all datasets are searched - even irrelevant ones - which could waste a lot of time.
    """
    # TODO - if git rev-list --objects pathspec filtering is fixed, there would be other approaches
    # we could take, some potentially using only a single call to rev-list.
    # See https://public-inbox.org/git/CAPJmHpWWJ4sssfG2oym7K=MsT3+KTHQP-QK88nfXNOtfcv07ew@mail.gmail.com/
    cmd = [
        *_rev_list_commits_command(repo),
        *start_commits,
        "--not",
        *stop_commits,
    ]
    result = set()
    try:
        p = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            encoding="utf8",
            env=tool_environment(),
        )
        for line in p.stdout:
            commit = repo[line.strip()]
            for tree_path, tree in all_trees_with_paths_in_tree(commit.tree):
                if tree_path in result:
                    continue
                for child in tree:
                    if dirname_filter(child.name):
                        result.add(tree_path)
                        break

        return result

    except subprocess.CalledProcessError as e:
        raise SubprocessError(
            f"There was a problem with git rev-list: {e}", called_process_error=e
        )


def rev_list_blobs(repo, start_commits, stop_commits, pathspecs):
    """
    Yield all the blobs referenced between the start and stop commits as tuples (commit_id, path, blob).
    Each blob will only be yielded once, so not necessarily at all paths and commits where it can be found.
    """
    for (commit_id, path, oid) in rev_list_object_oids(
        repo, start_commits, stop_commits, pathspecs
    ):
        obj = repo[oid]
        if obj.type == pygit2.GIT_OBJ_BLOB:
            yield commit_id, path, obj


def rev_list_matching_blobs(repo, start_commits, stop_commits, pathspecs, path_pattern):
    """
    Yield all the blobs with a path matching the given pattern referenced between the start and stop commits as tuples
    (commit_id, match_result, blob). To get the entire path, use match_result.group(0).
    """
    for (commit_id, path, oid) in rev_list_object_oids(
        repo, start_commits, stop_commits, pathspecs
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
    Yields tuples in the form: (commit_id, match_result, blob).
    To get the entire path, use match_result.group(0) - this can be decoded if necessary.
    To get the dataset-path, use match_result.group(1)
    """
    pathspecs = get_dataset_pathspecs(
        repo,
        start_commits,
        stop_commits,
        dirname_filter=lambda d: d.startswith(".table-dataset") or d == ".sno-dataset",
    )
    return rev_list_matching_blobs(
        repo, start_commits, stop_commits, pathspecs, FEATURE_BLOBS_PATTERN
    )


TILE_POINTER_FILES_PATTERN = re.compile(r"(.+)/\.point-cloud-dataset[^/]*/tile/.+")


def rev_list_tile_pointer_files(repo, start_commits, stop_commits):
    """
    Yield all the blobs with a path identifying them as LFS pointers to the tiles of a point-cloud dataset.
    Yields tuples in the form: (commit_id, match_result, blob).
    To get the entire path, use match_result.group(0) - this can be decoded if necessary.
    To get the dataset-path, use match_result.group(1)
    """
    pathspecs = get_dataset_pathspecs(
        repo,
        start_commits,
        stop_commits,
        dirname_filter=lambda d: d.startswith(".point-cloud-dataset"),
    )
    return rev_list_matching_blobs(
        repo, start_commits, stop_commits, pathspecs, TILE_POINTER_FILES_PATTERN
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
