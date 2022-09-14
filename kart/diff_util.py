import functools
import logging
import re
from pathlib import PurePosixPath
import subprocess

from kart.cli_util import tool_environment
from kart.diff_structs import ATTACHMENT_KEY, Delta, DeltaDiff, DatasetDiff, RepoDiff
from kart.exceptions import SubprocessError
from kart.key_filters import DatasetKeyFilter, RepoKeyFilter
from kart.structure import RepoStructure

L = logging.getLogger("kart.diff_util")


def get_all_ds_paths(
    base_rs: RepoStructure,
    target_rs: RepoStructure,
    repo_key_filter=RepoKeyFilter.MATCH_ALL,
):
    """Returns a list of all dataset paths in either RepoStructure (that match repo_key_filter).

    Args:
        base_rs (kart.structure.RepoStructure)
        target_rs (kart.structure.RepoStructure)
        repo_key_filter (kart.key_filters.RepoKeyFilter): Controls which datasets match and are included in the result.

    Returns:
        Sorted list of all dataset paths in either RepoStructure (that match repo_key_filter).
    """
    base_ds_paths = {ds.path for ds in base_rs.datasets()}
    target_ds_paths = {ds.path for ds in target_rs.datasets()}
    all_ds_paths = base_ds_paths | target_ds_paths

    if not repo_key_filter.match_all:
        all_ds_paths = repo_key_filter.filter_keys(all_ds_paths)

    return sorted(list(all_ds_paths))


def get_repo_diff(
    base_rs,
    target_rs,
    *,
    include_wc_diff=False,
    workdir_diff_cache=None,
    repo_key_filter=RepoKeyFilter.MATCH_ALL,
    convert_to_dataset_format=False,
    include_attachments=False,
):
    """
    Generates a RepoDiff containing an entry for every dataset in the repo
    (so long as it matches repo_key_filter and has any changes).

    base_rs, target_rs - kart.structure.RepoStructure objects to diff between.
    include_wc_diff - if True the diff generated will be from base_rs<>working_copy
        (in which case, target_rs must be the HEAD commit which the working copy is tracking).
    workdir_diff_cache - not required, but can be provided if a WorkdirDiffCache is already in use
        to save repeated work.
    repo_key_filter - controls which datasets (and PK values) match and are included in the diff.
    convert_to_dataset_format - whether to show the diff of what would be committed if files were
       converted to dataset format at commit-time (ie, for point-cloud tiles)
    include_attachments - whether to include a DatasetDiff in the result for changes that are not
       contained in any particular dataset.
    """

    all_ds_paths = get_all_ds_paths(base_rs, target_rs, repo_key_filter)

    if include_wc_diff and workdir_diff_cache is None:
        workdir_diff_cache = target_rs.repo.working_copy.workdir_diff_cache()

    repo_diff = RepoDiff()
    for ds_path in all_ds_paths:
        repo_diff[ds_path] = get_dataset_diff(
            ds_path,
            base_rs.datasets(),
            target_rs.datasets(),
            include_wc_diff=include_wc_diff,
            workdir_diff_cache=workdir_diff_cache,
            ds_filter=repo_key_filter[ds_path],
            convert_to_dataset_format=convert_to_dataset_format,
        )
    if include_attachments:
        attachment_diff = get_attachment_diff(
            base_rs, target_rs, repo_key_filter=repo_key_filter
        )
        if attachment_diff:
            repo_diff.recursive_set([ATTACHMENT_KEY, ATTACHMENT_KEY], attachment_diff)

    # No need to prune recursively since self.get_dataset_diff already prunes the dataset diffs.
    repo_diff.prune(recurse=False)
    return repo_diff


def get_dataset_diff(
    ds_path,
    base_datasets,
    target_datasets,
    *,
    include_wc_diff=False,
    workdir_diff_cache=None,
    ds_filter=DatasetKeyFilter.MATCH_ALL,
    convert_to_dataset_format=False,
):
    """
    Generates the DatasetDiff for the dataset at path dataset_path.

    base_rs, target_rs - kart.structure.RepoStructure objects to diff between.
    include_wc_diff - if True the diff generated will be from base_rs<>working_copy
        (in which case, target_rs must be the HEAD commit which the working copy is tracking).
    workdir_diff_cache - reusing the same WorkdirDiffCache for every dataset that is being diffed at one time
        is more efficient as it can save FileSystemWorkingCopy.raw_diff_from_index being called multiple times
    ds_filter - controls which PK values match and are included in the diff.
    """
    base_target_diff = None
    target_wc_diff = None

    if base_datasets == target_datasets:
        base_ds = target_ds = base_datasets.get(ds_path)

    else:
        # diff += base_ds<>target_ds
        base_ds = base_datasets.get(ds_path)
        target_ds = target_datasets.get(ds_path)

        if base_ds is not None:
            from_ds, to_ds = base_ds, target_ds
            reverse = False
        else:
            from_ds, to_ds = target_ds, base_ds
            reverse = True

        base_target_diff = from_ds.diff(to_ds, ds_filter=ds_filter, reverse=reverse)
        L.debug("base<>target diff (%s): %s", ds_path, repr(base_target_diff))

    if include_wc_diff:
        # diff += target_ds<>working_copy
        if workdir_diff_cache is None:
            workdir_diff_cache = target_ds.repo.working_copy.workdir_diff_cache()

        if target_ds is not None:
            target_wc_diff = target_ds.diff_to_working_copy(
                workdir_diff_cache,
                ds_filter=ds_filter,
                convert_to_dataset_format=convert_to_dataset_format,
            )
            L.debug(
                "target<>working_copy diff (%s): %s",
                ds_path,
                repr(target_wc_diff),
            )

    ds_diff = DatasetDiff.concatenated(
        base_target_diff, target_wc_diff, overwrite_original=True
    )
    ds_diff.prune()
    return ds_diff


ZEROES = re.compile(r"0+")


def get_attachment_diff(
    base_rs,
    target_rs,
    *,
    include_wc_diff=False,
    workdir_diff_cache=None,
    repo_key_filter=RepoKeyFilter.MATCH_ALL,
):
    """
    Returns a delta-diff for changed files aka attachments.
    Each delta just contains the old and new file OIDs - any more than this may be unhelpful since it takes
    CPU time to produce but isn't necessarily easier to consume than OIDs, which are straight-forward to
    turn into raw files once you know how. (Various diff-writers can transform these OIDs into inline diffs if you
    set the --diff-files flag).
    """

    # We don't yet support attachment diffs in the workdir
    assert not include_wc_diff

    old_tree = base_rs.tree
    new_tree = target_rs.tree
    repo = target_rs.repo

    # TODO - make sure this is skipping over datasets efficiently.
    # TODO - we could turn on rename detection.
    cmd = [
        "git",
        "-C",
        repo.path,
        "diff",
        old_tree.hex,
        new_tree.hex,
        "--raw",
        "--no-renames",
        "--",
        ":^.kart.*",  # Top-level hidden kart blobs
        ":^**/.*dataset*/**",  # Data inside datasets
    ]
    try:
        lines = (
            subprocess.check_output(
                cmd,
                encoding="utf8",
                env=tool_environment(),
            )
            .strip()
            .splitlines()
        )
    except subprocess.CalledProcessError as e:
        raise SubprocessError(
            f"There was a problem with git diff: {e}", called_process_error=e
        )

    attachment_deltas = DeltaDiff()

    for line in lines:
        parts = line.split()
        old_sha, new_sha, path = parts[2], parts[3], parts[5]
        if not path_matches_repo_key_filter(path, repo_key_filter):
            continue
        if is_dataset_metadata_xml(old_tree, new_tree, path):
            continue
        old_half_delta = (path, old_sha) if not ZEROES.fullmatch(old_sha) else None
        new_half_delta = (path, new_sha) if not ZEROES.fullmatch(new_sha) else None
        attachment_deltas.add_delta(Delta(old_half_delta, new_half_delta))

    return attachment_deltas


def path_matches_repo_key_filter(path, repo_key_filter):
    if repo_key_filter.match_all:
        return True
    # Return attachments that have a name that we are matching all of.
    if path in repo_key_filter and repo_key_filter[path].match_all:
        return True
    # Return attachments that are inside a folder that we are matching all of.
    for p, dataset_filter in repo_key_filter.items():
        if not dataset_filter.match_all:
            continue
        if p == path:
            return True
        if path.startswith(p) and (p.endswith("/") or path[len(p)] == "/"):
            return True
    # Don't return attachments inside a dataset / folder that we are only matching some of
    # ie, only matching certain features or meta items.
    return False


def is_dataset_metadata_xml(old_tree, new_tree, path):
    # metadata.xml is sometimes stored in the "attachment" area, alongside the dataset, rather than inside it.
    # Storing it in this unusual location adds complexity without actually solving any problems, so any datasets designed
    # after table.v3 don't do this. Since this change already appears in meta-diffs, we suppress it from attachment diffs.
    if not path.endswith("/metadata.xml"):
        return False
    parent_path = str(PurePosixPath(path).parent)
    for tree in (old_tree, new_tree):
        try:
            parent_tree = tree / parent_path
            for sibling in parent_tree:
                if sibling.name.startswith(".table-dataset"):
                    return True
        except KeyError:
            continue
    return False
