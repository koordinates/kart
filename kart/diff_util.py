import logging

from .diff_structs import RepoDiff, DatasetDiff
from .key_filters import RepoKeyFilter, DatasetKeyFilter


L = logging.getLogger("kart.diff_util")


def get_all_ds_paths(base_rs, target_rs, repo_key_filter=RepoKeyFilter.MATCH_ALL):
    """
    Returns a list of all dataset paths in either RepoStructure (that match repo_key_filter).

    base_rs, target_rs - kart.structure.RepoStructure objects
    repo_key_filter - controls which datasets match and are included in the result.
    """
    base_ds_paths = {ds.path for ds in base_rs.datasets}
    target_ds_paths = {ds.path for ds in target_rs.datasets}
    all_ds_paths = base_ds_paths | target_ds_paths

    if not repo_key_filter.match_all:
        all_ds_paths = all_ds_paths & repo_key_filter.keys()

    return sorted(list(all_ds_paths))


def get_repo_diff(
    base_rs, target_rs, working_copy=None, repo_key_filter=RepoKeyFilter.MATCH_ALL
):
    """
    Generates a RepoDiff containing an entry for every dataset in the repo
    (so long as it matches repo_key_filter and has any changes).

    base_rs, target_rs - kart.structure.RepoStructure objects to diff between.
    working_copy - if supplied the diff generated will be from base_rs<>working_copy
        (in which case, target_rs must be the HEAD commit which the working copy is tracking).
    repo_key_filter - controls which datasets (and PK values) match and are included in the diff.
    """

    all_ds_paths = get_all_ds_paths(base_rs, target_rs, repo_key_filter)
    repo_diff = RepoDiff()
    for ds_path in all_ds_paths:
        repo_diff[ds_path] = get_dataset_diff(
            ds_path, base_rs, target_rs, working_copy, repo_key_filter[ds_path]
        )
    # No need to recurse since self.get_dataset_diff already prunes the dataset diffs.
    repo_diff.prune(recurse=False)
    return repo_diff


def get_dataset_diff(
    ds_path, base_rs, target_rs, working_copy=None, ds_filter=DatasetKeyFilter.MATCH_ALL
):
    """
    Generates the DatasetDiff for the dataset at path dataset_path.

    base_rs, target_rs - kart.structure.RepoStructure objects to diff between.
    working_copy - if supplied the diff generated will be from base_rs<>working_copy
        (in which case, target_rs must be the HEAD commit which the working copy is tracking).
    ds_filter - controls which PK values match and are included in the diff.
    """
    ds_diff = None

    if base_rs != target_rs:
        # diff += base_rs<>target_rs
        base_ds = base_rs.datasets.get(ds_path)
        target_ds = target_rs.datasets.get(ds_path)

        params = {}
        if not base_ds:
            base_ds, target_ds = target_ds, base_ds
            params["reverse"] = True

        base_target_diff = base_ds.diff(target_ds, ds_filter=ds_filter, **params)
        L.debug("base<>target diff (%s): %s", ds_path, repr(base_target_diff))
        ds_diff = base_target_diff

    if working_copy:
        # diff += target_rs<>working_copy
        target_ds = target_rs.datasets.get(ds_path)
        target_wc_diff = working_copy.diff_db_to_tree(target_ds, ds_filter=ds_filter)
        L.debug(
            "target<>working_copy diff (%s): %s",
            ds_path,
            repr(target_wc_diff),
        )
        if ds_diff is None:
            ds_diff = target_wc_diff
        else:
            ds_diff += target_wc_diff

    if ds_diff is None:
        return DatasetDiff()
    else:
        ds_diff.prune()
        return ds_diff
