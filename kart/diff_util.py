import functools
import logging

import pygit2

from .diff_structs import DatasetDiff, RepoDiff
from .key_filters import DatasetKeyFilter, RepoKeyFilter
from .structure import RepoStructure

L = logging.getLogger("kart.diff_util")


class WCDiffContext:
    """
    Context shared between datasets during a single diff operation.
    This could be used for overriding what the working copy is or where it is found,
    but it is mostly useful for caching the results of diff operations that could be useful to
    more than a single dataset, but must have a shorter-lived scope than the repository itself.

    The operation of diffing the user's worktree with the worktree index is just such an operation -
    - we don't want to run it up front, in case there are no datasets that need this info
    - we want to run it as soon a the first dataset needs this info, then cache the result
    - we want the result to stay cached for the duration of the diff operation, but no longer
      (in eg a long-running test, there might be several diffs run and the worktree might change)
    """

    def __init__(self, repo, all_ds_paths=None):
        self.repo = repo
        self.is_bare = repo.is_bare
        self.all_ds_paths = all_ds_paths

    @property
    def table_working_copy(self):
        assert not self.is_bare
        return self.repo.working_copy

    @property
    def workdir_path(self):
        assert not self.is_bare
        return self.repo.workdir_path

    @property
    def workdir_index_path(self):
        return self.repo.gitdir_file("worktree-index")

    @functools.lru_cache(maxsize=1)
    def workdir_diff(self):
        # This is the main reason to hold onto this context throughout an entire diff -
        # we can reuse this result for more than one dataset.
        index = pygit2.Index(str(self.workdir_index_path))
        index._repo = self.repo
        return index.diff_to_workdir(pygit2.GIT_DIFF_INCLUDE_UNTRACKED)

    @functools.lru_cache(maxsize=1)
    def workdir_deltas_by_ds_path(self):
        """Returns all the deltas from self.workdir_diff() but grouped by dataset path."""
        with_and_without_slash = [
            (p.rstrip("/") + "/", p.rstrip("/")) for p in self.all_ds_paths
        ]

        def find_ds_path(delta):
            path = delta.old_file.path if delta.old_file else delta.new_file.path
            for with_slash, without_slash in with_and_without_slash:
                if path.startswith(with_slash):
                    return without_slash

        deltas_by_ds_path = {}
        for delta in self.workdir_diff().deltas:
            ds_path = find_ds_path(delta)
            deltas_by_ds_path.setdefault(ds_path, []).append(delta)

        return deltas_by_ds_path


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
    wc_diff_context=None,
    repo_key_filter=RepoKeyFilter.MATCH_ALL,
):
    """
    Generates a RepoDiff containing an entry for every dataset in the repo
    (so long as it matches repo_key_filter and has any changes).

    base_rs, target_rs - kart.structure.RepoStructure objects to diff between.
    include_wc_diff - if True the diff generated will be from base_rs<>working_copy
        (in which case, target_rs must be the HEAD commit which the working copy is tracking).
    wc_diff_context - not required, but can be used to control where the working-copy is found
    repo_key_filter - controls which datasets (and PK values) match and are included in the diff.
    """

    all_ds_paths = get_all_ds_paths(base_rs, target_rs, repo_key_filter)
    if wc_diff_context is None:
        wc_diff_context = WCDiffContext(target_rs.repo, all_ds_paths)

    repo_diff = RepoDiff()
    for ds_path in all_ds_paths:
        repo_diff[ds_path] = get_dataset_diff(
            ds_path,
            base_rs.datasets(),
            target_rs.datasets(),
            include_wc_diff=include_wc_diff,
            wc_diff_context=wc_diff_context,
            ds_filter=repo_key_filter[ds_path],
        )
    # No need to recurse since self.get_dataset_diff already prunes the dataset diffs.
    repo_diff.prune(recurse=False)
    return repo_diff


def get_dataset_diff(
    ds_path,
    base_datasets,
    target_datasets,
    *,
    include_wc_diff=False,
    wc_diff_context=None,
    ds_filter=DatasetKeyFilter.MATCH_ALL,
):
    """
    Generates the DatasetDiff for the dataset at path dataset_path.

    base_rs, target_rs - kart.structure.RepoStructure objects to diff between.
    include_wc_diff - if True the diff generated will be from base_rs<>working_copy
        (in which case, target_rs must be the HEAD commit which the working copy is tracking).
    wc_diff_context - reusing the same WCDiffContext for every dataset that is being diffed at one time
        is more efficient as it can save pygit2.Index.diff_to_worktree being called multiple times
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
        if wc_diff_context is None:
            wc_diff_context = WCDiffContext(target_ds.repo)

        target_wc_diff = target_ds.diff_to_wc(wc_diff_context, ds_filter=ds_filter)
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
