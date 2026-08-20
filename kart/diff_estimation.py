import threading

import pygit2

from kart.diff_util import get_dataset_diff
from kart.exceptions import SubprocessError
from kart import subprocess_util as subprocess

ACCURACY_SUBTREE_SAMPLES = {
    "veryfast": 2,
    "fast": 16,
    "medium": 32,
    "good": 64,
}


ACCURACY_CHOICES = ("veryfast", "fast", "medium", "good", "exact")


def get_exact_diff_blob_count(repo, tree1, tree2):
    """
    Returns an exact blob count for the diff between the two pygit2.Tree instances
    """
    if tree1 == tree2:
        return 0

    git_rev_spec = f"{tree1.id}..{tree2.id}"
    p = subprocess.Popen(
        [
            "git",
            "-C",
            repo.path,
            "diff",
            "--name-only",
            "--no-renames",
            git_rev_spec,
        ],
        stdout=subprocess.PIPE,
        encoding="utf-8",
    )
    count = sum(1 for x in p.stdout)
    retcode = p.wait()
    if retcode != 0:
        raise SubprocessError("Error calling git diff", exit_code=retcode)
    return count


def get_exact_diff_blob_type_counts(repo, tree1, tree2):
    """
    Returns exact counts of the blobs added, modified and deleted between the two
    pygit2.Tree instances, as a dict: {"inserts": int, "updates": int, "deletes": int}
    Types with a count of zero are omitted, as they are by DeltaDiff.type_counts()
    """
    if tree1 == tree2:
        return {}

    git_rev_spec = f"{tree1.id}..{tree2.id}"
    p = subprocess.Popen(
        [
            "git",
            "-C",
            repo.path,
            "diff",
            "--name-status",
            "--no-renames",
            # -z makes git use \0 as a separator, both between entries and between
            # each entry's status and path. Without it, unusual paths are quoted
            # and escaped, which is harder to parse.
            "-z",
            git_rev_spec,
        ],
        stdout=subprocess.PIPE,
        encoding="utf-8",
    )
    counts = {"inserts": 0, "updates": 0, "deletes": 0}
    # Each entry is "<status>\0<path>\0" - we only care about the statuses.
    # --no-renames means R (rename) can't occur, and C (copy) requires -C.
    pieces = iter(p.stdout.read().split("\0"))
    for status in pieces:
        if not status:
            # trailing separator at the end of the output
            continue
        try:
            next(pieces)  # the path this status refers to
        except StopIteration:
            raise SubprocessError(f"Truncated git diff output at status {status!r}")
        if status == "A":
            counts["inserts"] += 1
        elif status == "D":
            counts["deletes"] += 1
        elif status in ("M", "T"):
            # T is a change of object type, which shouldn't happen inside a dataset,
            # but if it does, it's a change to a blob that exists in both trees.
            counts["updates"] += 1
        else:
            raise SubprocessError(f"Unexpected git diff status {status!r}")

    retcode = p.wait()
    if retcode != 0:
        raise SubprocessError("Error calling git diff", exit_code=retcode)
    return {k: v for k, v in counts.items() if v}


def get_approximate_diff_blob_count(
    repo, accuracy, tree1, tree2, dataset_path, path_encoder
):
    """
    Returns an approximate blob count of the required accuracy for the diff between the two pygit2.Tree instances,
    as long as both Trees are either feature trees with features arranged according to the given path_encoder,
    or the empty tree.
    """
    if tree1 == tree2:
        return 0

    total_samples_to_take = ACCURACY_SUBTREE_SAMPLES[accuracy]
    return path_encoder.diff_estimate(
        tree1, tree2, path_encoder.branches, total_samples_to_take
    )


terminate_estimate_thread = threading.Event()


class ThreadTerminated(RuntimeError):
    pass


def get_data_tree(repo, ds):
    if ds:
        return ds.feature_tree if ds.DATASET_TYPE == "table" else ds.tile_tree
    else:
        return repo.empty_tree


TYPE_COUNTS_ANNOTATION_TYPE = "feature-change-type-counts-exact"


def _invert_type_counts(counts):
    """
    Given type counts for a diff, returns the type counts for the reverse diff:
    what was inserted was deleted, and vice versa.
    """
    result = {}
    if "deletes" in counts:
        result["inserts"] = counts["deletes"]
    if "updates" in counts:
        result["updates"] = counts["updates"]
    if "inserts" in counts:
        result["deletes"] = counts["inserts"]
    return result


def get_diff_feature_type_counts(repo, base, target):
    """
    Counts the features (or tiles) inserted, updated and deleted by the given diff,
    for each dataset in it.
    Returns a dict: {dataset_path: {"inserts": int, "updates": int, "deletes": int}}
    Datasets with no feature/tile changes are not present in the dict, and neither
    are types with a count of zero.

    Counts are always exact - unlike estimate_diff_feature_counts(), there's no
    approximation available, since sampling subtrees can't tell the types apart.
    """
    base = base.peel(pygit2.Tree)
    target = target.peel(pygit2.Tree)
    if base == target:
        return {}

    # Unlike the other annotation types, these counts differ depending on which way
    # around the diff is, so they're stored against an ordered key.
    annotation = repo.diff_annotations.get(
        base=base,
        target=target,
        annotation_type=TYPE_COUNTS_ANNOTATION_TYPE,
        ordered=True,
    )
    if annotation is not None:
        return annotation
    # If we've already done this diff in the other direction, we can just invert it
    # rather than doing the work again.
    reverse = repo.diff_annotations.get(
        base=target,
        target=base,
        annotation_type=TYPE_COUNTS_ANNOTATION_TYPE,
        ordered=True,
    )
    if reverse is not None:
        return {ds_path: _invert_type_counts(c) for ds_path, c in reverse.items()}

    base_rs = repo.structure(base)
    target_rs = repo.structure(target)
    all_ds_paths = {ds.path for ds in base_rs.datasets()} | {
        ds.path for ds in target_rs.datasets()
    }

    dataset_type_counts = {}
    for dataset_path in all_ds_paths:
        if terminate_estimate_thread.is_set():
            raise ThreadTerminated()

        base_ds = base_rs.datasets().get(dataset_path)
        target_ds = target_rs.datasets().get(dataset_path)
        if not base_ds and not target_ds:
            continue

        counts = get_exact_diff_blob_type_counts(
            repo,
            get_data_tree(repo, base_ds),
            get_data_tree(repo, target_ds),
        )
        if counts:
            dataset_type_counts[dataset_path] = counts

    repo.diff_annotations.store(
        base=base,
        target=target,
        annotation_type=TYPE_COUNTS_ANNOTATION_TYPE,
        data=dataset_type_counts,
        ordered=True,
    )

    if terminate_estimate_thread.is_set():
        raise ThreadTerminated()

    return dataset_type_counts


def estimate_diff_feature_counts(
    repo,
    base,
    target,
    *,
    include_wc_diff=False,
    accuracy,
):
    """
    Estimates feature counts for each dataset in the given diff.
    Returns a dict (keys are dataset paths; values are feature counts)
    Datasets with (probably) no features changed are not present in the dict.
    `accuracy` should be one of ACCURACY_CHOICES
    """
    base = base.peel(pygit2.Tree)
    target = target.peel(pygit2.Tree)
    if base == target and not include_wc_diff:
        return {}

    assert accuracy in ACCURACY_CHOICES

    # We can use the cache if we don't care about the working copy.
    if not include_wc_diff:
        annotation_type = f"feature-change-counts-{accuracy}"
        annotation = repo.diff_annotations.get(
            base=base,
            target=target,
            annotation_type=annotation_type,
        )
        if annotation is not None:
            return annotation

    base_rs = repo.structure(base)
    target_rs = repo.structure(target)

    base_ds_paths = {ds.path for ds in base_rs.datasets()}
    target_ds_paths = {ds.path for ds in target_rs.datasets()}
    all_ds_paths = base_ds_paths | target_ds_paths
    workdir_diff_cache = repo.working_copy.workdir_diff_cache()

    dataset_change_counts = {}
    for dataset_path in all_ds_paths:
        if terminate_estimate_thread.is_set():
            raise ThreadTerminated()

        base_ds = base_rs.datasets().get(dataset_path)
        target_ds = target_rs.datasets().get(dataset_path)
        if not base_ds and not target_ds:
            continue

        base_data_tree = get_data_tree(repo, base_ds)
        target_data_tree = get_data_tree(repo, target_ds)
        if (base_ds or target_ds).DATASET_TYPE != "table":
            # point-cloud datasets have a small number of tiles, so we can just count them.
            accuracy = "exact"

        if accuracy == "exact" and include_wc_diff:
            # can't really avoid this - to generate an exact count for this diff we have to generate the diff

            ds_diff = get_dataset_diff(
                dataset_path,
                base_rs.datasets(),
                target_rs.datasets(),
                include_wc_diff=include_wc_diff,
                workdir_diff_cache=workdir_diff_cache,
            )
            ds_total = len(ds_diff.get("feature", []))

        elif accuracy == "exact":
            # nice, simple, no stats involved. but slow :/
            ds_total = get_exact_diff_blob_count(repo, base_data_tree, target_data_tree)
        else:
            path_encoder = (
                base_ds.feature_path_encoder
                if base_ds
                else target_ds.feature_path_encoder
            )
            ds_total = get_approximate_diff_blob_count(
                repo,
                accuracy,
                base_data_tree,
                target_data_tree,
                dataset_path,
                path_encoder,
            )
            if include_wc_diff and target_ds:
                # TODO: this code shouldn't special-case tabular working copies
                table_wc = repo.working_copy.tabular
                if table_wc:
                    ds_total += table_wc.tracking_changes_count(target_ds)

        if ds_total:
            dataset_change_counts[dataset_path] = ds_total

    if not include_wc_diff:
        repo.diff_annotations.store(
            base=base,
            target=target,
            annotation_type=annotation_type,
            data=dataset_change_counts,
        )

    if terminate_estimate_thread.is_set():
        raise ThreadTerminated()

    return dataset_change_counts
