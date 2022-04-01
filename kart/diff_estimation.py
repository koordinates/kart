import subprocess
import threading

import pygit2

from kart.exceptions import SubprocessError

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
        raise SubprocessError("Error calling git diff", retcode)
    return count


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

    working_copy = repo.working_copy if include_wc_diff else None
    if not working_copy:
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

    dataset_change_counts = {}
    for dataset_path in all_ds_paths:
        if terminate_estimate_thread.is_set():
            raise ThreadTerminated()

        base_ds = base_rs.datasets().get(dataset_path)
        target_ds = target_rs.datasets().get(dataset_path)
        if not base_ds and not target_ds:
            continue

        base_feature_tree = base_ds.feature_tree if base_ds else repo.empty_tree
        target_feature_tree = target_ds.feature_tree if target_ds else repo.empty_tree

        if accuracy == "exact" and working_copy:
            # can't really avoid this - to generate an exact count for this diff we have to generate the diff
            from kart.diff_util import get_dataset_diff

            ds_diff = get_dataset_diff(
                dataset_path, base_rs.datasets(), target_rs.datasets(), working_copy
            )
            ds_total = len(ds_diff.get("feature", []))

        elif accuracy == "exact":
            # nice, simple, no stats involved. but slow :/
            ds_total = get_exact_diff_blob_count(
                repo, base_feature_tree, target_feature_tree
            )
        else:
            path_encoder = (
                base_ds.feature_path_encoder
                if base_ds
                else target_ds.feature_path_encoder
            )
            ds_total = get_approximate_diff_blob_count(
                repo,
                accuracy,
                base_feature_tree,
                target_feature_tree,
                dataset_path,
                path_encoder,
            )
            if working_copy and target_ds:
                ds_total += working_copy.tracking_changes_count(target_ds)

        if ds_total:
            dataset_change_counts[dataset_path] = ds_total

    if not working_copy:
        repo.diff_annotations.store(
            base=base,
            target=target,
            annotation_type=annotation_type,
            data=dataset_change_counts,
        )

    if terminate_estimate_thread.is_set():
        raise ThreadTerminated()

    return dataset_change_counts
