import logging
import statistics
import subprocess
import time

from kart.exceptions import SubprocessError

L = logging.getLogger("kart.diff_estimation")

# required_confidence -> z_score
Z_SCORES = {
    0.50: 0.0,
    0.60: 0.26,
    0.70: 0.53,
    0.75: 0.68,
    0.80: 0.85,
    0.85: 1.04,
    0.90: 1.29,
    0.95: 1.65,
    0.99: 2.33,
}

# accuracy -> (sample_size, required_confidence, z_score)
ACCURACY_PARAMS = {
    "veryfast": (2, 0.00001, 0.0),
    "fast": (2, 0.60, Z_SCORES[0.60]),
    "medium": (8, 0.80, Z_SCORES[0.80]),
    "good": (16, 0.95, Z_SCORES[0.95]),
}


def _feature_count_sample_trees(repo, git_rev_spec, tree_paths_sample, num_trees):
    p = subprocess.Popen(
        [
            "git",
            "-C",
            repo.path,
            "diff",
            "--name-only",
            "--no-renames",
            git_rev_spec,
            "--",
            *tree_paths_sample,
        ],
        stdout=subprocess.PIPE,
        encoding="utf-8",
    )
    tree_samples = {}
    for line in p.stdout:
        # path/to/dataset/.sno-dataset/feature/ab/cd/abcdef123
        # --> ab/cd
        root, tree, subtree, basename = line.rsplit("/", 3)
        k = f"{tree}/{subtree}"
        tree_samples.setdefault(k, 0)
        tree_samples[k] += 1
    retcode = p.wait()
    if retcode != 0:
        raise SubprocessError("Error calling git diff", retcode)
    r = list(tree_samples.values())
    r.extend([0] * (num_trees - len(r)))
    return r


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

    # TODO(craigds) - write some more comments explaining how it works.
    if tree1 == tree2:
        return 0

    git_rev_spec = f"{tree1.id}..{tree2.id}"
    sample_size, required_confidence, z_score = ACCURACY_PARAMS[accuracy]

    if path_encoder.DISTRIBUTED_FEATURES:
        # TODO
        max_trees = path_encoder.theoretical_max_trees
        raise NotImplementedError
    else:
        # integer PK encoder. First, find what range of trees we have
        max_tree_id = path_encoder.max_tree_id(repo, tree1, tree2)
        max_trees = max_tree_id + 1

    if sample_size >= max_trees:
        return get_exact_diff_blob_count(repo, tree1, tree2)

    sample_mean = 0
    while sample_size <= max_trees:
        L.debug(
            "sampling %d trees for dataset %s",
            sample_size,
            dataset_path,
        )
        t1 = time.monotonic()
        # Now take a sample of all trees present
        sample_tree_paths = list(
            path_encoder.sample_subtrees(sample_size, max_tree_id=max_tree_id)
        )
        samples = _feature_count_sample_trees(
            repo, git_rev_spec, sample_tree_paths, sample_size
        )
        sample_mean = statistics.mean(samples)
        sample_stdev = statistics.stdev(samples)
        t2 = time.monotonic()
        if accuracy == "veryfast":
            # Even if no features were found in the two trees, call it done.
            # This will be Good Enough if all you need to know is something like
            # "is the diff size probably less than 100K features?"
            break
        if sample_mean == 0:
            # No features were encountered in the sample.
            # This is likely quite a small diff.
            # Let's just sample a lot more trees.
            new_sample_size = min(max_trees, sample_size * 1024)
            L.debug(
                "sampled %s trees in %.3fs, found 0 features; increased sample size to %d",
                sample_size,
                t2 - t1,
                new_sample_size,
            )
            sample_size = new_sample_size
            continue
        # Try and get within 10% of the real mean.
        margin_of_error = 0.10 * sample_mean
        required_sample_size = min(
            max_trees,
            (z_score * sample_stdev / margin_of_error) ** 2,
        )
        L.debug(
            "sampled %s trees in %.3fs (ƛ=%.3f, s=%.3f). required: %.1f (margin: %.1f; confidence: %d%%)",
            sample_size,
            t2 - t1,
            sample_mean,
            sample_stdev,
            required_sample_size,
            margin_of_error * max_trees,
            required_confidence * 100,
        )
        if sample_size >= required_sample_size:
            break
        if sample_size == max_trees:
            break
        while sample_size < required_sample_size:
            sample_size *= 2
        sample_size = min(max_trees, sample_size)
    return int(round(sample_mean * max_trees))


def estimate_diff_feature_counts(
    base_rs,
    target_rs,
    *,
    working_copy=None,
    accuracy,
):
    """
    Estimates feature counts for each dataset in the given diff.
    Returns a dict (keys are dataset paths; values are feature counts)
    Datasets with (probably) no features changed are not present in the dict.
    `accuracy` should be one of ACCURACY_CHOICES
    """
    if base_rs == target_rs and not working_copy:
        return {}

    assert accuracy in ACCURACY_CHOICES
    assert base_rs.repo == target_rs.repo
    repo = base_rs.repo

    base_ds_paths = {ds.path for ds in base_rs.datasets}
    target_ds_paths = {ds.path for ds in target_rs.datasets}
    all_ds_paths = base_ds_paths | target_ds_paths

    annotation_type = f"feature-change-counts-{accuracy}"
    annotation = repo.diff_annotations.get(
        base_rs=base_rs,
        target_rs=target_rs,
        annotation_type=annotation_type,
    )
    if annotation is not None:
        return annotation

    dataset_change_counts = {}
    for dataset_path in all_ds_paths:
        base_ds = base_rs.datasets.get(dataset_path)
        target_ds = target_rs.datasets.get(dataset_path)
        if not base_ds and not target_ds:
            continue

        base_feature_tree = base_ds.feature_tree if base_ds else repo.empty_tree
        target_feature_tree = target_ds.feature_tree if target_ds else repo.empty_tree

        if accuracy == "exact" and working_copy:
            # can't really avoid this - to generate an exact count for this diff we have to generate the diff
            from kart.diff import get_dataset_diff

            ds_diff = get_dataset_diff(base_rs, target_rs, working_copy, dataset_path)
            ds_total = len(ds_diff.get("feature", []))

        elif accuracy == "exact":
            # nice, simple, no stats involved. but slow :/
            ds_total = get_exact_diff_blob_count(
                repo, base_feature_tree, target_feature_tree
            )
        else:
            path_encoder = (
                base_ds.feature_path_encoder()
                if base_ds
                else target_ds.feature_path_encoder()
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
            base_rs=base_rs,
            target_rs=target_rs,
            annotation_type=annotation_type,
            data=dataset_change_counts,
        )

    return dataset_change_counts
