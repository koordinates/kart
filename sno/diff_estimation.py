import logging
import statistics
import subprocess
import time

FEATURE_SUBTREES_PER_TREE = 256
FEATURE_TREE_NESTING = 2
MAX_TREES = FEATURE_SUBTREES_PER_TREE ** FEATURE_TREE_NESTING

L = logging.getLogger("sno.diff_estimation")
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


def _feature_count_sample_trees(rev_spec, feature_path, num_trees):
    num_full_subtrees = num_trees // 256
    paths = [f"{feature_path}{n:02x}" for n in range(num_full_subtrees)]
    paths.extend(
        [
            f"{feature_path}{num_full_subtrees:02x}/{n:02x}"
            for n in range(num_trees % 256)
        ]
    )

    p = subprocess.Popen(
        [
            "git",
            "diff",
            "--name-only",
            "--no-renames",
            rev_spec,
            "--",
            *paths,
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
    p.wait()
    r = list(tree_samples.values())
    r.extend([0] * (num_trees - len(r)))
    return r


ACCURACY_CHOICES = ("veryfast", "fast", "medium", "good", "exact")


def estimate_diff_feature_counts(
    base_rs,
    target_rs,
    working_copy,
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

    base_ds_paths = {ds.path for ds in base_rs.datasets}
    target_ds_paths = {ds.path for ds in target_rs.datasets}
    all_ds_paths = base_ds_paths | target_ds_paths
    rev_spec = f"{base_rs.tree.id}..{target_rs.tree.id}"

    dataset_change_counts = {}
    for dataset_path in all_ds_paths:
        if accuracy == "exact" and working_copy:
            # can't really avoid this - to generate an exact count for this diff we have to generate the diff
            from sno.diff import get_dataset_diff

            ds_diff = get_dataset_diff(
                base_rs,
                target_rs,
                working_copy,
                dataset_path,
            )
            if "feature" not in ds_diff:
                ds_total = 0
            else:
                ds_total = len(ds_diff["feature"])
        else:
            base_ds = base_rs.datasets.get(dataset_path)
            target_ds = target_rs.datasets.get(dataset_path)

            if not base_ds:
                base_ds, target_ds = target_ds, base_ds

            # Come up with a list of trees to diff.
            # TODO: decouple this stuff from dataset2 a bit (?)
            feature_path = f"{base_ds.path}/{base_ds.FEATURE_PATH}"
            ds_total = 0
            if (not target_ds) or base_ds.feature_tree != target_ds.feature_tree:
                if accuracy == "exact":
                    ds_total += sum(
                        _feature_count_sample_trees(rev_spec, feature_path, MAX_TREES)
                    )
                else:
                    if accuracy == "veryfast":
                        # only ever sample two trees
                        sample_size = 2
                        required_confidence = 0.00001
                        z_score = 0.0
                    else:
                        if accuracy == "fast":
                            sample_size = 2
                            required_confidence = 0.60
                        elif accuracy == "medium":
                            sample_size = 8
                            required_confidence = 0.80
                        elif accuracy == "good":
                            sample_size = 16
                            required_confidence = 0.95
                        z_score = Z_SCORES[required_confidence]

                    sample_mean = 0
                    while sample_size <= MAX_TREES:
                        L.debug(
                            "sampling %d trees for dataset %s",
                            sample_size,
                            dataset_path,
                        )
                        t1 = time.monotonic()
                        samples = _feature_count_sample_trees(
                            rev_spec, feature_path, sample_size
                        )
                        sample_mean = statistics.mean(samples)
                        sample_stdev = statistics.stdev(samples)

                        t2 = time.monotonic()
                        if accuracy == "veryfast":
                            # even if no features were found in the two trees, call it done.
                            # this will be Good Enough if all you need to know is something like
                            # "is the diff size probably less than 100K features?"
                            break
                        if sample_mean == 0:
                            # no features were encountered in the sample.
                            # this is likely quite a small diff.
                            # let's just sample a lot more trees.
                            new_sample_size = sample_size * 1024
                            if new_sample_size > MAX_TREES:
                                L.debug(
                                    "sampled %s trees in %.3fs, found 0 features; stopping",
                                    sample_size,
                                    t2 - t1,
                                )
                            else:
                                L.debug(
                                    "sampled %s trees in %.3fs, found 0 features; increased sample size to %d",
                                    sample_size,
                                    t2 - t1,
                                    new_sample_size,
                                )
                            sample_size = new_sample_size
                            continue

                        # try and get within 10% of the real mean.
                        margin_of_error = 0.10 * sample_mean
                        required_sample_size = min(
                            MAX_TREES, (z_score * sample_stdev / margin_of_error) ** 2
                        )
                        L.debug(
                            "sampled %s trees in %.3fs (ƛ=%.3f, s=%.3f). required: %.1f (margin: %.1f; confidence: %d%%)",
                            sample_size,
                            t2 - t1,
                            sample_mean,
                            sample_stdev,
                            required_sample_size,
                            margin_of_error * MAX_TREES,
                            required_confidence * 100,
                        )
                        if sample_size >= required_sample_size:
                            break

                        if sample_size == MAX_TREES:
                            break
                        while sample_size < required_sample_size:
                            sample_size *= 2
                        sample_size = min(MAX_TREES, sample_size)
                    ds_total += int(round(sample_mean * MAX_TREES))

            if working_copy:
                ds_total += working_copy.tracking_changes_count(base_ds)
        if ds_total:
            dataset_change_counts[dataset_path] = ds_total

    return dataset_change_counts
