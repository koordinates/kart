import re

import click


class _Unfiltered:
    def __getitem__(self, *args):
        # Every part of Unfiltered is also Unfiltered.
        return self

    def get(self, *args):
        # Every part of Unfiltered is also Unfiltered.
        return self

    def __setitem__(self, *args):
        pass

    def __contains__(self, *args):
        # Does this item match the filter "Unfiltered"?
        return True  # Yes.

    def add(self, *args):
        # Adding more items to this filter is a no-op.
        pass


# The result of build_feature_filter when all datasets or features should be returned.
UNFILTERED = _Unfiltered()


def build_feature_filter(feature_patterns):
    """
    Given a list of strings like ["datasetA:1", "datasetA:2", "datasetB"],
    returns a dict like {"datasetA": set(1, 2), "datasetB:" UNFILTERED}
    If no patterns are specified, returns UNFILTERED.
    """
    feature_filter = {}
    for feature_pattern in feature_patterns:
        add_to_feature_filter(feature_filter, feature_pattern)
    return feature_filter if feature_filter else UNFILTERED


_ENTIRE_DATASET_PATTERN = re.compile(r"^[^:]+$")
_SINGLE_FEATURE_PATTERN = re.compile(r"^(?P<dataset>[^:]+):(feature:)?(?P<pk>[^:]+)$")


def add_to_feature_filter(repo_filter, feature_pattern):
    for p in (_ENTIRE_DATASET_PATTERN, _SINGLE_FEATURE_PATTERN):
        match = p.match(feature_pattern)
        if match:
            break
    else:
        raise click.UsageError(
            f"Invalid format, should be <dataset> or <dataset>:<primary_key> - {feature_pattern}"
        )

    if p is _ENTIRE_DATASET_PATTERN:
        dataset = feature_pattern
        repo_filter[dataset] = UNFILTERED
    if p is _SINGLE_FEATURE_PATTERN:
        dataset = match.group("dataset")
        pk = match.group("pk")
        dataset_filter = repo_filter.setdefault(
            dataset, {"feature": set(), "meta": UNFILTERED}
        )
        dataset_filter["feature"].add(pk)
