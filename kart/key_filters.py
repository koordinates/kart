import re

import click

from .diff_structs import RichDict

# The following filters all apply to "keys", not to "values" - so they apply to meta item names or primary-key-values -
# since in Kart, the primary-key-value is the name of the feature, which can be known and filtered without loading the
# entire feature blob.
# This is to contrast these filters with "value" filters which would filter out features (or meta items) based not on
# their name, but on what they contain - such as spatial filters for feature geometries.


class UserStringKeyFilter(set):
    """
    A key filter that, given primary key values or similar,
    matches them against a set of strings the user has supplied.
    """

    def __init__(self, *args, match_all=False, **kwargs):
        super().__init__(*args, **kwargs)
        self.match_all = match_all

    def __contains__(self, key):
        if self.match_all:
            return True

        if isinstance(key, (tuple, list)):
            if len(key) == 1:
                key = str(key[0])
            else:
                key = ",".join(str(k) for k in key)
        else:
            key = str(key)
        return super().__contains__(key)

    def add(self, key):
        if not self.match_all:
            super().add(key)


UserStringKeyFilter.MATCH_ALL = UserStringKeyFilter(match_all=True)

# Aliases so that FeatureKeyFilter.MATCH_ALL works, which is a bit easier to remember.
MetaKeyFilter = UserStringKeyFilter
FeatureKeyFilter = UserStringKeyFilter


class KeyFilterDict(RichDict):
    """
    Abstract base class for DatasetKeyFilter and RepoKeyFilter.
    A RichDict that can match all - and if it does, appears to contain a child value
    at any/all keys, and that child also matches all.
    """

    def __init__(self, *args, match_all=False, **kwargs):
        super().__init__(*args, **kwargs)
        self.match_all = match_all

    def __contains__(self, key):
        return self.match_all or super().__contains__(key)

    def __getitem__(self, key):
        return (
            self.child_that_matches_all if self.match_all else super().__getitem__(key)
        )

    def get(self, key, default_value=None):
        return (
            self.child_that_matches_all
            if self.match_all
            else super().get(key, default_value)
        )

    def __setitem__(self, key, value):
        if not self.match_all:
            super().__setitem__(key, value)


class DatasetKeyFilter(KeyFilterDict):
    """
    A dict with the structure:
    {
        "meta": UserStringKeyFilter, "feature": UserStringKeyFilter}
    }
    for filtering meta items and features (although meta item filtering is not yet implemented).
    """

    child_type = UserStringKeyFilter
    child_that_matches_all = UserStringKeyFilter(match_all=True)


DatasetKeyFilter.MATCH_ALL = DatasetKeyFilter(match_all=True)


class RepoKeyFilter(KeyFilterDict):
    """
    A dict with the structure:
    {
        "dataset_path": DatasetKeyFilter, ...
    }
    for filtering items in any or all datasets.
    """

    child_type = DatasetKeyFilter
    child_that_matches_all = DatasetKeyFilter(match_all=True)

    _ENTIRE_DATASET_PATTERN = re.compile(r"^[^:]+$")
    _SINGLE_FEATURE_PATTERN = re.compile(
        r"^(?P<dataset>[^:]+):(feature:)?(?P<pk>[^:]+)$"
    )

    @classmethod
    def build_from_user_patterns(cls, user_patterns, implicit_meta=True):
        """
        Given a list of strings like ["datasetA:1", "datasetA:2", "datasetB"],
        builds a RepoKeyFilter with the appropriate entries for "datasetA" and "datasetB".
        If no patterns are specified, returns RepoKeyFilter.MATCH_ALL.
        If implicit_meta is True, then all meta changes are matched as soon as any feature changes are requested.
        """
        result = cls()
        for user_pattern in user_patterns:
            result.add_user_pattern(user_pattern, implicit_meta=implicit_meta)
        return result if result else cls.MATCH_ALL

    def add_user_pattern(self, user_pattern, implicit_meta=True):
        for p in (self._ENTIRE_DATASET_PATTERN, self._SINGLE_FEATURE_PATTERN):
            match = p.match(user_pattern)
            if match:
                break
        else:
            raise click.UsageError(
                f"Invalid filter format, should be <dataset> or <dataset>:<primary_key> - {user_pattern}"
            )

        if p is self._ENTIRE_DATASET_PATTERN:
            ds_path = user_pattern
            self[ds_path] = DatasetKeyFilter.MATCH_ALL

        if p is self._SINGLE_FEATURE_PATTERN:
            ds_path = match.group("dataset")
            pk = match.group("pk")

            ds_filter = self.get(ds_path)
            if not ds_filter:
                ds_filter = DatasetKeyFilter()
                if implicit_meta:
                    ds_filter["meta"] = UserStringKeyFilter.MATCH_ALL
                ds_filter["feature"] = UserStringKeyFilter()
                self[ds_path] = ds_filter

            ds_filter["feature"].add(pk)


RepoKeyFilter.MATCH_ALL = RepoKeyFilter(match_all=True)
