import fnmatch
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
TileKeyFilter = UserStringKeyFilter


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
    A dict with the structure something like the following:
    {
        "meta": UserStringKeyFilter, "feature": UserStringKeyFilter}
    }
    for filtering meta items, features, tiles etc.
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

    # https://github.com/koordinates/kart/blob/master/docs/DATASETS_v3.md#valid-dataset-names
    # note: we allow '*' here; it's not a valid dataset name character but it's used in the filter
    # pattern.
    FILTER_PATTERN = re.compile(
        # dataset part
        r'^(?P<dataset_glob>[^:<>"|?\x00-\x1f]+)'
        # optional sub-dataset part. This is optional; if a PK is given and ':feature' isn't, we assume feature anyway.
        # (i.e. 'datasetname:123' is equivalent to 'datasetname:feature:123'
        r"(?::(?P<subdataset>feature|meta|tile))?"
        # The rest of the pattern is either a meta key, a PK or a tilename
        r"(?::(?P<rest>.*))?"
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._dataset_glob_filters = {}

    def _bad_pattern(self, user_pattern):
        return click.UsageError(
            f"Invalid filter format, should be '<dataset>' or '<dataset>:<primary_key>': got {user_pattern!r}"
        )

    def _parse_user_pattern(self, user_pattern):
        match = self.FILTER_PATTERN.match(user_pattern)
        if not match:
            raise self._bad_pattern(user_pattern)
        groups = match.groupdict()
        dataset_glob = groups["dataset_glob"]
        if (
            dataset_glob.startswith(("/", "."))
            or dataset_glob.endswith(("/", "."))
            or "./" in dataset_glob
            or "/." in dataset_glob
        ):
            raise self._bad_pattern(user_pattern)

        subdataset = groups["subdataset"]
        if not subdataset:
            if groups["rest"]:
                # TODO - make this a bit smarter - this should default to "tile" for a tile-dataset.
                subdataset = "feature"

        return groups["dataset_glob"], subdataset, groups["rest"] or None

    @classmethod
    def build_from_user_patterns(cls, user_patterns):
        """
        Given a list of strings like ["datasetA:1", "datasetA:2", "datasetB"],
        builds a RepoKeyFilter with the appropriate entries for "datasetA" and "datasetB".
        If no patterns are specified, returns RepoKeyFilter.MATCH_ALL.
        """
        result = cls()
        for user_pattern in user_patterns:
            result.add_user_pattern(user_pattern)
        return result if result else cls.MATCH_ALL

    def add_user_pattern(self, user_pattern):
        dataset_glob, subdataset, rest = self._parse_user_pattern(user_pattern)

        if subdataset is None:
            # whole dataset
            self[dataset_glob] = DatasetKeyFilter.MATCH_ALL
            return
        # Either a meta or feature filter
        ds_filter = self.get(dataset_glob)
        if not ds_filter:
            ds_filter = DatasetKeyFilter()
            if rest:
                # Specific feature, tile or meta item
                ds_filter[subdataset] = UserStringKeyFilter()
            else:
                # All features, or all meta items
                ds_filter[subdataset] = UserStringKeyFilter.MATCH_ALL
            self[dataset_glob] = ds_filter
        ds_filter[subdataset].add(rest)

    def _dataset_glob_pattern_matching_key(self, key):
        if self._dataset_glob_filters:
            for glob_pattern in self._dataset_glob_filters.keys():
                if fnmatch.fnmatch(key, glob_pattern):
                    return glob_pattern
        return False

    def filter_keys(self, keys: set):
        matched_keys = keys & self.keys()
        matched_keys.update(
            {
                k
                for k in keys - matched_keys
                if self._dataset_glob_pattern_matching_key(k)
            }
        )
        return matched_keys

    def __contains__(self, key):
        return super().__contains__(key) or self._dataset_glob_pattern_matching_key(key)

    def __getitem__(self, key):
        if self.match_all:
            return self.child_that_matches_all
        try:
            return super().__getitem__(key)
        except KeyError:
            glob_pattern = self._dataset_glob_pattern_matching_key(key)
            if not glob_pattern:
                raise
            return self._dataset_glob_filters[glob_pattern]

    def get(self, key, default_value=None):
        try:
            return self[key]
        except KeyError:
            return default_value

    def __setitem__(self, key, value):
        if self.match_all:
            return
        if "*" in key:
            # escape the glob for passing to fnmatch later.
            # This is because fnmatch actually processes '*?[]' chars specially, but we only want to support '*' for now.
            for char in "?[]":
                key = key.replace(char, f"[{char}]")
            self._dataset_glob_filters[key] = value
        super().__setitem__(key, value)


RepoKeyFilter.MATCH_ALL = RepoKeyFilter(match_all=True)
