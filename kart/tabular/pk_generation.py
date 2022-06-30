import pygit2

from .v2 import TableV2
from .v3 import TableV3
from .import_source import TableImportSource
from kart.schema import Schema, ColumnSchema


class PkGeneratingTableImportSource(TableImportSource):
    """
    Wrapper of TableImportSource that makes it appear to have a primary key, even though the delegate TableImportSource does not.
    In the simplest case, every feature encountered is just assigned a primary key from the sequence 1, 2, 3...
    However, this mapping from each feature to its primary key is stored as metadata the imported dataset, so that if
    the same (or similar) data is reimported, then the same primary keys can be assigned to each feature. The
    reimport will reuse primary keys for any features that are unchanged, and any new primary keys that must be
    assigned are appended in the same manner to this mapping.

    Note that reimporting depends only on the data to be imported and the stored primary key metadata - local edits to
    imported features have no effect on how the data is reimported.

    For the sake of efficiency, the entire feature is not stored in the mapping, but only a hash of its contents.
    Since multiple features with the same contents may be imported, the mapping to be stored has the structure:

    >>> {feature hash -> [list of primary keys]}

    In fact the inverse mapping is stored, since it has a simpler structure, primary keys are unique:

    >>> {primary key -> feature hash}

    This is stored in $DATASET_PATH/meta/generated-pks.json, along with the column-schema of the new primary key::

      {
        "primaryKeySchema": {
          "id": "ad068414-3a04-45ab-851d-bfa5104c60d6",
          "name": "auto_pk",
          "dataType": "integer",
          "primaryKeyIndex": 0,
          "size": 64
        },
        "generatedPrimaryKeys": {
          "1": "181e23cf3a3c5e74254707687c4be2b5b02dbf63",
          "2": "021ac25fcf4dafc72053f84d2b87ec5662adcb83",
          "3": "8e775122edbdd367c8d383fffeabf6580de485fd",
          ...
        }
      }

    During a reimport, if similarity_detection_limit is set to some X > 0, and the import results in a number
    of inserts + deletes[1] that is less than X, then these inserts and deletes will be searched to see if we can
    find some inserts that are similar (not identical) to the deletes. These matching new features will be
    reassigned a primary key from the delete, connecting the two features together so that that insert + delete
    will instead show up in the log as an edit.
    Currently, two features are considered "similar" if they differ by only one field, and they have only one such
    counterpart.
    Searching these inserts and deletes is computationally costly - every insert must be checked against every delete
    so each doubling of the limit can result in four times the processing time.

    [1] An insert is a feature which is not present in the previous import and so could not be reassigned an existing
    primary key, and a delete is a feature that was present in the previous import but not in the current one.
    """

    GENERATED_PKS_ITEM = "generated-pks.json"
    GENERATED_PKS_PATH = "meta/" + GENERATED_PKS_ITEM

    DEFAULT_PK_COL = {
        "id": ColumnSchema.new_id(),
        "name": "auto_pk",
        "dataType": "integer",
        "primaryKeyIndex": 0,
        "size": 64,
    }

    @classmethod
    def wrap_source_if_needed(cls, source, repo, **kwargs):
        """Wraps an TableImportSource in a PkGeneratingTableImportSource if the original data lacks a primary key."""
        return (
            source
            if source.schema.pk_columns
            else PkGeneratingTableImportSource(source, repo, **kwargs)
        )

    @classmethod
    def wrap_sources_if_needed(cls, sources, repo, **kwargs):
        """Wraps any of the given TableImportSources that lack a primary key, returns the result as a new list."""
        return [cls.wrap_source_if_needed(source, repo, **kwargs) for source in sources]

    def __init__(self, delegate, repo, *, similarity_detection_limit=0):
        self.repo = repo
        self.delegate = delegate

        # Similarity detection limit - the maximum number of (inserts + deletes) we will look through
        # to see if some of them can be paired up to make edits.
        self.similarity_detection_limit = similarity_detection_limit

        self.load_data_from_repo()
        self._schema_with_pk = Schema.from_column_dicts(
            [self.pk_col] + self.delegate.schema.to_column_dicts()
        )

    def load_data_from_repo(self):
        self.repo.ensure_supported_version()

        self.prev_dest_tree = self._prev_import_dest_tree()

        if not self.prev_dest_tree:
            self.prev_dest_dataset = None
            self.prev_dest_schema = None
            self.pk_col = self.DEFAULT_PK_COL
            self.primary_key = self.pk_col["name"]
            self.pk_to_hash = {}
            self.first_new_pk = 1

            self.similarity_detection_limit = 0
            self.similarity_detection_insert_limit = 0
            return

        self.prev_dest_dataset = self.repo.dataset_class(
            self.prev_dest_tree, self.dest_path, self.repo
        )
        self.prev_dest_schema = self.prev_dest_dataset.schema

        data = self.prev_dest_dataset.get_meta_item(self.GENERATED_PKS_ITEM)
        self.pk_col = data["primaryKeySchema"]
        self.primary_key = self.pk_col["name"]

        # The primary-key of an imported feature -> hash of feature contents, for every feature ever imported.
        self.pk_to_hash = {
            # JSON has string-keys - generated primary keys are integers.
            int(pk): feature_hash
            for pk, feature_hash in data["generatedPrimaryKeys"].items()
        }

        # First primary key to use if we can't find a historical but unassigned one.
        self.first_new_pk = max(self.pk_to_hash) + 1 if self.pk_to_hash else 1

        # The number of inserts, deletes, previous- and current-feature-count, are related by the given formula:
        # prev-FC + inserts - deletes = curr-FC
        # Since we know prev-FC and curr-FC already, we can already calculate the number of inserts we can encounter
        # before we know that (inserts + deletes) definitely exceeds the similarity_detection_limit, and once that many
        # inserts are encountered, we give up on similarity detection.
        feature_count_delta = self.feature_count - self.prev_dest_dataset.feature_count
        if abs(feature_count_delta) > self.similarity_detection_limit:
            self.similarity_detection_insert_limit = 0
        else:
            self.similarity_detection_insert_limit = (
                max(self.similarity_detection_limit + feature_count_delta, 0) // 2
            )

    def _prev_import_dest_tree(self):
        """Returns the dataset tree that was created the last time this datasource was imported."""
        if not self.repo.head_tree:
            return None

        current_pks_blob = self._get_generated_pks_blob(self.repo.head_tree)
        if current_pks_blob is None:
            return None

        prev_import_commit = None
        try:
            for commit in self.repo.walk(self.repo.head_commit.id):
                if self._get_generated_pks_blob(commit) == current_pks_blob:
                    prev_import_commit = commit
                else:
                    # We've reached the commit before the previous import
                    break

            return prev_import_commit.peel(pygit2.Tree) / self.dest_path

        except KeyError:
            # This can happen for shallow-clones - we couldn't find the last-import commit.
            #  We return the tree of the last non-import commit instead.
            # This means similarity detection works subtly differently.
            return self.repo.head_tree / self.dest_path

    def _get_generated_pks_blob(self, commit_or_tree):
        root_tree = commit_or_tree.peel(pygit2.Tree)
        try:
            dataset = self.repo.dataset_class(
                root_tree / self.dest_path, self.dest_path, self.repo
            )
            return dataset.inner_tree / self.GENERATED_PKS_PATH
        except KeyError:
            return None

    def to_dict(self):
        return {
            "primaryKeySchema": self.pk_col,
            "generatedPrimaryKeys": self.pk_to_hash,
        }

    def _invert_pk_map(self, pk_to_hash):
        result = {}
        for pk, h in pk_to_hash.items():
            result.setdefault(h, [])
            result[h].append(pk)
        return result

    def _is_schema_similar_to_last_import(self):
        if not self.prev_dest_schema:
            return False
        dt = self.prev_dest_schema.diff_type_counts(self.schema)
        if dt["pk_updates"] > 0 or dt["inserts"] > 1 or dt["deletes"] > 1:
            return False

        return True

    def features(self):
        # Next primary key to use if we can't find a historical but unassigned one in hash_to_unassigned_pks.
        next_new_pk = self.first_new_pk

        # Subset of hash_to_pks - only contains primary keys that have not yet been assigned during the current import.
        # Meaning, if we need a primary key for a feature, we should first check this dict to find a historical one that
        # hasn't yet been assigned to a feature during this import, and reassign it to the current feature.
        hash_to_unassigned_pks = self._invert_pk_map(self.pk_to_hash)

        # Features that we couldn't reassign PKs to - so far they are inserts, but if we can find some similar deletes
        # once we know the full list of inserts and deletes, then we can reassign PKs from the deletes, so that they
        # become edits.
        buffered_inserts = []
        buffered_insert_limit = self.similarity_detection_insert_limit

        if not self._is_schema_similar_to_last_import():
            buffered_insert_limit = 0

        for orig_feature in self.delegate.features():
            feature = {self.primary_key: None, **orig_feature}
            feature_hash = self.schema.hash_feature(feature, without_pk=True)

            pks = hash_to_unassigned_pks.get(feature_hash)
            reassigned_pk = pks.pop(0) if pks else None

            if reassigned_pk is not None:
                # This feature is exactly the same as a historical one that had a PK,
                # and that PK has not yet been assigned this import. We re-assign it now.
                feature[self.primary_key] = reassigned_pk
                yield feature

            elif buffered_insert_limit > 0:
                # New feature, but we don't assign it a PK just yet.
                # We buffer this feature for now - maybe we'll find a similar one from among the deleted
                # features later, which we can reuse the PK for, making this an edit.
                # We can do this once we have the full list of new and deleted features).
                buffered_inserts.append(feature)

                if len(buffered_inserts) > buffered_insert_limit:
                    # Too many inserts - give up on finding similar ones from among the deletes.
                    yield from self._assign_pk_range(buffered_inserts, next_new_pk)
                    next_new_pk += len(buffered_inserts)
                    buffered_inserts = []
                    buffered_insert_limit = 0

            else:
                # New feature. Assign it a new PK and yield it.
                yield self._assign_pk(feature, next_new_pk, feature_hash=feature_hash)
                next_new_pk += 1

        if buffered_inserts:

            # Look for matching inserts-deletes - reassign the PK from the delete, treat is as an edit:
            yield from self._match_similar_features_and_remove(
                self._find_deleted_features(hash_to_unassigned_pks), buffered_inserts
            )
            # Just assign new PKs to those we couldn't find a match for.
            yield from self._assign_pk_range(buffered_inserts, next_new_pk)

    def get_features(self, row_pks, *, ignore_missing=False):
        # we implement this so you can specifically call it with an empty
        # list of PKs.
        # this means you can use `kart import --replace-ids=` to import
        # a meta-only change with no actual feature changes, efficiently.
        # Calling it with a non-empty list of PKs raises an error.
        for pk in row_pks:
            raise NotImplementedError(
                f"{self.__class__.__name__} doesn't support retrieving features by PK"
            )
            # make this a generator, even though it's never non-empty
            yield None

    def _assign_pk_range(self, features, pk):
        for feature in features:
            yield self._assign_pk(feature, pk)
            pk += 1

    def _assign_pk(self, feature, pk, feature_hash=None):
        if feature_hash is None:
            feature_hash = self.schema.hash_feature(feature, without_pk=True)

        feature[self.primary_key] = pk
        self.pk_to_hash[pk] = feature_hash
        return feature

    def _match_similar_features_and_remove(self, old_features, new_features):
        orig_old_features_len = len(old_features)
        orig_new_features_len = len(new_features)
        similar_count = 0

        for old_feature, new_feature in self._fast_pop_similar_pairs(
            old_features, new_features
        ):
            pk = old_feature[self.primary_key]
            yield self._assign_pk(new_feature, pk)
            similar_count += 1

        assert len(old_features) == orig_old_features_len - similar_count
        assert len(new_features) == orig_new_features_len - similar_count

    def _fast_pop_similar_pairs(self, orig_old_features, orig_new_features):
        # This is a faster implementation of _pop_similar_pairs.
        # However, it hard-codes that a certain similarity metric is used -
        # where features are considered similar if they differ by no more than one (non-pk) field.
        # If a different metric is to be used, you must use _pop_similar_pairs instead, and
        # implement the metric in _is_similar.

        OLD, NEW = 0, 1
        hashes = {}

        old_features = [
            FeaturePlusHashes(f, self.prev_dest_schema) for f in orig_old_features
        ]
        new_features = [FeaturePlusHashes(f, self.schema) for f in orig_new_features]

        for f in old_features:
            for h in f.all_related_hashes():
                entry = hashes.setdefault(h, [[], []])
                entry[OLD].append(f)

        for f in new_features:
            for h in f.all_related_hashes():
                entry = hashes.get(h)
                if entry:
                    entry[NEW].append(f)

        for entry in hashes.values():
            if len(entry[OLD]) == 1 and len(entry[NEW]) == 1:
                old_feature = entry[OLD][0]
                new_feature = entry[NEW][0]
                old_feature.try_add_match(new_feature)
                new_feature.try_add_match(old_feature)

        for f in old_features:
            if f.is_uniquely_matched():
                yield f.feature, f.match.feature
                orig_old_features.remove(f.feature)
                orig_new_features.remove(f.match.feature)

    def _pop_similar_pairs(self, old_features, new_features):
        # This is just a slower version of _fast_pop_similar_pairs.
        # However, this version would work for any arbitrary implementation of _is_similar.
        # The other implementation requires a particular implementation as part of the search algorithm.

        # Copy old_features so we can remove from it while iterating over it:
        for old_feature in old_features.copy():
            new_feature = self._find_sole_similar(old_feature, new_features)
            if (
                new_feature is not None
                and self._find_sole_similar(new_feature, old_features) is not None
            ):
                old_features.remove(old_feature)
                new_features.remove(new_feature)
                yield old_feature, new_feature

    def _find_sole_similar(self, target, source_list):
        match_count = 0
        for s in source_list:
            if self._is_similar(s, target):
                match = s
                match_count += 1
                if match_count > 1:
                    break

        return match if match_count == 1 else None

    def _is_similar(self, lhs, rhs):
        # TODO - make the similarity metric configurable, if this is useful,
        # and use this slower implementation when the user has configured it.
        raise NotImplementedError(
            "Generic similarity metric not yet implemented - see _fast_pop_similar_pairs"
        )

    def _find_deleted_features(self, hash_to_unassigned_pks):
        unassigned_pks = set()
        for pks in hash_to_unassigned_pks.values():
            unassigned_pks.update(pks)

        filtered_dataset_class = {2: FilteredTableV2, 3: FilteredTableV3}[
            self.repo.table_dataset_version
        ]

        def pk_filter(pk):
            return pk in unassigned_pks

        filtered_ds = filtered_dataset_class(
            self.prev_dest_tree, self.dest_path, self.repo, pk_filter
        )
        return list(filtered_ds.features())

    def check_fully_specified(self):
        self.delegate.check_fully_specified()

    @property
    def dest_path(self):
        return self.delegate.dest_path

    @dest_path.setter
    def dest_path(self, dest_path):
        self.delegate.dest_path = dest_path

    def get_meta_item(self, name, missing_ok=True):
        if name == "schema.json":
            return self._schema_with_pk.to_column_dicts()
        elif name == self.GENERATED_PKS_ITEM:
            return self.to_dict()
        else:
            return self.delegate.get_meta_item(name, missing_ok=missing_ok)

    def meta_items(self):
        return {
            **self.delegate.meta_items(),
            "schema.json": self._schema_with_pk.to_column_dicts(),
            self.GENERATED_PKS_ITEM: self.to_dict(),
        }

    def align_schema_to_existing_schema(self, existing_schema):
        existing_schema_without_pk = Schema(existing_schema.non_pk_columns)
        return self.delegate.align_schema_to_existing_schema(existing_schema_without_pk)

    def crs_definitions(self):
        return self.delegate.crs_definitions()

    def get_crs_definition(self, identifier=None):
        return self.delegate.get_crs_definition(identifier)

    @property
    def has_geometry(self):
        return self.schema.has_geometry

    @property
    def feature_count(self):
        return self.delegate.feature_count

    @property
    def table(self):
        return self.delegate.table

    def __enter__(self):
        return self.delegate.__enter__()

    def __exit__(self, *args):
        return self.delegate.__exit__(*args)

    def __str__(self):
        return f"PkGeneratingTableImportSource({self.delegate})"

    def import_source_desc(self):
        return self.delegate.import_source_desc()

    def aggregate_import_source_desc(self, import_sources):
        return self.delegate.aggregate_import_source_desc(import_sources)


class FilteredTableDataset:
    """A dataset that only yields features with pk where `pk_filter(pk)` returns True."""

    def __init__(self, tree, path, repo, pk_filter):
        super().__init__(tree, path, repo)
        self.pk_filter = pk_filter

    def feature_blobs(self):
        for blob in super().feature_blobs():
            pk = self.decode_path_to_1pk(blob.name)
            if self.pk_filter(pk):
                yield blob


class FilteredTableV3(FilteredTableDataset, TableV3):
    pass


class FilteredTableV2(FilteredTableDataset, TableV2):
    pass


class FeaturePlusHashes:
    _MORE_THAN_ONE_MATCH = object()

    __slots__ = ["feature", "schema", "match"]

    def __init__(self, feature, schema):
        self.feature = feature
        self.schema = schema
        self.match = None

    def __hash__(self):
        return id(self)

    def __eq__(self, other):
        return other is self

    def all_related_hashes(self):
        value_hashes = []
        feature_hash = 0
        # Generate the overall hash of the feature, but in separate pieces.
        for col in self.schema.non_pk_columns:
            value = self.feature[col.name]
            value_hash = hash((col.id, value))
            feature_hash ^= value_hash
            value_hashes.append(value_hash)

        # Add the feature at its overall hash.
        yield feature_hash
        # Remove each piece one at a time to make a hash for each variation of the feature missing exactly one field.
        for value_hash in value_hashes:
            yield feature_hash ^ value_hash

    def try_add_match(self, other):
        if self.match is None:
            self.match = other
        elif self.match == other:
            pass
        else:
            self.match = self._MORE_THAN_ONE_MATCH

    def is_uniquely_matched(self):
        return (
            self.match is not None
            and self.match is not self._MORE_THAN_ONE_MATCH
            and self.match.match is self
        )
