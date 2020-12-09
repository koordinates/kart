import functools
from .serialise_util import (
    json_pack,
    json_unpack,
)
from collections.abc import Iterable
from .schema import ColumnSchema, Schema


class PkGeneratingImportSource:
    """
    Wrapper of ImportSource that makes it appear to have a primary key, even though the delegate ImportSource does not.
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
          "name": "generated-pk",
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

    """

    GENERATED_PKS_PATH = ".sno-dataset/meta/generated-pks.json"

    DEFAULT_PK_COL = {
        "id": ColumnSchema.new_id(),
        "name": "generated-pk",
        "dataType": "integer",
        "primaryKeyIndex": 0,
        "size": 64,
    }

    @classmethod
    def wrap_if_needed(cls, source_or_sources, repo):
        """
        Wraps an ImportSource in a PkGeneratingImportSource if the original data lacks a primary key.
        If multiple ImportSources are supplied, wraps those that lack a primary key and returns a new list.
        """
        if isinstance(source_or_sources, Iterable):
            sources = source_or_sources
            return [cls.wrap_if_needed(s, repo) for s in sources]

        source = source_or_sources
        if not source.schema.pk_columns:
            return PkGeneratingImportSource(source, repo)
        else:
            return source

    def __init__(self, delegate, repo):
        self.delegate = delegate
        self.load_data_from_repo(repo)

    def load_data_from_repo(self, repo):
        tree = repo.head_tree
        generated_pks_blob = None

        if tree is not None:
            try:
                generated_pks_blob = tree / self.dest_path / self.GENERATED_PKS_PATH
            except KeyError:
                pass

        if not generated_pks_blob:
            self.pk_col = self.DEFAULT_PK_COL
            self.primary_key = self.pk_col["name"]
            self.pk_to_hash = {}
            self.hash_to_pks = {}
            self.hash_to_unassigned_pks = {}
            self.next_new_pk = 1
            return

        data = json_unpack(generated_pks_blob.data)
        self.pk_col = data["primaryKeySchema"]
        self.primary_key = self.pk_col["name"]

        # The primary-key of an imported feature -> hash of feature contents, for every feature ever imported.
        self.pk_to_hash = {
            # JSON has string-keys - generated primary keys are integers.
            int(pk): feature_hash
            for pk, feature_hash in data["generatedPrimaryKeys"].items()
        }

        # Hash of feature contents -> primary key(s) of imported feature(s), for every feature ever imported.
        self.hash_to_pks = self._invert_pk_map(self.pk_to_hash)

        # Subset of hash_to_pks - only contains primary keys that have not yet been assigned during the current import.
        # Meaning, if we need a primary key for a feature, we should first check this dict to find a historical one that
        # hasn't yet been assigned to a feature during this import.
        self.hash_to_unassigned_pks = self._invert_pk_map(self.pk_to_hash)

        # Next primary key to use if we can't find a historical but unassigned one in hash_to_unassigned_pks.
        self.next_new_pk = max(self.pk_to_hash) + 1 if self.pk_to_hash else 1

    def encode_generated_pk_data(self, relative=False):
        path = self.GENERATED_PKS_PATH
        if not relative:
            path = "/".join(self.dest_path, self.GENERATED_PKS_PATH)

        data = {
            "primaryKeySchema": self.pk_col,
            "generatedPrimaryKeys": self.pk_to_hash,
        }

        return path, json_pack(data)

    def _invert_pk_map(self, pk_to_hash):
        result = {}
        for pk, h in pk_to_hash.items():
            result.setdefault(h, [])
            result[h].append(pk)
        return result

    @property
    @functools.lru_cache(maxsize=1)
    def schema(self):
        cols = self.delegate.schema.to_column_dicts()
        return Schema.from_column_dicts([self.pk_col] + cols)

    def features(self):
        schema = self.schema
        for feature in self.delegate.features():
            feature[self.primary_key] = None
            feature_hash = schema.hash_feature(feature, without_pk=True)
            feature[self.primary_key] = self.generate_pk(feature_hash)
            yield feature

    def generate_pk(self, feature_hash):
        unused_pks = self.hash_to_unassigned_pks.get(feature_hash)
        if unused_pks:
            return unused_pks.pop(0)

        pk = self.next_new_pk
        self.next_new_pk += 1

        self.hash_to_pks.setdefault(feature_hash, [])
        self.hash_to_pks[feature_hash].append(pk)
        self.pk_to_hash[pk] = feature_hash
        return pk

    def check_fully_specified(self):
        self.delegate.check_fully_specified()

    @property
    def dest_path(self):
        return self.delegate.dest_path

    @dest_path.setter
    def dest_path(self, dest_path):
        self.delegate.dest_path = dest_path

    def get_meta_item(self, name):
        return self.delegate.get_meta_item(name)

    def meta_items(self):
        yield from self.delegate.meta_items()

    def crs_definitions(self):
        yield from self.delegate.crs_definitions()

    def get_crs_definition(self, identifier=None):
        return self.delegate.get_crs_definition(identifier)

    @property
    def has_geometry(self):
        return self.schema.has_geometry

    @property
    def feature_count(self):
        return self.delegate.feature_count

    def __enter__(self):
        return self.delegate.__enter__()

    def __exit__(self, *args):
        return self.delegate.__exit__(*args)

    def __str__(self):
        return f"PkGeneratingImportSource({self.__class__.__name__})"

    def import_source_desc(self):
        return self.delegate.import_source_desc()

    def aggregate_import_source_desc(self, import_sources):
        return self.delegate.aggregate_import_source_desc(import_sources)
