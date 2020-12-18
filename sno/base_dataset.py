import functools
import logging

from .import_source import ImportSource


class BaseDataset(ImportSource):
    """
    Common interface for all datasets - so far this is Dataset1, Dataset2 -
    and even Dataset0 (but this is only supported for `sno upgrade`)
    """

    # Constants that subclasses should generally define.

    VERSION = None  # Version eg 1
    DATASET_DIRNAME = None  # Eg ".sno-dataset"
    DATASET_PATH = None  # Eg ".sno-dataset/"

    META_PATH = None  # Eg ".sno-dataset/meta/"
    FEATURE_PATH = None  # Eg ".sno-dataset/feature/"

    def __init__(self, tree, path):
        """
        Create a dataset at the given tree, which has the given path.
        The tree should contain a sno directory eg ".sno-table" or ".sno-dataset".
        The tree can be None if this dataset hasn't yet been written to the repo.
        """
        if self.__class__ is BaseDataset:
            raise TypeError(
                "Cannot construct a BaseDataset - use a subclass (see BaseDataset.for_version)"
            )

        self.tree = tree
        self.path = path.strip("/")
        self.table_name = self.path.replace("/", "__")
        self.L = logging.getLogger(self.__class__.__qualname__)

    def __repr__(self):
        return f"<{self.__class__.__name__}: {self.path}>"

    @classmethod
    def for_version(cls, version):
        from .dataset1 import Dataset1
        from .dataset2 import Dataset2

        version = int(version)
        if version == 1:
            return Dataset1
        elif version == 2:
            return Dataset2

        raise ValueError(f"No Dataset implementation found for version={version}")

    def default_dest_path(self):
        # ImportSource method - by default, a dataset should import with the same path it already has.
        return self.path

    @classmethod
    def is_dataset_tree(cls, tree):
        """
        Returns True if the given tree seems to contain a dataset of this type.
        Used for finding all the datasets in a given repo, etc.
        """
        return (
            tree is not None
            and cls.DATASET_DIRNAME in tree
            and (tree / cls.DATASET_DIRNAME).type_str == "tree"
        )

    @property
    @functools.lru_cache(maxsize=1)
    def meta_tree(self):
        """Returns the root of the meta tree. Caller take care: will fail if no meta tree exists."""
        return self.tree / self.META_PATH

    @property
    @functools.lru_cache(maxsize=1)
    def feature_tree(self):
        """Returns the root of the feature tree. Caller take care: fails if no feature tree exists."""
        return self.tree / self.FEATURE_PATH

    def get_data_at(self, rel_path, as_memoryview=False, missing_ok=False):
        """
        Return the data at the given relative path from within this dataset.

        Data is usually returned as a bytestring.
        If as_memoryview=True is given, data is returned as a memoryview instead
        (this avoids a copy, so can make loops more efficient for many rows)

        If missing_ok is true, we return None instead of raising a KeyError for
        missing data.
        """
        leaf = None
        try:
            leaf = self.tree / str(rel_path)
        except KeyError:
            pass

        if leaf is not None and leaf.type_str == "blob":
            if as_memoryview:
                try:
                    return memoryview(leaf)
                except TypeError:
                    pass
            else:
                try:
                    return leaf.data
                except AttributeError:
                    pass

        # If we got here, that means leaf wasn't a blob, or one of the above
        # exceptions happened...
        if missing_ok:
            return None
        else:
            raise KeyError(f"No data found at rel-path {rel_path}, type={type(leaf)}")

    def full_path(self, rel_path):
        """Given a path relative to this dataset, returns its full path from the repo root."""
        return f"{self.path}/{rel_path}"

    def rel_path(self, full_path):
        """Given a full path to something in this dataset, returns its path relative to the dataset."""
        if not full_path.startswith(f"{self.path}/"):
            raise ValueError(f"{full_path} is not a descendant of {self.path}")
        return full_path[len(self.path) + 1 :]

    def ensure_rel_path(self, path):
        """Given either a relative path or a full path, return the equivalent relative path."""
        if path.startswith(self.DATASET_PATH):
            return path
        return self.rel_path(path)

    def decode_path(self, rel_path):
        """
        Given a path in this layer of the sno repository - eg ".sno-dataset/49/3e/Bg==" -
        returns a tuple in either of the following forms:
        1. ("feature", primary_key)
        2. ("meta", metadata_file_path)
        """
        if rel_path.startswith(self.DATASET_PATH):
            rel_path = rel_path[len(self.DATASET_PATH) :]
        if rel_path.startswith("meta/"):
            return ("meta", rel_path[len("meta/") :])
        pk = self.decode_path_to_1pk(rel_path)
        return ("feature", pk)

    @functools.lru_cache()
    def get_meta_item(self, name):
        """Finds or generates the meta item with the given name, according to the V2 spec."""
        raise NotImplementedError()

    @functools.lru_cache()
    def get_gpkg_meta_item(self, name):
        """Finds or generates a gpkg meta item with the given name, according to the GPKG / V1 spec."""
        raise NotImplementedError()

    @property
    @functools.lru_cache(maxsize=1)
    def primary_key(self):
        """Returns the name of the primary key column."""
        # TODO - adapt this interface when we support more than one primary key.
        if len(self.schema.pk_columns) == 1:
            return self.schema.pk_columns[0].name
        raise ValueError(f"No single primary key: {self.schema.pk_columns}")

    @property
    @functools.lru_cache(maxsize=1)
    def has_geometry(self):
        return self.geom_column_name is not None

    @property
    @functools.lru_cache(maxsize=1)
    def geom_column_name(self):
        geom_columns = self.schema.geometry_columns
        return geom_columns[0].name if geom_columns else None

    def features(self):
        """
        Yields a dict for every feature. Dicts contain key-value pairs for each feature property,
        and geometries use sno.geometry.Geometry objects, as in the following example::

        {
            "fid": 123,
            "geom": Geometry(b"..."),
            "name": "..."
            "last-modified": "..."
        }

        Each dict is guaranteed to iterate in the same order as the columns are ordered in the schema,
        so that zip(schema.columns, feature.values()) matches each field with its column.
        """
        for blob in self.feature_blobs():
            yield self.get_feature(path=blob.name, data=memoryview(blob))

    @property
    def feature_count(self):
        """The total number of features in this dataset."""
        return sum(1 for blob in self.feature_blobs())

    def get_features(self, row_pks, *, ignore_missing=False):
        """
        Yields a dict for each of the specified features.
        If ignore_missing is True, then failing to find a specified feature does not raise a KeyError.
        """
        for pk_values in row_pks:
            try:
                yield self.get_feature(pk_values)
            except KeyError:
                if ignore_missing:
                    continue
                else:
                    raise

    def get_feature(self, pk_values=None, *, path=None, data=None):
        """
        Return the feature with the given primary-key value(s).
        A single feature will be returned - multiple pk_values should only be supplied if there are multiple pk columns.

        The caller must supply at least one of (pk_values, path) so we know which feature is meant. We can infer
        whichever one is missing from the one supplied. If the caller knows both already, they can supply both, to avoid
        redundant work. Similarly, if the caller knows data, they can supply that too to avoid redundant work.
        """
        raise NotImplementedError()


class IntegrityError(ValueError):
    pass
