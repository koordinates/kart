import functools
import logging

from . import crs_util
from .import_source import ImportSource
from . import meta_items
from .serialise_util import json_unpack
from .utils import ungenerator


class BaseDataset(ImportSource):
    """
    Common interface for all datasets - mostly used by Dataset3,
    but also implemented by legacy datasets ie during `kart upgrade`.

    A Dataset instance is immutable since it is a view of a particular git tree.
    To get a new version of a dataset, commit the desired changes,
    then instantiate a new Dataset instance that references the new git tree.

    A dataset has a user-defined path eg `path/to/dataset`, and inside that it
    has a  hidden folder with a special name - cls.DATASET_DIRNAME.
    The path to this folder is the inner_path: `path/to/dataset/DATASET_DIRNAME`.
    Similarly, a dataset's tree is the tree at `path/to/dataset`,
    and its inner_tree is the tree at `path/to/dataset/DATASET_DIRNAME`.

    All relative paths are defined as being relative to the inner_path / inner_tree.
    """

    # Constants that subclasses should generally define.

    VERSION = None  # Version eg 1
    DATASET_DIRNAME = None  # Eg ".table-dataset" or ".sno-dataset"

    # Paths are all defined relative to the inner path -

    META_PATH = None  # Eg "meta/"
    FEATURE_PATH = None  # Eg "feature/"

    META_ITEM_NAMES = meta_items.META_ITEM_NAMES

    def __init__(self, tree, path, repo=None):
        """
        Create a dataset at the given tree, which has the given path.
        The tree should contain a child tree with the name DATASET_DIRNAME.
        The tree can be None if this dataset hasn't yet been written to the repo.
        """
        if self.__class__ is BaseDataset:
            raise TypeError("Cannot construct a BaseDataset - you may want Dataset3")

        if tree is not None:
            self.tree = tree
            self.inner_tree = (
                tree / self.DATASET_DIRNAME if self.DATASET_DIRNAME else self.tree
            )
        else:
            self.inner_tree = self.tree = None

        self.path = path.strip("/")
        self.inner_path = (
            f"{path}/{self.DATASET_DIRNAME}" if self.DATASET_DIRNAME else self.path
        )

        self.table_name = self.path.replace("/", "__")
        self.L = logging.getLogger(self.__class__.__qualname__)

        self.repo = repo

    @classmethod
    def new_dataset_for_writing(cls, path, schema, repo=None):
        result = cls(None, path, repo=repo)
        result._original_schema = schema
        return result

    def __repr__(self):
        return f"<{self.__class__.__name__}: {self.path}>"

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
        """Returns the root of the meta tree, or the empty tree if no meta tree exists."""
        try:
            return self.inner_tree / self.META_PATH
        except KeyError:
            return self.repo.empty_tree if self.repo else None

    @property
    def attachment_tree(self):
        return self.tree

    @property
    @functools.lru_cache(maxsize=1)
    def feature_tree(self):
        """Returns the root of the feature tree, or the empty tree if no meta tree exists."""
        try:
            return (
                self.inner_tree / self.FEATURE_PATH
                if self.FEATURE_PATH
                else self.inner_tree
            )
        except KeyError:
            return self.repo.empty_tree if self.repo else None

    def get_data_at(self, rel_path, as_memoryview=False, missing_ok=False, tree=None):
        """
        Return the data at the given relative path from within this dataset.

        Data is usually returned as a bytestring.
        If as_memoryview=True is given, data is returned as a memoryview instead
        (this avoids a copy, so can make loops more efficient for many rows)

        If missing_ok is true, we return None instead of raising a KeyError for
        missing data.

        If tree is set, the caller can override the tree in which to look for the data.
        """
        leaf = None
        tree = tree or self.inner_tree
        try:
            leaf = tree / str(rel_path)
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

    def get_json_data_at(self, rel_path, missing_ok=False):
        data = self.get_data_at(rel_path, missing_ok=missing_ok)
        return json_unpack(data) if data is not None else None

    def full_path(self, rel_path):
        """Given a path relative to this dataset, returns its full path from the repo root."""
        return f"{self.inner_path}/{rel_path}"

    def full_attachment_path(self, rel_path):
        """Given the path of an attachment relative to this dataset's attachment path, returns its full path from the repo root."""
        return f"{self.path}/{rel_path}"

    def rel_path(self, full_path):
        """Given a full path to something in this dataset, returns its path relative to the dataset."""
        if not full_path.startswith(f"{self.inner_path}/"):
            raise ValueError(f"{full_path} is not a descendant of {self.inner_path}")
        return full_path[len(self.inner_path) + 1 :]

    def ensure_rel_path(self, path):
        """Given either a relative path or a full path, return the equivalent relative path."""
        if path.startswith(self.inner_path):
            return self.rel_path(path)
        return path

    def decode_path(self, rel_path):
        """
        Given a relative path to something inside this dataset
        eg "[DATASET_DIRNAME]/feature/49/3e/Bg==", or simply "feature/49/3e/Bg==""
        returns a tuple in either of the following forms:
        1. ("feature", primary_key)
        2. ("meta", metadata_file_path)
        """
        if self.DATASET_DIRNAME and rel_path.startswith(f"{self.DATASET_DIRNAME}/"):
            rel_path = rel_path[len(self.DATASET_DIRNAME) + 1 :]
        if rel_path.startswith("meta/"):
            return ("meta", rel_path[len("meta/") :])
        pk = self.decode_path_to_1pk(rel_path)
        return ("feature", pk)

    @functools.lru_cache()
    def get_meta_item(self, name, missing_ok=True):
        """Loads a meta item stored in the meta tree."""
        rel_path = self.META_PATH + name
        data = self.get_data_at(rel_path, missing_ok=missing_ok)
        if data is None:
            return data

        if rel_path.endswith(".json"):
            return json_unpack(data)
        elif rel_path.endswith(".wkt"):
            return crs_util.normalise_wkt(ensure_text(data))
        else:
            return ensure_text(data)

    @functools.lru_cache()
    @ungenerator(dict)
    def meta_items(self, only_standard_items=True):
        if not self.meta_tree:
            return

        for name in self.META_ITEM_NAMES:
            value = self.get_meta_item(name)
            if value:
                yield name, value

        for identifier, definition in self.crs_definitions().items():
            yield f"crs/{identifier}.wkt", definition

        if not only_standard_items:
            yield from self.extra_meta_items()

    @functools.lru_cache()
    @ungenerator(dict)
    def extra_meta_items(self):
        if not self.meta_tree:
            return

        extra_names = [obj.name for obj in self.meta_tree if obj.type_str == "blob"]
        for name in sorted(extra_names):
            yield name, self.get_meta_item(name)

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
        and geometries use kart.geometry.Geometry objects, as in the following example::

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

    def align_schema_to_existing_schema(self, existing_schema):
        raise RuntimeError(
            "Dataset object is immutable, aligning schema is not supported"
        )


class IntegrityError(ValueError):
    pass
