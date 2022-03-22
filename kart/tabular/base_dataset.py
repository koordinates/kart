import functools
import logging
import time

from kart import crs_util
from kart.promisor_utils import LibgitSubcode
from kart.serialise_util import ensure_text, json_unpack
from kart.spatial_filter import SpatialFilter
from kart.utils import ungenerator

from . import meta_items
from .import_source import ImportSource

L = logging.getLogger("kart.tabular.base_dataset")


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
    DATASET_TYPE = "table"

    # Paths are all defined relative to the inner path -

    META_PATH = None  # Eg "meta/"
    FEATURE_PATH = None  # Eg "feature/"

    META_ITEM_NAMES = meta_items.META_ITEM_NAMES

    NUM_FEATURES_PER_PROGRESS_LOG = 10_000

    def __init__(self, tree, path, dirname=None, repo=None):
        """
        Create a dataset at the given tree, which has the given path.
        The tree should contain a child tree with the name DATASET_DIRNAME.
        The tree can be None if this dataset hasn't yet been written to the repo.
        """
        if self.__class__ is BaseDataset:
            raise TypeError("Cannot construct a BaseDataset - you may want Dataset3")

        if dirname is None:
            dirname = self.DATASET_DIRNAME

        if tree is not None:
            self.tree = tree
            self.inner_tree = tree / dirname if dirname else self.tree
        else:
            self.inner_tree = self.tree = None

        self.path = path.strip("/")
        self.inner_path = f"{path}/{dirname}" if dirname else self.path

        self.table_name = self.dataset_path_to_table_name(self.path)
        self.L = logging.getLogger(self.__class__.__qualname__)

        self.repo = repo

    @classmethod
    def new_dataset_for_writing(cls, path, schema, repo=None):
        result = cls(None, path, cls.DATASET_DIRNAME, repo=repo)
        result._schema = schema
        return result

    @classmethod
    def dataset_path_to_table_name(cls, ds_path):
        return ds_path.strip("/").replace("/", "__")

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
        """Returns the root of the feature tree, or the empty tree if no feature tree exists."""
        try:
            return (
                self.inner_tree / self.FEATURE_PATH
                if self.FEATURE_PATH
                else self.inner_tree
            )
        except KeyError:
            return self.repo.empty_tree if self.repo else None

    @property
    def schema(self):
        if not hasattr(self, "_schema"):
            self._schema = super().schema
        return self._schema

    def get_blob_at(self, rel_path, missing_ok=False, tree=None):
        """
        Return the blob at the given relative path from within this dataset.

        If missing_ok is true, we return None instead of raising a KeyError for
        missing data.

        If tree is set, the caller can override the tree in which to look for the data.
        """
        leaf = None
        key_error = None
        tree = tree or self.inner_tree
        try:
            leaf = tree / str(rel_path)
            if leaf is not None and leaf.type_str == "blob":
                return leaf
        except KeyError as e:
            key_error = e
        except (AttributeError, TypeError):
            pass

        # If we got here, that means leaf wasn't a blob, or one of the above
        # exceptions happened...
        if missing_ok:
            return None
        else:
            detail = f": {key_error.args[0]}" if key_error is not None else ""
            k = KeyError(
                f"No data found at rel-path {rel_path}, type={type(leaf)}{detail}"
            )
            if hasattr(key_error, "subcode"):
                k.subcode = key_error.subcode
            elif key_error is not None and leaf is None:
                # TODO - remove this clause once windows also has libgit subcodes.
                k.subcode = LibgitSubcode.ENOSUCHPATH
            raise k

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
        blob = self.get_blob_at(rel_path, missing_ok=missing_ok, tree=tree)
        if blob is not None:
            return memoryview(blob) if as_memoryview else blob.data
        return None

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
            yield from self.extra_meta_items().items()

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

    def features(self, spatial_filter=SpatialFilter.MATCH_ALL, log_progress=False):
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

        spatial_filter - restricts the features yielded to those that are in a particular geographic area.
        log_progress - can be set to True, or to a callable logger method eg L.info, to enable logging.
        """
        if log_progress:
            plog = L.info if log_progress is True else log_progress
            log_progress = bool(log_progress)

        spatial_filter = spatial_filter.transform_for_dataset(self)

        n_read = 0
        n_chunk = 0
        n_matched = 0
        n_total = self.feature_count
        t0 = time.monotonic()
        t0_chunk = t0

        if log_progress:
            plog("0.0%% 0/%d features... @0.0s", n_total)

        for blob in self.feature_blobs():
            n_read += 1
            n_chunk += 1
            try:
                feature = self.get_feature_from_blob(blob)
            except KeyError as e:
                if spatial_filter.feature_is_prefiltered(e):
                    feature = None
                else:
                    raise

            if feature is not None and spatial_filter.matches(feature):
                n_matched += 1
                yield feature

            if log_progress and n_chunk == self.NUM_FEATURES_PER_PROGRESS_LOG:
                t = time.monotonic()
                self._log_feature_progress(
                    plog, n_read, n_chunk, n_matched, n_total, t0, t0_chunk, t
                )
                t0_chunk = t
                n_chunk = 0

        if log_progress and n_total:
            t = time.monotonic()
            self._log_feature_progress(
                plog, n_read, n_chunk, n_matched, n_total, t0, t0_chunk, t
            )
            plog("Overall rate: %d features/s", (n_read / (t - t0 or 0.001)))

    def _log_feature_progress(
        self, plog, num_read, num_chunk, num_matched, num_total, t0, t0_chunk, t
    ):
        plog(
            "%.1f%% %d/%d features... @%.1fs (+%.1fs, ~%d F/s)",
            num_read / num_total * 100,
            num_read,
            num_total,
            t - t0,
            t - t0_chunk,
            num_chunk / (t - t0_chunk or 0.001),
        )
        if num_matched != num_read:
            plog(
                "(of %d features read, wrote %d to the working copy that match the spatial filter)",
                num_read,
                num_matched,
            )

    @property
    def feature_count(self):
        """The total number of features in this dataset."""
        return sum(1 for blob in self.feature_blobs())

    def get_features(
        self, row_pks, *, ignore_missing=False, spatial_filter=SpatialFilter.MATCH_ALL
    ):
        """
        Yields a dict for each of the specified features.
        If ignore_missing is True, then failing to find a specified feature does not raise a KeyError.
        If the spatial filter is set, only features which match the spatial filter will be returned.
        """

        spatial_filter = spatial_filter.transform_for_dataset(self)
        for pk_values in row_pks:
            try:
                feature = self.get_feature(pk_values)
                if spatial_filter.matches(feature):
                    yield feature
            except KeyError as e:
                if ignore_missing or spatial_filter.feature_is_prefiltered(e):
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

    def get_feature_from_blob(self, feature_blob):
        return self.get_feature(path=feature_blob.name, data=memoryview(feature_blob))


class IntegrityError(ValueError):
    pass
