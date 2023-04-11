import functools

from kart.base_dataset import BaseDataset
from kart.spatial_filter import SpatialFilter
from kart.working_copy import PartType
from kart.progress_util import progress_bar

from .import_source import TableImportSource


class TableDataset(BaseDataset, TableImportSource):
    """
    Common interface for all datasets - mostly used by TableV3,
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

    # TableDataset is abstract - more dataset interface fields are defined by the concrete subclasses.

    DATASET_TYPE = "table"

    ITEM_TYPE = "feature"

    # Paths are all defined relative to the inner path:
    FEATURE_PATH = "feature/"

    WORKING_COPY_PART_TYPE = PartType.TABULAR

    NUM_FEATURES_PER_PROGRESS_LOG = 10_000

    def __init__(self, tree, path, repo, dirname=None):
        super().__init__(tree, path, repo, dirname=dirname)

        if self.__class__ is TableDataset:
            raise TypeError("Cannot construct a TableDataset - you may want TableV3")

        self.table_name = self.dataset_path_to_table_name(self.path)

        self.repo = repo

    @classmethod
    def new_dataset_for_writing(cls, path, schema, repo):
        """
        Creates a new dataset instance that can be used to write to a new dataset.
        """
        result = cls(None, path, repo)
        result._schema = schema
        return result

    @classmethod
    def dataset_path_to_table_name(cls, ds_path):
        return ds_path.strip("/").replace("/", "__")

    def __repr__(self):
        return f"<{self.__class__.__name__}: {self.path}>"

    def default_dest_path(self):
        # TableImportSource method - by default, a dataset should import with the same path it already has.
        return self.path

    @property
    def attachment_tree(self):
        return self.tree

    @property
    def feature_tree(self):
        return self.get_subtree(self.FEATURE_PATH)

    @property
    def schema(self):
        if not hasattr(self, "_schema"):
            self._schema = super().schema
        return self._schema

    def decode_path(self, path):
        """
        Given a relative path to something inside this dataset eg "feature/49/3e/Bg==""
        returns a tuple in either of the following forms:
        1. ("feature", primary_key)
        2. ("meta", meta_item_path)
        """
        rel_path = self.ensure_rel_path(path)
        if rel_path.startswith("feature/"):
            pk = self.decode_path_to_1pk(rel_path)
            return ("feature", pk)
        return super().decode_path(rel_path)

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

    def features(self, spatial_filter=SpatialFilter.MATCH_ALL, show_progress=False):
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
        show_progress - enables tqdm progress bar to show progress as we iterate through the features.
        """
        spatial_filter = spatial_filter.transform_for_dataset(self)

        n_read = 0
        n_matched = 0
        n_total = self.feature_count if show_progress else 0
        progress = progress_bar(
            show_progress=show_progress, total=n_total, unit="F", desc=self.path
        )

        with progress as p:
            for blob in self.feature_blobs():
                n_read += 1
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

                p.update(1)

        if show_progress and not spatial_filter.match_all:
            p.write(
                f"(of {n_read} features read, wrote {n_matched} matching features to the working copy due to spatial filter)"
            )

    @property
    def feature_count(self):
        """The total number of features in this dataset."""
        return self.count_blobs_in_subtree(self.FEATURE_PATH)

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
