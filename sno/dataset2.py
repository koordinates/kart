import functools
import os

import pygit2

from .structure import DatasetStructure
from .schema import Legend, Schema
from .serialise_util import (
    msg_pack,
    msg_unpack,
    json_pack,
    json_unpack,
    b64encode_str,
    b64decode_str,
    hexhash,
    ensure_bytes,
    ensure_text,
)


def find_blobs_in_tree(tree, max_depth=4):
    """
    Recursively yields possible blobs in the given directory tree,
    up to a given max_depth.
    """
    for entry in tree:
        if isinstance(entry, pygit2.Blob):
            yield entry
        elif max_depth > 0:
            yield from find_blobs_in_tree(entry, max_depth - 1)


# So tests can patch this out. it's hard to mock memoryviews...
_blob_to_memoryview = memoryview


class Dataset2(DatasetStructure):
    """
    - Uses messagePack to serialise features.
    - Stores each feature in a blob with path dependent on primary key values.
    - Add at any location: `sno import GPKG:my.gpkg:mytable path/to/mylayer`

    any/structure/mylayer/
      .sno-table/
        meta/
          schema              = [current schema JSON]
          legend/
            [legend-a-hash]   = [column-id0, column-id1, ...]
            [legend-b-hash]   = [column-id0, column-id1, ...]
            ...

        [hex(pk-hash):2]/
          [hex(pk-hash):2]/
            [base64(pk-value)]  = [msgpack([legend-x-hash, value0, value1, ...])]

    Dataset2 is initialised pointing at a particular directory tree, and uses that
    to read features and schemas. However, it never writes to the tree, since this
    is not straight-forward in git/sno and involves batching writes into a commit.
    Therefore, there are no methods which write, only methods which return things
    which *should be written*. The caller must write these to a commit.
    """

    FEATURE_PATH = ".sno-table/feature/"
    META_PATH = ".sno-table/meta/"
    LEGEND_PATH = ".sno-table/meta/legend/"
    SCHEMA_PATH = ".sno-table/meta/schema"

    TITLE_PATH = ".sno-table/meta/title"
    DESCRIPTION_PATH = ".sno-table/meta/description"

    SRS_PATH = ".sno-table/meta/srs/"

    DATASET_METADATA_PATH = ".sno-table/meta/metadata/dataset.json"

    @property
    def version(self):
        return 2

    @property
    @functools.lru_cache(maxsize=1)
    def feature_tree(self):
        return self.tree / self.FEATURE_PATH

    def get_data_at(self, rel_path, as_memoryview=False):
        """
        Return the data at the given relative path from within this dataset.

        Data is usually returned as a bytestring.
        If as_memoryview=True is given, data is returned as a memoryview instead
        (this avoids a copy, so can make loops more efficient for many rows)
        """
        leaf = None
        try:
            leaf = self.tree / str(rel_path)
        except KeyError:
            pass
        else:
            if leaf.type_str == 'blob':
                if as_memoryview:
                    try:
                        return _blob_to_memoryview(leaf)
                    except TypeError:
                        pass
                else:
                    try:
                        return leaf.data
                    except AttributeError:
                        pass
        # if we got here, that means leaf wasn't a blob, or one of the above
        # exceptions happened...
        raise KeyError(f"No data found at rel-path {rel_path}, type={type(leaf)}")

    def iter_meta_items(self, include_hidden=False):
        exclude = () if include_hidden else ("legend", "version")
        return self._iter_meta_items(exclude=exclude)

    @functools.lru_cache()
    def get_meta_item(self, name):
        if name == "version":
            return 2
        try:
            rel_path = self.META_PATH + name
            data = self.get_data_at(rel_path)

            # TODO - make schema path end with ".json"?
            if rel_path == self.SCHEMA_PATH or rel_path.endswith(".json"):
                return json_unpack(data)
            elif not rel_path.startswith(self.LEGEND_PATH):
                return ensure_text(data)
            else:
                return data

        except KeyError:
            from . import gpkg_adapter

            if name in gpkg_adapter.V2_META_ITEMS:
                return None  # We happen not to have this meta-item, but it is real.
            elif gpkg_adapter.is_gpkg_meta_item(name):
                # These items are not stored, but generated from other items that are stored.
                return gpkg_adapter.generate_gpkg_meta_item(self, name)
            raise  # This meta-item doesn't exist at all.

    def get_srs_definition(self, srs_name):
        """Return the SRS definition stored with the given name."""
        return self.get_meta_item(f"srs/{srs_name}.wkt")

    def srs_definitions(self):
        """Return all stored srs definitions in a dict."""
        for blob in find_blobs_in_tree(self.tree / self.SRS_PATH):
            # -4 -> Remove ".wkt"
            yield blob.name[:-4], ensure_text(blob.data)

    @functools.lru_cache()
    def get_legend(self, legend_hash):
        """Load the legend with the given hash from this dataset."""
        path = self.LEGEND_PATH + legend_hash
        return Legend.loads(self.get_data_at(path))

    def encode_legend(self, legend):
        """
        Given a legend, returns the path and the data which *should be written*
        to write this legend. This is almost the inverse of get_legend, except
        Dataset2 doesn't write the data.
        """
        return self.full_path(self.LEGEND_PATH + legend.hexhash()), legend.dumps()

    @property
    @functools.lru_cache(maxsize=1)
    def schema(self):
        """Load the current schema from this dataset."""
        return Schema.loads(self.get_data_at(self.SCHEMA_PATH))

    def encode_schema(self, schema):
        """
        Given a schema, returns the path and the data which *should be written*
        to write this schema. This is almost the inverse of calling .schema,
        except Dataset2 doesn't write the data. (Note that the schema's legend
        should also be stored if any features are written with this schema.)
        """
        return self.full_path(self.SCHEMA_PATH), schema.dumps()

    def get_raw_feature_dict(self, pk_values=None, *, path=None, data=None):
        """
        Gets the feature with the given primary key(s) / at the given "full" path.
        The result is a "raw" feature dict, values are keyed by column ID,
        and contains exactly those values that are actually stored in the tree,
        which might not be the same values that are now in the schema.
        To get a feature consistent with the current schema, call get_feature.
        """

        # The caller must supply at least one of (pk_values, path) so we know which
        # feature is meant. We can infer whichever one is missing from the one supplied.
        # If the caller knows both already, they can supply both, to avoid redundant work.
        # Similarly, if the caller knows data, they can supply that too to avoid redundant work.
        if pk_values is None and path is None:
            raise ValueError("Either <pk_values> or <path> must be supplied")

        if pk_values is not None:
            pk_values = self.schema.sanitise_pks(pk_values)
        else:
            pk_values = self.decode_path_to_pks(path)

        if data is None:
            if path is not None:
                rel_path = self.ensure_rel_path(path)
            else:
                rel_path = self.encode_pks_to_path(pk_values, relative=True)
            data = self.get_data_at(rel_path, as_memoryview=True)

        legend_hash, non_pk_values = msg_unpack(data)
        legend = self.get_legend(legend_hash)
        return legend.value_tuples_to_raw_dict(pk_values, non_pk_values)

    def get_feature(
        self, pk_values=None, *, path=None, data=None, keys=True, ogr_geoms=None
    ):
        """
        Gets the feature with the given primary key(s) / at the given "full" path.
        The result is either a dict of values keyed by column name (if keys=True)
        or a tuple of values in schema order (if keys=False).
        """
        raw_dict = self.get_raw_feature_dict(pk_values=pk_values, path=path, data=data)
        return self.schema.feature_from_raw_dict(raw_dict, keys=keys)

    def features(self, keys=True, fast=None):
        """
        Returns a generator that calls get_feature once per feature.
        Each entry in the generator is the path of the feature and then the feature itself.
        """

        # TODO: optimise.
        # TODO: don't return the path of each feature by default - most callers don't care.
        # (but this is the interface shared by dataset1 at the moment.)
        if self.FEATURE_PATH not in self.tree:
            return
        for blob in find_blobs_in_tree(self.tree / self.FEATURE_PATH):
            yield blob.name, self.get_feature(
                path=blob.name, data=blob.data, keys=keys
            ),

    def feature_count(self, fast=None):
        if self.FEATURE_PATH not in self.tree:
            return 0
        return sum(1 for blob in find_blobs_in_tree(self.tree / self.FEATURE_PATH))

    @classmethod
    def decode_path_to_pks(cls, path):
        """Given a feature path, returns the pk values encoded in it."""
        encoded = os.path.basename(path)
        return msg_unpack(b64decode_str(encoded))

    @classmethod
    def decode_path_to_1pk(cls, path):
        decoded = cls.decode_path_to_pks(path)
        if len(decoded) != 1:
            raise ValueError(f"Expected a single pk_value, got {decoded}")
        return decoded[0]

    def encode_raw_feature_dict(self, raw_feature_dict, legend):
        """
        Given a "raw" feature dict (keyed by column IDs) and a schema, returns the path
        and the data which *should be written* to write this feature. This is almost the
        inverse of get_raw_feature_dict, except Dataset2 doesn't write the data.
        """
        pk_values, non_pk_values = legend.raw_dict_to_value_tuples(raw_feature_dict)
        path = self.encode_pks_to_path(pk_values)
        data = msg_pack([legend.hexhash(), non_pk_values])
        return path, data

    def encode_feature(self, feature, schema=None):
        """
        Given a feature (either a dict keyed by column name, or a list / tuple in schema order),
        returns the path and the data which *should be written* to write this feature. This is
        almost the inverse of get_feature, except Dataset2 doesn't write the data.
        """
        if schema is None:
            schema = self.schema
        raw_dict = schema.feature_to_raw_dict(feature)
        return self.encode_raw_feature_dict(raw_dict, schema.legend)

    def encode_pks_to_path(self, pk_values, relative=False):
        """
        Given some pk values, returns the path the feature should be written to.
        pk_values should be a list or tuple of pk values.
        """
        packed_pk = msg_pack(pk_values)
        pk_hash = hexhash(packed_pk)
        filename = b64encode_str(packed_pk)
        rel_path = f"{self.FEATURE_PATH}{pk_hash[:2]}/{pk_hash[2:4]}/{filename}"
        return rel_path if relative else self.full_path(rel_path)

    def encode_1pk_to_path(self, pk_value, relative=False):
        """Given a feature's only pk value, returns the path the feature should be written to."""
        if isinstance(pk_value, (list, tuple)):
            raise ValueError(f"Expected a single pk value, got {pk_value}")
        return self.encode_pks_to_path((pk_value,), relative=relative)

    def import_iter_meta_blobs(self, repo, source):
        schema = source.schema
        yield self.encode_schema(schema)
        yield self.encode_legend(schema.legend)

        rel_meta_blobs = [
            (self.TITLE_PATH, source.get_meta_item("title")),
            (self.DESCRIPTION_PATH, source.get_meta_item("description")),
            (self.DATASET_METADATA_PATH, source.get_meta_item("metadata/dataset.json")),
        ]

        for path, definition in source.srs_definitions():
            rel_meta_blobs.append((f"{self.SRS_PATH}{path}.wkt", definition))

        for rel_path, content in rel_meta_blobs:
            if content is None:
                continue
            is_json = rel_path.endswith(".json")
            content = json_pack(content) if is_json else ensure_bytes(content)
            yield self.full_path(rel_path), content

    def import_iter_feature_blobs(self, resultset, source):
        schema = source.schema
        for feature in resultset:
            yield self.encode_feature(feature, schema)

    @property
    def primary_key(self):
        # TODO - datasets v2 model supports more than one primary key.
        # This function needs to be changed when we have a working copy v2 that does too.
        if len(self.schema.pk_columns) == 1:
            return self.schema.pk_columns[0].name
        raise ValueError(f"No single primary key: {self.schema.pk_columns}")

    def encode_feature_blob(self, feature):
        # TODO - the dataset interface still needs some work:
        # - having a _blob version of encode_feature is too many similar methods.
        return self.encode_feature(feature, self.schema)[1]

    def get_feature_tuples(self, row_pks, col_names=None, *, ignore_missing=False):
        # TODO - make the signature more like the features method, which supports results as tuples or dicts.
        # TODO - support col_names (and maybe support it for features method too).
        for pk in row_pks:
            try:
                yield self.get_feature(pk, keys=False)
            except KeyError:
                if ignore_missing:
                    continue
                else:
                    raise
