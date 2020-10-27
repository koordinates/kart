import functools
import os

import click
import pygit2

from . import crs_util, gpkg_adapter
from .rich_base_dataset import RichBaseDataset
from .exceptions import InvalidOperation, NotYetImplemented, PATCH_DOES_NOT_APPLY
from .meta_items import META_ITEM_NAMES
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


class Dataset2(RichBaseDataset):
    """
    - Uses messagePack to serialise features.
    - Stores each feature in a blob with path dependent on primary key values.
    - Add at any location: `sno import GPKG:my.gpkg:mytable path/to/mylayer`

    any/structure/mylayer/
      .sno-dataset/
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

    VERSION = 2

    DATASET_DIRNAME = ".sno-dataset"
    DATASET_PATH = ".sno-dataset/"

    FEATURE_PATH = DATASET_PATH + "feature/"
    META_PATH = DATASET_PATH + "meta/"

    LEGEND_DIRNAME = "legend"
    LEGEND_PATH = META_PATH + "legend/"
    SCHEMA_PATH = META_PATH + "schema.json"

    TITLE_PATH = META_PATH + "title"
    DESCRIPTION_PATH = META_PATH + "description"

    CRS_PATH = META_PATH + "crs/"

    METADATA_PATH = META_PATH + "metadata/"
    DATASET_METADATA_PATH = METADATA_PATH + "dataset.json"

    @functools.lru_cache()
    def get_meta_item(self, name):
        if name == "version":
            return 2

        rel_path = self.META_PATH + name
        data = self.get_data_at(rel_path, missing_ok=name in META_ITEM_NAMES)
        if data is None:
            return data

        if rel_path.startswith(self.LEGEND_PATH):
            return data

        if rel_path.endswith(".json"):
            return json_unpack(data)
        elif rel_path.endswith(".wkt"):
            return crs_util.normalise_wkt(ensure_text(data))
        else:
            return ensure_text(data)

    @functools.lru_cache()
    def get_gpkg_meta_item(self, name):
        return gpkg_adapter.generate_gpkg_meta_item(self, name, self.table_name)

    def crs_definitions(self):
        """Yields (identifier, definition) for all CRS definitions in this dataset."""
        if not self.tree or self.CRS_PATH not in self.tree:
            return
        for blob in find_blobs_in_tree(self.tree / self.CRS_PATH):
            # -4 -> Remove ".wkt"
            yield blob.name[:-4], crs_util.normalise_wkt(ensure_text(blob.data))

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

    def get_feature(self, pk_values=None, *, path=None, data=None, keys=True):
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
        if self.FEATURE_PATH not in self.tree:
            return
        for blob in find_blobs_in_tree(self.tree / self.FEATURE_PATH):
            yield self.get_feature(path=blob.name, data=blob.data, keys=keys)

    @property
    def feature_count(self):
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

    def encode_raw_feature_dict(self, raw_feature_dict, legend, relative=False):
        """
        Given a "raw" feature dict (keyed by column IDs) and a legend, returns the path
        and the data which *should be written* to write this feature. This is almost the
        inverse of get_raw_feature_dict, except Dataset2 doesn't write the data.
        """
        pk_values, non_pk_values = legend.raw_dict_to_value_tuples(raw_feature_dict)
        path = self.encode_pks_to_path(pk_values, relative=relative)
        data = msg_pack([legend.hexhash(), non_pk_values])
        return path, data

    def encode_feature(self, feature, schema=None, relative=False):
        """
        Given a feature (either a dict keyed by column name, or a list / tuple in schema order),
        returns the path and the data which *should be written* to write this feature. This is
        almost the inverse of get_feature, except Dataset2 doesn't write the data.
        """
        if schema is None:
            schema = self.schema
        raw_dict = schema.feature_to_raw_dict(feature)
        return self.encode_raw_feature_dict(raw_dict, schema.legend, relative=relative)

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

        for path, definition in source.crs_definitions():
            rel_meta_blobs.append((f"{self.CRS_PATH}{path}.wkt", definition))

        for rel_path, content in rel_meta_blobs:
            if content is None:
                continue
            is_json = rel_path.endswith(".json")
            content = json_pack(content) if is_json else ensure_bytes(content)
            yield self.full_path(rel_path), content

    def iter_legend_blob_data(self):
        """
        Generates (full_path, blob_data) tuples for each legend in this dataset
        """
        legend_tree = self.meta_tree / "legend"
        for blob in legend_tree:
            yield (
                self.full_path(self.LEGEND_PATH + blob.name),
                blob.data,
            )

    def import_iter_feature_blobs(self, resultset, source, replacing_dataset=None):
        schema = source.schema
        if replacing_dataset is not None and replacing_dataset.schema != source.schema:
            # Optimisation: Try to avoid rewriting features for compatible schema changes.
            change_types = replacing_dataset.schema.diff_type_counts(source.schema)
            if not change_types["pk_updates"]:
                # We can probably avoid rewriting all features.
                for feature in resultset:
                    pk_values = (feature[replacing_dataset.primary_key],)
                    rel_path = self.encode_pks_to_path(pk_values, relative=True)
                    try:
                        existing_data = replacing_dataset.get_data_at(
                            rel_path, as_memoryview=True
                        )
                    except KeyError:
                        # this feature isn't in the dataset we're replacing
                        yield self.encode_feature(feature, schema)
                        continue

                    existing_feature_raw_dict = replacing_dataset.get_raw_feature_dict(
                        pk_values, data=existing_data
                    )
                    # This adapts the existing feature to the new schema
                    existing_feature = schema.feature_from_raw_dict(
                        existing_feature_raw_dict
                    )
                    if existing_feature == feature:
                        # Nothing changed? No need to rewrite the feature blob
                        yield self.encode_pks_to_path(pk_values), existing_data
                    else:
                        yield self.encode_feature(feature, schema)
                return
        for feature in resultset:
            yield self.encode_feature(feature, schema)

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

    def apply_meta_diff(
        self, meta_diff, tree_builder, *, allow_missing_old_values=False
    ):
        """Apply a meta diff to this dataset. Checks for conflicts."""
        if not meta_diff:
            return

        # Applying diffs works even if there is no tree yet created for the dataset,
        # as is the case when the dataset is first being created right now.
        meta_tree = self.meta_tree if self.tree is not None else ()

        has_conflicts = False
        with tree_builder.chdir(self.META_PATH):
            for delta in meta_diff.values():
                name = delta.key
                old_value = delta.old_value
                new_value = delta.new_value

                # Schema.json needs some special-casing - for one thing, we need to write the legend too.
                if name == "schema.json":
                    old_schema = (
                        Schema.from_column_dicts(old_value) if old_value else None
                    )
                    new_schema = (
                        Schema.from_column_dicts(new_value) if new_value else None
                    )

                    if old_schema and new_schema:
                        if not old_schema.is_pk_compatible(new_schema):
                            raise NotYetImplemented(
                                "Schema changes that involve primary key changes are not yet supported"
                            )
                    if new_schema:
                        legend = new_schema.legend
                        tree_builder.insert(
                            f"{self.LEGEND_DIRNAME}/{legend.hexhash()}", legend.dumps()
                        )

                # Conflict detection
                if delta.type == "delete" and name not in meta_tree:
                    has_conflicts = True
                    click.echo(
                        f"{self.path}: Trying to delete nonexistent meta item: {name}"
                    )
                    continue
                if (
                    delta.type == "insert"
                    and (not allow_missing_old_values)
                    and name in meta_tree
                ):
                    has_conflicts = True
                    click.echo(
                        f"{self.path}: Trying to create meta item that already exists: {name}"
                    )
                    continue

                if delta.type == "update" and name not in meta_tree:
                    has_conflicts = True
                    click.echo(
                        f"{self.path}: Trying to update nonexistent meta item: {name}"
                    )
                    continue
                if delta.type == "update" and self.get_meta_item(name) != old_value:
                    has_conflicts = True
                    click.echo(
                        f"{self.path}: Trying to update out-of-date meta item: {name}"
                    )
                    continue

                # General case
                if new_value is not None:
                    if name.endswith(".json"):
                        new_value = json_pack(new_value)
                    else:
                        new_value = ensure_bytes(new_value)
                    tree_builder.insert(name, new_value)
                else:
                    tree_builder.remove(name)

        if has_conflicts:
            raise InvalidOperation(
                "Patch does not apply",
                exit_code=PATCH_DOES_NOT_APPLY,
            )
