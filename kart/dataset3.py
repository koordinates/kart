import functools
import os

import click
import pygit2

from . import crs_util
from .dataset3_paths import PathEncoder
from .rich_base_dataset import RichBaseDataset
from .exceptions import InvalidOperation, NotYetImplemented, PATCH_DOES_NOT_APPLY
from .meta_items import META_ITEM_NAMES, ATTACHMENT_META_ITEMS
from .schema import Legend, Schema
from .serialise_util import (
    msg_pack,
    msg_unpack,
    json_pack,
    json_unpack,
    b64decode_str,
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


class Dataset3(RichBaseDataset):
    """
    - Uses messagePack to serialise features.
    - Stores each feature in a blob with path dependent on primary key values.
    - Stored at a particular path eg "path/to/my/layer".

    any/structure/mylayer/
      .sno-dataset/
        meta/
          schema              = [current schema JSON]
          legend/
            [legend-a-hash]   = [column-id0, column-id1, ...]
            [legend-b-hash]   = [column-id0, column-id1, ...]
            ...

        feature/
          encoded_path(pk_value) = [msgpack([legend-x-hash, value0, value1, ...])]

    Dataset3 is initialised pointing at a particular directory tree, and uses that
    to read features and schemas. However, it never writes to the tree, since this
    is not straight-forward in git/kart and involves batching writes into a commit.
    Therefore, there are no methods which write, only methods which return things
    which *should be written*. The caller must write these to a commit.

    Path encoding is handled by PathEncoder in dataset3_paths.py - the exact system
    for choosing a path based on a PK can differ, but typically they look something
    like: A/B/C/D/E/base64(msgpack(pk_value)), where A-E are characters from the
    Base64 alphabet. See dataset3_paths.py for details.
    """

    VERSION = 3

    DATASET_DIRNAME = ".table-dataset"  # New name for V3 datasets.

    # All relative paths should be relative to self.inner_tree - that is, to the tree named DATASET_DIRNAME.
    FEATURE_PATH = "feature/"
    META_PATH = "meta/"

    LEGEND_DIRNAME = "legend"
    LEGEND_PATH = META_PATH + "legend/"
    SCHEMA_PATH = META_PATH + "schema.json"

    TITLE_PATH = META_PATH + "title"
    DESCRIPTION_PATH = META_PATH + "description"

    CRS_PATH = META_PATH + "crs/"

    # Attachments
    METADATA_XML = "metadata.xml"

    @classmethod
    def is_dataset_tree(cls, tree):
        if tree is None:
            return False
        return (
            cls.DATASET_DIRNAME in tree
            and (tree / cls.DATASET_DIRNAME).type_str == "tree"
        )

    @functools.lru_cache()
    def get_meta_item(self, name):
        if name == "version":
            return 2

        if name in ATTACHMENT_META_ITEMS:
            rel_path = name
            meta_item_tree = self.attachment_tree
        else:
            rel_path = self.META_PATH + name
            meta_item_tree = self.inner_tree

        data = self.get_data_at(
            rel_path, missing_ok=name in META_ITEM_NAMES, tree=meta_item_tree
        )
        if data is None:
            return data

        if rel_path.startswith(self.LEGEND_PATH):
            return data

        if rel_path.endswith("schema.json"):
            # Unfortunately, some schemas might be stored slightly differently to others -
            # - eg with or without null attributes. This normalises them.
            return Schema.normalise_column_dicts(json_unpack(data))
        if rel_path.endswith(".json"):
            return json_unpack(data)
        elif rel_path.endswith(".wkt"):
            return crs_util.normalise_wkt(ensure_text(data))
        else:
            return ensure_text(data)

    def crs_definitions(self):
        """Yields (identifier, definition) for all CRS definitions in this dataset."""
        if not self.inner_tree or self.CRS_PATH not in self.inner_tree:
            return
        for blob in find_blobs_in_tree(self.inner_tree / self.CRS_PATH):
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
        Dataset3 doesn't write the data.
        """
        return self.full_path(self.LEGEND_PATH + legend.hexhash()), legend.dumps()

    def encode_schema(self, schema):
        """
        Given a schema, returns the path and the data which *should be written*
        to write this schema. This is almost the inverse of calling .schema,
        except Dataset3 doesn't write the data. (Note that the schema's legend
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

    def get_feature(self, pk_values=None, *, path=None, data=None):
        """
        Gets the feature with the given primary key(s) / at the given "full" path.
        The result is a dict of values keyed by column name.
        """
        raw_dict = self.get_raw_feature_dict(pk_values=pk_values, path=path, data=data)
        return self.schema.feature_from_raw_dict(raw_dict)

    def feature_blobs(self):
        """
        Returns a generator that calls get_feature once per feature.
        Each entry in the generator is the path of the feature and then the feature itself.
        """
        if self.FEATURE_PATH not in self.inner_tree:
            return
        yield from find_blobs_in_tree(self.inner_tree / self.FEATURE_PATH)

    @functools.lru_cache(maxsize=1)
    def feature_path_encoder(self, schema=None):
        schema = schema or self.schema
        if self.inner_tree is None:
            # no meta tree; we must be still creating this dataset
            # figure out a sensible path encoder to use:
            pks = schema.pk_columns
            if len(pks) == 1 and pks[0].data_type == "integer":
                return PathEncoder.INT_PK_ENCODER

            else:
                return PathEncoder.GENERAL_ENCODER
        else:
            try:
                path_structure = self.get_meta_item("path-structure.json")
            except KeyError:
                return PathEncoder.LEGACY_ENCODER
        return PathEncoder.get(**path_structure)

    def decode_path_to_pks(self, path):
        """Given a feature path, returns the pk values encoded in it."""
        encoded = os.path.basename(path)
        return msg_unpack(b64decode_str(encoded))

    def decode_path_to_1pk(self, path):
        decoded = self.decode_path_to_pks(path)
        if len(decoded) != 1:
            raise ValueError(f"Expected a single pk_value, got {decoded}")
        return decoded[0]

    def encode_raw_feature_dict(
        self, raw_feature_dict, legend, relative=False, *, schema=None
    ):
        """
        Given a "raw" feature dict (keyed by column IDs) and a legend, returns the path
        and the data which *should be written* to write this feature. This is almost the
        inverse of get_raw_feature_dict, except Dataset3 doesn't write the data.
        """
        pk_values, non_pk_values = legend.raw_dict_to_value_tuples(raw_feature_dict)
        path = self.encode_pks_to_path(pk_values, relative=relative, schema=schema)
        data = msg_pack([legend.hexhash(), non_pk_values])
        return path, data

    def encode_feature(self, feature, schema=None, relative=False):
        """
        Given a feature (either a dict keyed by column name, or a list / tuple in schema order),
        returns the path and the data which *should be written* to write this feature. This is
        almost the inverse of get_feature, except Dataset3 doesn't write the data.
        """
        if schema is None:
            schema = self.schema
        raw_dict = schema.feature_to_raw_dict(feature)
        return self.encode_raw_feature_dict(
            raw_dict, schema.legend, relative=relative, schema=schema
        )

    def encode_pks_to_path(self, pk_values, relative=False, *, schema=None):
        """
        Given some pk values, returns the path the feature should be written to.
        pk_values should be a list or tuple of pk values.
        """
        encoder = self.feature_path_encoder(schema)
        rel_path = f"{self.FEATURE_PATH}{encoder.encode_pks_to_path(pk_values)}"
        return rel_path if relative else self.full_path(rel_path)

    def encode_1pk_to_path(self, pk_value, relative=False, *, schema=None):
        """Given a feature's only pk value, returns the path the feature should be written to."""
        if isinstance(pk_value, (list, tuple)):
            raise ValueError(f"Expected a single pk value, got {pk_value}")
        return self.encode_pks_to_path((pk_value,), relative=relative)

    def import_iter_meta_blobs(self, repo, source):
        schema = source.schema
        yield self.encode_schema(schema)
        yield self.encode_legend(schema.legend)

        meta_blobs = [
            (self.TITLE_PATH, source.get_meta_item("title")),
            (self.DESCRIPTION_PATH, source.get_meta_item("description")),
            (self.METADATA_XML, source.get_meta_item("metadata.xml")),
        ]

        path_encoder = self.feature_path_encoder()
        if path_encoder is not PathEncoder.LEGACY_ENCODER:
            meta_blobs.append(path_encoder.encode_path_structure_data(relative=True))

        for path, definition in source.crs_definitions():
            meta_blobs.append((f"{self.CRS_PATH}{path}.wkt", definition))

        if hasattr(source, "encode_generated_pk_data"):
            meta_blobs.append(source.encode_generated_pk_data(relative=True))

        for rel_path, content in meta_blobs:
            if content is None:
                continue
            if not isinstance(content, bytes):
                if rel_path.endswith(".json"):
                    content = json_pack(content)
                else:
                    content = ensure_bytes(content)

            full_path = (
                self.full_attachment_path(rel_path)
                if rel_path in ATTACHMENT_META_ITEMS
                else self.full_path(rel_path)
            )
            yield full_path, content

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

    def import_iter_feature_blobs(
        self, repo, resultset, source, replacing_dataset=None
    ):
        schema = source.schema
        if replacing_dataset:
            # Optimisation: Try to avoid rewriting features for compatible schema changes.
            # this can take some time, but often results in much fewer git objects produced.
            # For example, consider revision A with this feature:
            #   {"x": 1, "y": 2}
            # Now at revision B we add a nullable column, so the new version of the feature is:
            #   {"x": 1, "y": 2, "z": NULL}
            # In this situation, during the import of revision B, we can avoid writing the
            # new blob, because the existing feature blob from revision A can be 'upgraded'
            # to the new schema without changing anything.
            #
            # This optimisation is useful in the following situations:
            #  * a column was added but some values remain NULL (example above)
            #  * a column was dropped, and some rows have no other values changed
            for feature in resultset:
                try:
                    pk_values = (feature[replacing_dataset.primary_key],)
                    rel_path = self.encode_pks_to_path(
                        pk_values, relative=True, schema=schema
                    )
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
                    yield self.encode_pks_to_path(
                        pk_values, schema=schema
                    ), existing_data
                else:
                    yield self.encode_feature(feature, schema)
        else:
            for feature in resultset:
                yield self.encode_feature(feature, schema)

    def apply_meta_diff(
        self, meta_diff, tree_builder, *, allow_missing_old_values=False
    ):
        """Apply a meta diff to this dataset. Checks for conflicts."""
        if not meta_diff:
            return
        self._check_meta_diff_is_commitable(meta_diff)

        has_conflicts = False

        # Apply diff to hidden meta items folder: <dataset>/.table-dataset/meta/<item-name>
        with tree_builder.chdir(f"{self.inner_path}/{self.META_PATH}"):
            has_conflicts |= self._apply_meta_deltas_to_tree(
                (d for d in meta_diff.values() if d.key not in ATTACHMENT_META_ITEMS),
                tree_builder,
                self.meta_tree if self.inner_tree is not None else None,
                allow_missing_old_values=allow_missing_old_values,
            )

        # Apply diff to visible attachment meta items: <dataset>/<item-name>
        with tree_builder.chdir(self.path):
            has_conflicts |= self._apply_meta_deltas_to_tree(
                (d for d in meta_diff.values() if d.key in ATTACHMENT_META_ITEMS),
                tree_builder,
                self.attachment_tree,
                allow_missing_old_values=allow_missing_old_values,
            )

        if has_conflicts:
            raise InvalidOperation(
                "Patch does not apply",
                exit_code=PATCH_DOES_NOT_APPLY,
            )

    def _check_meta_diff_is_commitable(self, meta_diff):
        # This is currently the only case where we sometimes generate a diff we cannot commit -
        # if the user has tried to attach more than one XML metadata blob to a dataset.
        if "metadata.xml" in meta_diff and isinstance(
            meta_diff["metadata.xml"].new_value, list
        ):
            raise NotYetImplemented(
                "Sorry, committing more than one XML metadata file is not supported"
            )

    def _apply_meta_deltas_to_tree(
        self, deltas, tree_builder, existing_tree, *, allow_missing_old_values=False
    ):
        # Applying diffs works even if there is no tree yet created for the dataset,
        # as is the case when the dataset is first being created right now.
        if existing_tree is None:
            # This lets us test if something is in existing_tree without crashing.
            existing_tree = ()

        has_conflicts = False
        for delta in deltas:
            name = delta.key
            old_value = delta.old_value
            new_value = delta.new_value

            # Schema.json needs some special-casing - for one thing, we need to write the legend too.
            if name == "schema.json":
                old_schema = Schema.from_column_dicts(old_value) if old_value else None
                new_schema = Schema.from_column_dicts(new_value) if new_value else None

                if old_schema and new_schema:
                    if not old_schema.is_pk_compatible(new_schema):
                        raise NotYetImplemented(
                            "Schema changes that involve primary key changes are not yet supported"
                        )
                if new_schema:
                    legend = new_schema.legend
                    tree_builder.insert(
                        f"{self.LEGEND_DIRNAME}/{legend.hexhash()}",
                        legend.dumps(),
                    )
                path_encoder = self.feature_path_encoder(new_schema)
                if (
                    new_schema
                    and not existing_tree
                    and path_encoder is not PathEncoder.LEGACY_ENCODER
                ):
                    tree_builder.insert(
                        "path-structure.json", json_pack(path_encoder.to_dict())
                    )

            # Conflict detection
            if delta.type == "delete" and name not in existing_tree:
                has_conflicts = True
                click.echo(
                    f"{self.path}: Trying to delete nonexistent meta item: {name}"
                )
                continue
            if (
                delta.type == "insert"
                and (not allow_missing_old_values)
                and name in existing_tree
            ):
                has_conflicts = True
                click.echo(
                    f"{self.path}: Trying to create meta item that already exists: {name}"
                )
                continue

            if delta.type == "update" and name not in existing_tree:
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

        return has_conflicts
