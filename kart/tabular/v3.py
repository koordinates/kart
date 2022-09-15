import functools
import os
import re

import pygit2

from kart.base_dataset import BaseDataset
from kart.core import find_blobs_in_tree
from kart.exceptions import (
    PATCH_DOES_NOT_APPLY,
    InvalidOperation,
    NotYetImplemented,
)
from kart import meta_items
from kart.meta_items import MetaItemDefinition, MetaItemFileType, MetaItemVisibility
from kart.schema import Legend, Schema
from kart.serialise_util import (
    b64decode_str,
    ensure_bytes,
    ensure_text,
    json_pack,
    json_unpack,
    msg_pack,
    msg_unpack,
)
from .v3_paths import PathEncoder
from .rich_table_dataset import RichTableDataset


class TableV3(RichTableDataset):
    """
    - Uses messagePack to serialise features.
    - Stores each feature in a blob with path dependent on primary key values.
    - Stored at a particular path eg "path/to/my/layer".

    any/structure/mylayer/
      .table-dataset/
        meta/
          schema.json         = [current schema JSON]
          legend/
            [legend-a-hash]   = [column-id0, column-id1, ...]
            [legend-b-hash]   = [column-id0, column-id1, ...]
            ...

        feature/
          encoded_path(pk_value) = [msgpack([legend-x-hash, value0, value1, ...])]

    TableV3 is initialised pointing at a particular directory tree, and uses that
    to read features and schemas. However, it never writes to the tree, since this
    is not straight-forward in git/kart and involves batching writes into a commit.
    Therefore, there are no methods which write, only methods which return things
    which *should be written*. The caller must write these to a commit.

    Path encoding is handled by PathEncoder in v3_paths.py - the exact system
    for choosing a path based on a PK can differ, but typically they look something
    like: A/B/C/D/E/base64(msgpack(pk_value)), where A-E are characters from the
    Base64 alphabet. See v3_paths.py for details.
    """

    VERSION = 3

    DATASET_DIRNAME = ".table-dataset"  # New name for V3 datasets.

    META_PATH = "meta/"
    LEGEND_DIRNAME = "legend"
    LEGEND_PATH = META_PATH + "legend/"
    SCHEMA_PATH = META_PATH + "schema.json"

    # === Visible meta-items ===
    TITLE = meta_items.TITLE
    DESCRIPTION = meta_items.DESCRIPTION
    METADATA_XML = meta_items.METADATA_XML
    SCHEMA_JSON = meta_items.SCHEMA_JSON
    CRS_DEFINITIONS = meta_items.CRS_DEFINITIONS

    # == Hidden meta-items (which don't show in diffs) ==
    # How automatically generated PKs have been assigned so far:
    GENERATED_PKS = MetaItemDefinition(
        "generated-pks.json", MetaItemFileType.JSON, MetaItemVisibility.HIDDEN
    )
    # How primary keys are converted to feature paths:
    PATH_STRUCTURE = MetaItemDefinition(
        "path-structure.json", MetaItemFileType.JSON, MetaItemVisibility.INTERNAL_ONLY
    )
    # Legends are used to help decode each feature:
    LEGEND = MetaItemDefinition(
        re.compile(r"legend/(.*)"),
        MetaItemFileType.BYTES,
        MetaItemVisibility.INTERNAL_ONLY,
    )

    META_ITEMS = (
        TITLE,
        DESCRIPTION,
        METADATA_XML,
        SCHEMA_JSON,
        CRS_DEFINITIONS,
        GENERATED_PKS,
        PATH_STRUCTURE,
        LEGEND,
    )

    # This meta-item is generally stored in the "attachment" area, alongside the dataset, rather than inside it.
    # Storing it in this unusual location adds complexity without actually solving any problems, so any datasets
    # designed after table.v3 don't do this.
    ATTACHMENT_META_ITEMS = ("metadata.xml",)

    @functools.lru_cache()
    def get_meta_item(self, meta_item_path, missing_ok=True):
        # Handle meta-items stored in the attachment area:
        if (
            meta_item_path in self.ATTACHMENT_META_ITEMS
            and meta_item_path not in self.meta_tree
            and self.tree is not None
            and meta_item_path in self.tree
        ):
            return ensure_text(self.get_data_at(meta_item_path, from_tree=self.tree))

        return super().get_meta_item(meta_item_path, missing_ok=missing_ok)

    @functools.lru_cache(maxsize=1)
    def crs_definitions(self):
        """Returns {identifier: definition} dict for all CRS definitions in this dataset."""
        result = self.get_meta_items_matching(self.CRS_DEFINITIONS)
        return {
            self.CRS_DEFINITIONS.match_group(path, 1): value
            for path, value in result.items()
        }

    @functools.lru_cache()
    def get_legend(self, legend_hash):
        """Load the legend with the given hash from this dataset."""
        path = self.LEGEND_PATH + legend_hash
        return Legend.loads(self.get_data_at(path))

    def encode_legend(self, legend):
        """
        Given a legend, returns the path and the data which *should be written*
        to write this legend. This is almost the inverse of get_legend, except
        TableV3 doesn't write the data.
        """
        return (
            self.ensure_full_path(self.LEGEND_PATH + legend.hexhash()),
            legend.dumps(),
        )

    def encode_schema(self, schema):
        """
        Given a schema, returns the path and the data which *should be written*
        to write this schema. This is almost the inverse of calling .schema,
        except TableV3 doesn't write the data. (Note that the schema's legend
        should also be stored if any features are written with this schema.)
        """
        return self.ensure_full_path(self.SCHEMA_PATH), schema.dumps()

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
        elif getattr(data, "type", None) == pygit2.GIT_OBJ_BLOB:
            # Data is a blob - open a memoryview on it.
            data = memoryview(data)

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
        Returns a generator that yields every feature blob in turn.
        """
        if self.FEATURE_PATH not in self.inner_tree:
            return
        yield from find_blobs_in_tree(self.inner_tree / self.FEATURE_PATH)

    @property
    @functools.lru_cache(maxsize=1)
    def feature_path_encoder(self):
        if not self.inner_tree:
            # No meta tree; we must be still creating this dataset.
            # Figure out a sensible path encoder to use:
            pks = self.schema.pk_columns
            if len(pks) == 1 and pks[0].data_type == "integer":
                return PathEncoder.INT_PK_ENCODER
            else:
                return PathEncoder.GENERAL_ENCODER
        # Otherwise, load the path-structure meta-item.
        path_structure = self.get_meta_item("path-structure.json", missing_ok=True)
        if path_structure is not None:
            return PathEncoder.get(**path_structure)
        return PathEncoder.LEGACY_ENCODER

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
        inverse of get_raw_feature_dict, except TableV3 doesn't write the data.
        """
        pk_values, non_pk_values = legend.raw_dict_to_value_tuples(raw_feature_dict)
        path = self.encode_pks_to_path(pk_values, relative=relative, schema=schema)
        data = msg_pack([legend.hexhash(), non_pk_values])
        return path, data

    def encode_feature(self, feature, schema=None, relative=False):
        """
        Given a feature (either a dict keyed by column name, or a list / tuple in schema order),
        returns the path and the data which *should be written* to write this feature. This is
        almost the inverse of get_feature, except TableV3 doesn't write the data.
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
        encoder = self.feature_path_encoder
        rel_path = f"{self.FEATURE_PATH}{encoder.encode_pks_to_path(pk_values)}"
        return rel_path if relative else self.ensure_full_path(rel_path)

    def encode_1pk_to_path(self, pk_value, relative=False, *, schema=None):
        """Given a feature's only pk value, returns the path the feature should be written to."""
        if isinstance(pk_value, (list, tuple)):
            raise ValueError(f"Expected a single pk value, got {pk_value}")
        return self.encode_pks_to_path((pk_value,), relative=relative)

    def import_iter_meta_blobs(self, repo, source):
        # The source schema is a meta item.
        # The legend of said schema is not a meta item, but must also be written.
        yield self.encode_legend(source.schema.legend)

        # This can include non-standard meta-items, like generated-pks.json
        meta_items = source.meta_items()

        # The path encoder is not a meta-item of the source, since it is only a property
        # of how we are importing the data into this dataset. But it must also be written.
        path_encoder = self.feature_path_encoder
        if path_encoder is not PathEncoder.LEGACY_ENCODER:
            meta_items["path-structure.json"] = path_encoder.to_dict()

        for rel_path, content in meta_items.items():
            if content is None:
                continue
            if not isinstance(content, bytes):
                if rel_path.endswith(".json"):
                    content = json_pack(content)
                else:
                    content = ensure_bytes(content)

            if rel_path in self.ATTACHMENT_META_ITEMS:
                full_path = self.full_attachment_path(rel_path)
            else:
                if not rel_path.startswith(self.META_PATH):
                    rel_path = self.META_PATH + rel_path
                full_path = self.ensure_full_path(rel_path)

            yield full_path, content

        for rel_path, content in source.attachment_items():
            yield self.full_attachment_path(rel_path), content

    def iter_legend_blob_data(self):
        """
        Generates (full_path, blob_data) tuples for each legend in this dataset
        """
        legend_tree = self.meta_tree / "legend"
        for blob in legend_tree:
            yield (
                self.ensure_full_path(self.LEGEND_PATH + blob.name),
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
        self, meta_diff, object_builder, *, resolve_missing_values_from_ds=None
    ):
        """Apply a meta diff to this dataset. Checks for conflicts."""
        if not meta_diff:
            return

        no_conflicts = True

        resolve_missing_values_from_tree = None
        if resolve_missing_values_from_ds:
            resolve_missing_values_from_tree = resolve_missing_values_from_ds.meta_tree

        # Apply diff to hidden meta items folder: <dataset>/.table-dataset/meta/<item-name>
        with object_builder.chdir(f"{self.inner_path}/{self.META_PATH}"):
            no_conflicts &= self._apply_meta_deltas_to_tree(
                (
                    d
                    for d in meta_diff.values()
                    if d.key not in self.ATTACHMENT_META_ITEMS
                ),
                object_builder,
                self.meta_tree if self.inner_tree is not None else None,
                resolve_missing_values_from_tree=resolve_missing_values_from_tree,
            )

        if resolve_missing_values_from_ds:
            resolve_missing_values_from_tree = (
                resolve_missing_values_from_ds.attachment_tree
            )

        # Apply diff to visible attachment meta items: <dataset>/<item-name>
        with object_builder.chdir(self.path):
            no_conflicts &= self._apply_meta_deltas_to_tree(
                (d for d in meta_diff.values() if d.key in self.ATTACHMENT_META_ITEMS),
                object_builder,
                self.attachment_tree,
                resolve_missing_values_from_tree=resolve_missing_values_from_tree,
            )

        if not no_conflicts:
            raise InvalidOperation(
                "Patch does not apply",
                exit_code=PATCH_DOES_NOT_APPLY,
            )

    def _apply_meta_deltas_to_tree(
        self,
        deltas,
        object_builder,
        existing_tree,
        *,
        resolve_missing_values_from_tree=None,
    ):
        # Applying diffs works even if there is no tree yet created for the dataset,
        # as is the case when the dataset is first being created right now.
        if existing_tree is None:
            # This lets us test if something is in existing_tree without crashing.
            existing_tree = ()

        no_conflicts = True
        for delta in deltas:
            # Schema.json needs some special-casing - for one thing, we need to write the legend too.
            if delta.key == "schema.json":
                no_conflicts &= self._apply_schema_json_delta_to_tree(
                    delta,
                    object_builder,
                    existing_tree,
                    resolve_missing_values_from_tree=resolve_missing_values_from_tree,
                )
            else:
                # General case:
                no_conflicts &= self._apply_meta_delta_to_tree(
                    delta,
                    object_builder,
                    existing_tree,
                    resolve_missing_values_from_tree=resolve_missing_values_from_tree,
                )

        return no_conflicts

    def _apply_schema_json_delta_to_tree(
        self,
        delta,
        object_builder,
        existing_tree,
        *,
        resolve_missing_values_from_tree=False,
    ):
        old_value = delta.old_value
        new_value = delta.new_value

        old_schema = Schema.from_column_dicts(old_value) if old_value else None
        new_schema = Schema.from_column_dicts(new_value) if new_value else None

        if old_schema and new_schema:
            if not old_schema.is_pk_compatible(new_schema):
                raise NotYetImplemented(
                    "Schema changes that involve primary key changes are not yet supported"
                )
        if new_schema:
            legend = new_schema.legend
            object_builder.insert(
                f"{self.LEGEND_DIRNAME}/{legend.hexhash()}",
                legend.dumps(),
            )
        path_encoder = self.feature_path_encoder
        if (
            new_schema
            and not existing_tree
            and path_encoder is not PathEncoder.LEGACY_ENCODER
        ):
            object_builder.insert(
                "path-structure.json", json_pack(path_encoder.to_dict())
            )

        return self._apply_meta_delta_to_tree(
            delta,
            object_builder,
            existing_tree,
            resolve_missing_values_from_tree=resolve_missing_values_from_tree,
        )
