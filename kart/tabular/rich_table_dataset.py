import functools
import time

import click
from kart.diff_format import DiffFormat
from osgeo import osr

from kart import crs_util
from kart.diff_structs import Delta, DeltaDiff, DatasetDiff
from kart.exceptions import PATCH_DOES_NOT_APPLY, InvalidOperation, NotYetImplemented
from kart.key_filters import DatasetKeyFilter, FeatureKeyFilter
from kart.promisor_utils import fetch_promised_blobs, object_is_promised
from kart.schema import Schema
from kart.spatial_filter import SpatialFilter

from .table_dataset import TableDataset


class RichTableDataset(TableDataset):
    """
    Adds extra functionality to the TableDataset.
    This is in a separate class to TableDataset so that the core functionality -
    and the missing abstract functionality that needs to be implemented - isn't lost amongst
    this more advanced functionality (which is built on top of the core functionality).

    If you are only using a TableDataset as an TableImportSource, just extend TableDataset -
    this functionality isn't needed. For example, see Dataset0.
    """

    def features_plus_blobs(self):
        for blob in self.feature_blobs():
            yield self.get_feature(path=blob.name, data=memoryview(blob)), blob

    def features_with_crs_ids(
        self, spatial_filter=SpatialFilter.MATCH_ALL, show_progress=False
    ):
        """
        Same as table_dataset.features(), but includes the CRS ID from the schema in every Geometry object.
        By contrast, the returned Geometries from table_dataset.features() all contain a CRS ID of zero,
        so the schema must be consulted separately to learn about CRS IDs.
        """
        yield from self._add_crs_ids_to_features(
            self.features(spatial_filter, show_progress=show_progress)
        )

    def get_features_with_crs_ids(
        self, row_pks, *, ignore_missing=False, spatial_filter=SpatialFilter.MATCH_ALL
    ):
        """
        Same as table_dataset.get_features(...), but includes the CRS ID from the schema in every Geometry object.
        By contrast, the returned Geometries from table_dataset.get_features(...) all contain a CRS ID of zero,
        so the schema must be consulted separately to learn about CRS IDs.
        """
        yield from self._add_crs_ids_to_features(
            self.get_features(
                row_pks, ignore_missing=ignore_missing, spatial_filter=spatial_filter
            )
        )

    def get_feature_with_crs_id(self, pk_values=None, *, path=None, data=None):
        """
        Same as table_dataset.get_feature(...), but includes the CRS ID from the schema in every Geometry object.
        By contrast, the returned Geometry from table_dataset.get_feature() will contain a CRS ID of zero,
        so the schema must be consulted separately to learn about CRS IDs.
        """
        return self._add_crs_ids_to_feature(
            self.get_feature(pk_values=pk_values, path=path, data=data),
            self._cols_to_crs_ids(),
        )

    def _add_crs_ids_to_features(self, features):
        cols_to_crs_ids = self._cols_to_crs_ids()

        if not cols_to_crs_ids:
            yield from features
        else:
            for feature in features:
                yield self._add_crs_ids_to_feature(feature, cols_to_crs_ids)

    def _add_crs_ids_to_feature(self, feature, cols_to_crs_ids):
        for col_name, crs_id in cols_to_crs_ids.items():
            geometry = feature[col_name]
            if geometry is not None:
                feature[col_name] = geometry.with_crs_id(crs_id)
        return feature

    def _cols_to_crs_ids(self):
        result = {}
        for col in self.schema.geometry_columns:
            crs_name = col.get("geometryCRS", None)
            crs_id = crs_util.get_identifier_int_from_dataset(self, crs_name)
            if crs_id:
                result[col.name] = crs_id
        return result

    @functools.lru_cache()
    def get_geometry_transform(self, target_crs):
        """
        Find the transform to reproject this dataset into the target CRS.
        Returns None if the CRS for this dataset is unknown.
        """
        crs_definition = self.get_crs_definition()
        if crs_definition is None:
            return None
        try:
            src_crs = crs_util.make_crs(crs_definition)
            return osr.CoordinateTransformation(src_crs, target_crs)
        except RuntimeError as e:
            raise InvalidOperation(
                f"Can't reproject dataset {self.path!r} into target CRS: {e}"
            )

    def diff(
        self,
        other,
        ds_filter=DatasetKeyFilter.MATCH_ALL,
        reverse=False,
        diff_format=DiffFormat.FULL,
    ):
        """
        Generates a Diff from self -> other.
        If reverse is true, generates a diff from other -> self.
        """
        ds_diff = super().diff(other, ds_filter=ds_filter, reverse=reverse)
        feature_filter = ds_filter.get("feature", ds_filter.child_type())

        # If the user is asking for a no data changes diff, just check if the feature subtree is different.
        if diff_format == DiffFormat.NO_DATA_CHANGES:
            self_subtree = self.get_subtree("feature")
            other_subtree = other.get_subtree("feature") if other else self._empty_tree
            data_changes = self_subtree != other_subtree

            ds_diff["data_changes"]: bool = data_changes

        # Else do a full diff.
        else:
            ds_diff["feature"] = DeltaDiff(
                self.diff_feature(other, feature_filter, reverse=reverse)
            )
        return ds_diff

    def diff_to_working_copy(
        self,
        workdir_diff_cache,
        ds_filter=DatasetKeyFilter.MATCH_ALL,
        *,
        convert_to_dataset_format=None,
    ):
        table_wc = self.repo.working_copy.tabular
        if table_wc is None:
            return DatasetDiff()
        return table_wc.diff_dataset_to_working_copy(self, ds_filter)

    def diff_feature(
        self, other, feature_filter=FeatureKeyFilter.MATCH_ALL, reverse=False
    ):
        """
        Yields feature deltas from self -> other, but only for features that match the feature_filter.
        If reverse is true, yields feature deltas from other -> self.
        """
        yield from self.diff_subtree(
            other,
            "feature",
            key_filter=feature_filter,
            key_decoder_method="decode_path_to_1pk",
            value_decoder_method="get_feature_promise_from_path",
            reverse=reverse,
        )

    def get_feature_promise_from_path(self, feature_path):
        feature_blob = self.get_blob_at(feature_path)
        return functools.partial(self.get_feature_from_blob, feature_blob)

    def apply_diff(
        self, dataset_diff, object_builder, *, resolve_missing_values_from_ds=None
    ):
        """
        Given a diff that only affects this dataset, write it to the given treebuilder.
        Blobs will be created in the repo, and referenced in the resulting tree, but
        no commit is created - this is the responsibility of the caller.
        """
        # TODO - support multiple primary keys.
        meta_diff = dataset_diff.get("meta")
        schema = None
        if meta_diff:
            self.apply_meta_diff(
                meta_diff,
                object_builder,
                resolve_missing_values_from_ds=resolve_missing_values_from_ds,
            )

            if "schema.json" in meta_diff and meta_diff["schema.json"].new_value:
                schema = Schema(meta_diff["schema.json"].new_value)

        feature_diff = dataset_diff.get("feature")
        if feature_diff:
            self.apply_feature_diff(
                feature_diff,
                object_builder,
                schema=schema,
                resolve_missing_values_from_ds=resolve_missing_values_from_ds,
            )

    def apply_meta_diff(
        self, meta_diff, object_builder, *, resolve_missing_values_from_ds=None
    ):
        """Applies a meta diff. Not supported until Datasets V2"""
        if not meta_diff:
            return

        raise NotYetImplemented(
            f"Meta changes are not supported for version {self.version}"
        )

    def check_feature_insertion_for_conflicts(
        self,
        delta,
        *,
        new_path,
        schema_changed_since_patch,
        resolve_missing_values_from_ds,
    ):
        """
        Given a delta with no old value, checks for conflicts.

        Returns a boolean indicating whether there was a conflict.

        A conflict occurs if either:
            * a feature was already inserted with the same primary key value.
            * this is a minimal style patch and the 'insertion' is actually an edit,
              and the feature in the `resolve_missing_values_from_ds` dataset is not the
              same as the feature with the same PK in this current dataset.
        """

        if resolve_missing_values_from_ds is None:
            click.echo(
                f"{self.path}: Trying to create feature that already exists: {delta.new_key}",
                err=True,
            )
            return True
        feature_conflict_since_patch = False
        if schema_changed_since_patch:
            # can't use feature OID check here, since schema changes mean that two objects with
            # the same OID can actually resolve to different features.
            # So we have to call get_feature() twice for every feature
            old_feature = resolve_missing_values_from_ds.get_feature(path=new_path)
            current_feature = self.get_feature(path=new_path)
            feature_conflict_since_patch = old_feature != current_feature
        else:
            # Fast path - check old features against old features by just comparing OIDs, mostly.
            current_blob = self.inner_tree / new_path
            try:
                old_blob = resolve_missing_values_from_ds.inner_tree / new_path
            except KeyError:
                # this really was an insert. but it's a conflict, because the PK has been used
                # by a later insert (because new_path is in self.inner_tree)
                feature_conflict_since_patch = True
            else:
                old_feature = None
                current_feature = None

                if current_blob.oid != old_blob.oid:
                    # Two different blobs, but we still need to check the feature is different.
                    old_feature = resolve_missing_values_from_ds.get_feature(
                        path=new_path
                    )
                    current_feature = self.get_feature(path=new_path)
                    current_feature.update(delta.new.value)

                    feature_conflict_since_patch = old_feature != current_feature

        if feature_conflict_since_patch:
            click.echo(
                f"{self.path}: Feature was modified since patch: {delta.new_key}",
                err=True,
            )

        return feature_conflict_since_patch

    def apply_feature_diff(
        self,
        feature_diff,
        object_builder,
        *,
        schema=None,
        resolve_missing_values_from_ds=None,
        new_feature_encoder=None,
    ):
        """Applies a feature diff."""
        if not feature_diff:
            return

        schema_changed_since_patch = False
        if resolve_missing_values_from_ds is not None:
            schema_changed_since_patch = (
                resolve_missing_values_from_ds.schema != self.schema
            )

        with object_builder.chdir(self.inner_path):
            # Applying diffs works even if there is no tree yet created for the dataset,
            # as is the case when the dataset is first being created right now.
            tree = self.inner_tree or ()

            encode_kwargs = {}
            if schema is not None:
                encode_kwargs = {"schema": schema}

            has_conflicts = False
            for delta in feature_diff.values():
                old_key = delta.old_key
                new_key = delta.new_key
                old_path = (
                    self.encode_1pk_to_path(old_key, relative=True)
                    if old_key is not None
                    else None
                )
                new_path = (
                    self.encode_1pk_to_path(new_key, relative=True, **encode_kwargs)
                    if new_key is not None
                    else None
                )

                # Conflict detection
                if delta.type == "delete" and old_path not in tree:
                    has_conflicts = True
                    click.echo(
                        f"{self.path}: Trying to delete nonexistent feature: {old_key}",
                        err=True,
                    )
                    continue

                if delta.type == "insert" and new_path in tree:
                    if self.check_feature_insertion_for_conflicts(
                        delta,
                        new_path=new_path,
                        schema_changed_since_patch=schema_changed_since_patch,
                        resolve_missing_values_from_ds=resolve_missing_values_from_ds,
                    ):
                        has_conflicts = True
                        continue

                if delta.type == "update" and old_path not in tree:
                    has_conflicts = True
                    click.echo(
                        f"{self.path}: Trying to update nonexistent feature: {old_key}",
                        err=True,
                    )
                    continue

                if (
                    delta.type == "update"
                    and self.get_feature(old_key) != delta.old_value
                ):
                    has_conflicts = True
                    click.echo(
                        f"{self.path}: Trying to update already-changed feature: {old_key}",
                        err=True,
                    )
                    continue

                # Actually write the feature diff:
                if old_path and old_path != new_path:
                    object_builder.remove(old_path)
                if delta.new_value:
                    path, data = self.encode_feature(
                        delta.new.value, relative=True, **encode_kwargs
                    )
                    object_builder.insert(path, data)

            if has_conflicts:
                raise InvalidOperation(
                    "Patch does not apply",
                    exit_code=PATCH_DOES_NOT_APPLY,
                )

    def all_features_diff(
        self,
        feature_filter=FeatureKeyFilter.MATCH_ALL,
        delta_type=Delta.insert,
        flags=0,
    ):
        assert delta_type in (Delta.insert, Delta.delete)
        feature_diff = DeltaDiff()
        for blob in self.feature_blobs():
            pk = self.decode_path_to_1pk(blob.name)
            if pk not in feature_filter:
                continue
            feature_promise = functools.partial(self.get_feature_from_blob, blob)
            delta = delta_type((pk, feature_promise))
            delta.flags = flags
            feature_diff.add_delta(delta)
        return feature_diff

    def fetch_missing_dirty_features(self, working_copy):
        """Fetch all the promised features in this dataset that are marked as dirty in the working copy."""

        # Attempting this more than once in a single kart invocation will waste time and have no effect.
        if getattr(self, "fetch_missing_dirty_features_attempted", False):
            return False
        self.fetch_missing_dirty_features_attempted = True

        click.echo(f"Fetching missing but required features in {self.path}", err=True)
        dirty_pks = working_copy.get_dirty_pks(self)
        if self.schema.pk_columns[0].data_type == "integer":
            dirty_pks = (int(pk) for pk in dirty_pks)
        dirty_paths = (self.encode_1pk_to_path(pk, relative=True) for pk in dirty_pks)
        promised_blob_ids = []
        for dirty_path in dirty_paths:
            dirty_blob = None
            try:
                dirty_blob = self.inner_tree / dirty_path
                dirty_blob.size
            except KeyError as e:
                if dirty_blob is not None and object_is_promised(e):
                    promised_blob_ids.append(dirty_blob.oid.hex)
        fetch_promised_blobs(self.repo, promised_blob_ids)
