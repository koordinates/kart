import functools
import time

import click
import pygit2
from osgeo import osr

from kart import crs_util
from kart.diff_structs import DatasetDiff, Delta, DeltaDiff
from kart.exceptions import PATCH_DOES_NOT_APPLY, InvalidOperation, NotYetImplemented
from kart.key_filters import DatasetKeyFilter, FeatureKeyFilter, MetaKeyFilter
from kart.promisor_utils import fetch_promised_blobs, object_is_promised
from kart.spatial_filter import SpatialFilter

from .table_dataset import TableDataset
from .schema import Schema


class RichBaseDataset(TableDataset):
    """
    Adds extra functionality to the TableDataset.
    This is in a separate class to TableDataset so that the core functionality -
    and the missing abstract functionality that needs to be implemented - isn't lost amongst
    this more advanced functionality (which is built on top of the core functionality).

    If you are only using a TableDataset as an ImportSource, just extend TableDataset - this functionality isn't needed.
    For example, see Dataset0.
    """

    RTREE_INDEX_EXTENSIONS = ("kart-idxd", "kart-idxi")

    def features_plus_blobs(self):
        for blob in self.feature_blobs():
            yield self.get_feature(path=blob.name, data=memoryview(blob)), blob

    def features_with_crs_ids(
        self, spatial_filter=SpatialFilter.MATCH_ALL, log_progress=False
    ):
        """
        Same as table_dataset.features(), but includes the CRS ID from the schema in every Geometry object.
        By contrast, the returned Geometries from table_dataset.features() all contain a CRS ID of zero,
        so the schema must be consulted separately to learn about CRS IDs.
        """
        yield from self._add_crs_ids_to_features(
            self.features(spatial_filter, log_progress=log_progress)
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
            crs_name = col.extra_type_info.get("geometryCRS", None)
            crs_id = crs_util.get_identifier_int_from_dataset(self, crs_name)
            if crs_id:
                result[col.name] = crs_id
        return result

    def build_spatial_index(self, path):
        """
        Internal proof-of-concept method for building a spatial index across the repository.

        Uses Rtree (libspatialindex underneath): http://toblerity.org/rtree/index.html
        """
        import rtree

        if not self.has_geometry:
            raise ValueError("No geometry to index")

        def _indexer():
            t0 = time.monotonic()

            c = 0
            for feature in self.features():
                c += 1
                pk = feature[self.primary_key]
                geom = feature[self.geom_column_name]

                if geom is None:
                    continue

                e = geom.envelope(only_2d=True, calculate_if_missing=True)
                yield (pk, e, None)

                if c % 50000 == 0:
                    print(f"  {c} features... @{time.monotonic()-t0:.1f}s")

        p = rtree.index.Property()
        p.dat_extension = self.RTREE_INDEX_EXTENSIONS[0]
        p.idx_extension = self.RTREE_INDEX_EXTENSIONS[1]
        p.leaf_capacity = 1000
        p.fill_factor = 0.9
        p.overwrite = True
        p.dimensionality = 2

        t0 = time.monotonic()
        idx = rtree.index.Index(path, _indexer(), properties=p, interleaved=False)
        t1 = time.monotonic()
        b = idx.bounds
        c = idx.count(b)
        del idx
        t2 = time.monotonic()
        print(f"Indexed {c} features ({b}) in {t1-t0:.1f}s; flushed in {t2-t1:.1f}s")

    def get_spatial_index(self, path):
        """
        Retrieve a spatial index built with build_spatial_index().

        Query with .nearest(coords), .intersection(coords), .count(coords)
        http://toblerity.org/rtree/index.html
        """
        import rtree

        p = rtree.index.Property()
        p.dat_extension = self.RTREE_INDEX_EXTENSIONS[0]
        p.idx_extension = self.RTREE_INDEX_EXTENSIONS[1]

        idx = rtree.index.Index(path, properties=p)
        return idx

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

    def diff(self, other, ds_filter=DatasetKeyFilter.MATCH_ALL, reverse=False):
        """
        Generates a Diff from self -> other.
        If reverse is true, generates a diff from other -> self.
        """
        ds_diff = DatasetDiff()
        meta_filter = ds_filter.get("meta", ds_filter.child_type())
        ds_diff["meta"] = self.diff_meta(other, meta_filter, reverse=reverse)
        feature_filter = ds_filter.get("feature", ds_filter.child_type())
        ds_diff["feature"] = DeltaDiff(
            self.diff_feature(other, feature_filter, reverse=reverse)
        )
        return ds_diff

    def diff_meta(self, other, meta_filter=MetaKeyFilter.MATCH_ALL, reverse=False):
        """
        Returns a diff from self -> other, but only for meta items.
        If reverse is true, generates a diff from other -> self.
        """
        if reverse:
            old, new = other, self
        else:
            old, new = self, other

        meta_old = (
            {k: v for k, v in old.meta_items().items() if k in meta_filter}
            if old
            else {}
        )
        meta_new = (
            {k: v for k, v in new.meta_items().items() if k in meta_filter}
            if new
            else {}
        )
        return DeltaDiff.diff_dicts(meta_old, meta_new)

    _INSERT_UPDATE_DELETE = (
        pygit2.GIT_DELTA_ADDED,
        pygit2.GIT_DELTA_MODIFIED,
        pygit2.GIT_DELTA_DELETED,
    )
    _INSERT_UPDATE = (pygit2.GIT_DELTA_ADDED, pygit2.GIT_DELTA_MODIFIED)
    _UPDATE_DELETE = (pygit2.GIT_DELTA_MODIFIED, pygit2.GIT_DELTA_DELETED)

    def diff_feature(
        self, other, feature_filter=FeatureKeyFilter.MATCH_ALL, reverse=False
    ):
        """
        Yields feature deltas from self -> other, but only for features that match the feature_filter.
        If reverse is true, yields feature deltas from other -> self.
        """
        params = {"flags": pygit2.GIT_DIFF_SKIP_BINARY_CHECK}
        if reverse:
            params["swap"] = True

        if other is None:
            diff_index = self.inner_tree.diff_to_tree(**params)
            self.L.debug(
                "diff (%s -> None / %s): %s changes",
                self.inner_tree.id,
                "R" if reverse else "F",
                len(diff_index),
            )
        else:
            diff_index = self.inner_tree.diff_to_tree(other.inner_tree, **params)
            self.L.debug(
                "diff (%s -> %s / %s): %s changes",
                self.inner_tree.id,
                other.inner_tree.id,
                "R" if reverse else "F",
                len(diff_index),
            )
        # TODO - call diff_index.find_similar() to detect renames.

        if reverse:
            old, new = other, self
        else:
            old, new = self, other

        for d in diff_index.deltas:
            if d.old_file and not d.old_file.path.startswith(self.FEATURE_PATH):
                continue
            elif d.new_file and not d.new_file.path.startswith(self.FEATURE_PATH):
                continue

            self.L.debug(
                "diff(): %s %s %s", d.status_char(), d.old_file.path, d.new_file.path
            )

            if d.status not in self._INSERT_UPDATE_DELETE:
                # RENAMED, COPIED, IGNORED, TYPECHANGE, UNMODIFIED, UNREADABLE, UNTRACKED
                raise NotImplementedError(f"Delta status: {d.status_char()}")

            if d.status in self._UPDATE_DELETE:
                old_path = d.old_file.path
                old_pk = old.decode_path_to_1pk(old_path)
            else:
                old_pk = None

            if d.status in self._INSERT_UPDATE:
                new_path = d.new_file.path
                new_pk = new.decode_path_to_1pk(d.new_file.path)
            else:
                new_pk = None

            if str(old_pk) not in feature_filter and str(new_pk) not in feature_filter:
                continue

            if d.status == pygit2.GIT_DELTA_ADDED:
                self.L.debug("diff(): insert %s (%s)", new_path, new_pk)
            elif d.status == pygit2.GIT_DELTA_MODIFIED:
                self.L.debug(
                    "diff(): update %s %s -> %s %s",
                    old_path,
                    old_pk,
                    new_path,
                    new_pk,
                )
            elif d.status == pygit2.GIT_DELTA_DELETED:
                self.L.debug("diff(): delete %s %s", old_path, old_pk)

            if d.status in self._UPDATE_DELETE:
                old_feature_blob = old.get_blob_at(old_path)
                old_feature_promise = functools.partial(
                    old.get_feature_from_blob, old_feature_blob
                )
                old_half_delta = old_pk, old_feature_promise
            else:
                old_half_delta = None

            if d.status in self._INSERT_UPDATE:
                new_feature_blob = new.get_blob_at(new_path)
                new_feature_promise = functools.partial(
                    new.get_feature_from_blob, new_feature_blob
                )
                new_half_delta = new_pk, new_feature_promise
            else:
                new_half_delta = None

            yield Delta(old_half_delta, new_half_delta)

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
                schema = Schema.from_column_dicts(meta_diff["schema.json"].new_value)

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
                    self.encode_1pk_to_path(new_key, relative=True)
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
