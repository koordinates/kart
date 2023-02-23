import contextlib
import logging
import os
import time
from datetime import datetime
from pathlib import Path

import click
from osgeo import gdal

import sqlalchemy as sa
from kart import crs_util
from kart.exceptions import InvalidOperation
from kart import meta_items
from kart.sqlalchemy import text_with_inlined_params
from kart.sqlalchemy.adapter.gpkg import KartAdapter_GPKG
from kart.schema import Schema
from sqlalchemy.dialects.sqlite.base import SQLiteIdentifierPreparer
from sqlalchemy.orm import sessionmaker

from . import TableWorkingCopyStatus
from .base import TableWorkingCopy
from .table_defs import GpkgKartTables, GpkgTables

L = logging.getLogger("kart.tabular.working_copy.gpkg")


class WorkingCopy_GPKG(TableWorkingCopy):
    """
    GPKG working copy implementation.

    Requirements:
    1. Can read and write to the filesystem at the specified path.
    """

    WORKING_COPY_TYPE_NAME = "GPKG"

    SUPPORTED_META_ITEMS = (
        meta_items.TITLE,
        meta_items.DESCRIPTION,
        meta_items.SCHEMA_JSON,
        meta_items.CRS_DEFINITIONS,
    )

    def __init__(self, repo, location):
        self.repo = repo
        self.path = self.location = location
        self.adapter = KartAdapter_GPKG
        self.engine = self.adapter.create_engine(
            self.full_path,
            # Don't prevent us from re-using the connections in new threads.
            # This is here to support `kart diff -o json-lines --add-feature-count-estimate=...`,
            # which runs a thread to insert the estimate into the output.
            # That's safe because the workingcopy connection there never writes.
            # If we ever need to add threads which might write simultaneously, we'll need to turn
            # this back on.
            connect_args={"check_same_thread": False},
        )
        self.sessionmaker = sessionmaker(bind=self.engine)
        self.preparer = SQLiteIdentifierPreparer(self.engine.dialect)

        self.db_schema = None
        self.kart_tables = GpkgKartTables(repo.is_kart_branded)

    @classmethod
    def check_valid_creation_location(cls, wc_location, repo):
        cls.check_valid_location(wc_location, repo)

        gpkg_path = (repo.workdir_path / wc_location).resolve()
        if gpkg_path.exists():
            desc = "path" if gpkg_path.is_dir() else "GPKG file"
            raise InvalidOperation(
                f"Error creating GPKG working copy at {wc_location} - {desc} already exists"
            )

    @classmethod
    def clearly_doesnt_exist(cls, wc_location, repo):
        gpkg_path = (repo.workdir_path / wc_location).resolve()
        return not gpkg_path.exists()

    @classmethod
    def check_valid_location(cls, wc_location, repo):
        if not str(wc_location).endswith(".gpkg"):
            suggested_path = f"{os.path.splitext(str(wc_location))[0]}.gpkg"
            raise click.UsageError(
                f"Invalid GPKG path - expected .gpkg suffix, eg {suggested_path}"
            )

    @classmethod
    def normalise_location(cls, wc_location, repo):
        """Rewrites a relative path (relative to the current directory) as relative to the repo.workdir_path."""
        gpkg_path = Path(wc_location)
        if not gpkg_path.is_absolute():
            try:
                return str(
                    os.path.relpath(gpkg_path.resolve(), repo.workdir_path.resolve())
                )
            except ValueError:
                pass
        return str(gpkg_path)

    @property
    def full_path(self):
        """Return a full absolute path to the working copy"""
        return (self.repo.workdir_path / self.path).resolve()

    @property
    def _tracking_table_requires_cast(self):
        return False

    @contextlib.contextmanager
    def session(
        self,
    ):
        """
        Context manager for GeoPackage DB sessions, yields a connection object inside a transaction

        Calling again yields the _same_ connection, the transaction/etc only happen in the outer one.
        """
        L = logging.getLogger(f"{self.__class__.__qualname__}.session")

        if hasattr(self, "_session"):
            # Inner call - reuse existing session.
            L.debug("session: existing...")
            yield self._session
            L.debug("session: existing/done")
            return

        # Outer call - create new session:
        L.debug("session: new...")
        self._session = self.sessionmaker()

        try:
            # TODO - use tidier syntax for opening transactions from sqlalchemy.
            self._session.execute("BEGIN TRANSACTION;")
            yield self._session
            self._session.commit()

        except Exception:
            self._session.rollback()
            raise
        finally:
            self._session.close()
            del self._session
            L.debug("session: new/done")

    def delete(self, keep_db_schema_if_possible=False):
        """Delete the working copy files."""
        self.full_path.unlink()

        # for sqlite this might include wal/journal/etc files
        # app.gpkg -> app.gpkg-wal, app.gpkg-journal
        # https://www.sqlite.org/shortnames.html
        for f in Path(self.full_path).parent.glob(Path(self.path).name + "-*"):
            f.unlink()

    def status(self, check_if_dirty=False, allow_unconnectable=False):
        result = 0
        if not self.full_path.is_file():
            return result

        result |= TableWorkingCopyStatus.FILE_EXISTS
        with self.session() as sess:
            kart_table_count = sess.scalar(
                """
                SELECT count(*) FROM sqlite_master
                WHERE type='table' AND name IN (:kart_state_name, :kart_track_name);
                """,
                {
                    "kart_state_name": self.KART_STATE_NAME,
                    "kart_track_name": self.KART_TRACK_NAME,
                },
            )
            user_table_count = sess.scalar(
                """SELECT count(*) FROM sqlite_master WHERE type='table' AND NAME NOT LIKE 'gpkg%';"""
            )
            if kart_table_count or user_table_count:
                result |= TableWorkingCopyStatus.NON_EMPTY
            if kart_table_count == 2:
                result |= TableWorkingCopyStatus.INITIALISED
            if user_table_count:
                result |= TableWorkingCopyStatus.HAS_DATA

        if (
            (TableWorkingCopyStatus.INITIALISED & result)
            and check_if_dirty
            and self.is_dirty()
        ):
            result |= TableWorkingCopyStatus.DIRTY

        return result

    def create_and_initialise(self):
        with self.session() as sess:
            # Create standard GPKG tables:
            GpkgTables().create_all(sess)
            # Create Kart-specific tables:
            self.kart_tables.create_all(sess)

    def _create_table_for_dataset(self, sess, dataset):
        table_spec = self.adapter.v2_schema_to_sql_spec(dataset.schema, dataset)

        sess.execute(
            f"""CREATE TABLE {self.table_identifier(dataset)} ({table_spec});"""
        )

    def _identifier_already_used(self, sess, table_name, identifier):
        # Returns truthy value if gpkg_contents is already using this identifier
        # for a different table. Identifiers must be UNIQUE.
        return sess.scalar(
            sa.select([sa.func.count()])
            .select_from(GpkgTables.gpkg_contents)
            .where(
                sa.and_(
                    GpkgTables.gpkg_contents.c.identifier == identifier,
                    GpkgTables.gpkg_contents.c.table_name != table_name,
                )
            )
        )

    def _identifier_prefix(self, dataset):
        # Prefixes an identifier to make sure it is unique - if needed.
        # User-visible so we return a sensible prefix.
        return f"{dataset.table_name}: "

    def _write_meta(self, sess, dataset):
        """
        Populate the following tables with data from this dataset:
        gpkg_contents, gpkg_geometry_columns, gpkg_spatial_ref_sys, gpkg_metadata, gpkg_metadata_reference
        """
        table_name = dataset.table_name
        gpkg_meta_items = dict(
            KartAdapter_GPKG.all_gpkg_meta_items(dataset, table_name)
        )
        gpkg_contents = gpkg_meta_items["gpkg_contents"]
        gpkg_contents["table_name"] = table_name
        gpkg_geometry_columns = gpkg_meta_items.get("gpkg_geometry_columns")
        gpkg_spatial_ref_sys = gpkg_meta_items.get("gpkg_spatial_ref_sys")

        with self.session() as sess:
            # Update GeoPackage core tables
            if gpkg_spatial_ref_sys:
                sess.execute(
                    GpkgTables.gpkg_spatial_ref_sys.insert().prefix_with("OR REPLACE"),
                    gpkg_spatial_ref_sys,
                )

            new_identifier = gpkg_contents["identifier"]
            if self._identifier_already_used(sess, table_name, new_identifier):
                # Prefix the identifier with table_name in case of conflict.
                gpkg_contents["identifier"] = (
                    self._identifier_prefix(dataset) + new_identifier
                )

            # Our repo copy doesn't include all fields from gpkg_contents
            # but the default value for last_change (now), and NULL for {min_x,max_x,min_y,max_y}
            # should deal with the remaining fields.
            sess.execute(
                GpkgTables.gpkg_contents.insert().prefix_with("OR REPLACE"),
                gpkg_contents,
            )

            if gpkg_geometry_columns:
                sess.execute(
                    GpkgTables.gpkg_geometry_columns.insert().prefix_with("OR REPLACE"),
                    gpkg_geometry_columns,
                )

            gpkg_metadata = gpkg_meta_items.get("gpkg_metadata")
            gpkg_metadata_reference = gpkg_meta_items.get("gpkg_metadata_reference")
            if gpkg_metadata and gpkg_metadata_reference:
                self._write_meta_metadata(
                    sess, table_name, gpkg_metadata, gpkg_metadata_reference
                )

    def _write_meta_metadata(
        self,
        sess,
        table_name,
        gpkg_metadata,
        gpkg_metadata_reference,
    ):
        """Populate gpkg_metadata and gpkg_metadata_reference tables."""
        # gpkg_metadata_reference.md_file_id is a foreign key -> gpkg_metadata.id,
        # have to make sure these IDs still match once we insert.
        metadata_id_map = {}
        for row in gpkg_metadata:
            params = dict(row.items())
            params.pop("id")

            r = sess.execute(GpkgTables.gpkg_metadata.insert(), params)
            metadata_id_map[row["id"]] = r.lastrowid

        for row in gpkg_metadata_reference:
            params = dict(row.items())
            params["md_file_id"] = metadata_id_map[row["md_file_id"]]
            params["md_parent_id"] = metadata_id_map.get(row["md_parent_id"], None)
            params["table_name"] = table_name

            sess.execute(GpkgTables.gpkg_metadata_reference.insert(), params)

    # Some types are approximated as text in GPKG - see super()._remove_hidden_meta_diffs
    @classmethod
    def try_align_schema_col(cls, old_col_dict, new_col_dict):
        old_type = old_col_dict["dataType"]
        new_type = new_col_dict["dataType"]

        # Some types have to be approximated as other types in GPKG, and they also lose any extra type info.
        if KartAdapter_GPKG.APPROXIMATED_TYPES.get(old_type) == new_type:
            new_col_dict["dataType"] = new_type = old_type
            for key in KartAdapter_GPKG.APPROXIMATED_TYPES_EXTRA_TYPE_INFO:
                new_col_dict[key] = old_col_dict.get(key)

        # GPKG can't store a certain type of timestamp:
        if old_type == "timestamp":
            old_timezone = old_col_dict.get("timezone")
            if (
                KartAdapter_GPKG.APPROXIMATED_TYPES.get((old_type, old_timezone))
                == new_type
            ):
                new_col_dict["dataType"] = new_type = old_type
                new_col_dict["timezone"] = old_timezone

        # GPKG primary keys have to be int64, so we approximate int8, int16, int32 primary keys as int64s.
        if old_type == "integer" and new_type == "integer":
            if new_col_dict.get("size") != old_col_dict.get("size"):
                if new_col_dict.get("primaryKeyIndex") is not None:
                    new_col_dict["size"] = old_col_dict.get("size")

        return new_type == old_type

    def _remove_hidden_meta_diffs(self, dataset, ds_meta_items, wc_meta_items):
        # Fix up anything we may have done to the primary key before calling super()
        if "schema.json" in ds_meta_items and "schema.json" in wc_meta_items:
            wc_meta_items["schema.json"] = self._restore_approximated_primary_key(
                ds_meta_items["schema.json"], wc_meta_items["schema.json"]
            )

        if "title" in wc_meta_items:
            wc_meta_items["title"] = self._restore_approximated_title(
                dataset, ds_meta_items.get("title"), wc_meta_items["title"]
            )

        super()._remove_hidden_meta_diffs(dataset, ds_meta_items, wc_meta_items)

    def _restore_approximated_title(self, dataset, ds_title, wc_title):
        # If the identifier column has non-unique values, we have to prefix them in GPKG.
        # We remove the prefixes in the remove_hidden_meta_diffs step.
        prefix = self._identifier_prefix(dataset)
        if wc_title.startswith(prefix) and not (ds_title or "").startswith(prefix):
            wc_title = wc_title[len(prefix) :]
        return wc_title or None

    def _restore_approximated_primary_key(self, ds_schema, wc_schema):
        """
        GPKG requires that there is a primary key of type INTEGER (int64) in geospatial tables.
        If the dataset had a primary key of another type, then this will have been approximated.
        If the PK remains this same type when roundtripped, we remove this diff and treat it as the same.
        """
        ds_pk = ds_schema.first_pk_column
        wc_pk = wc_schema.first_pk_column
        if not ds_pk or not wc_pk:
            return wc_schema

        if wc_pk["dataType"] != "integer":
            # This isn't a compliant GPKG that we would create, maybe the user edited it.
            # Keep the diff as it is.
            return wc_schema

        if ds_pk["dataType"] == "integer":
            if wc_pk.get("size") == ds_pk.get("size"):
                return wc_schema

            # Dataset PK type of int8, int16, int32 was approximated as int64.
            # Restore it to its original size
            wc_schema = list(wc_schema)
            index = wc_schema.index(wc_pk)
            wc_schema[index] = dict(wc_schema[index])
            wc_schema[index]["size"] = ds_pk.get("size")
            return Schema(wc_schema)
        else:
            # Dataset PK has non-integer PK type, which would be approximated by demoting it to a non-PK
            # and adding another column of type INTEGER PK that is not found in the dataset.
            # If the working copy still has this structure, restore the original PK as a PK.
            demoted_pk = wc_schema.get_by_name(ds_pk["name"])
            if demoted_pk is None or demoted_pk["dataType"] != ds_pk["dataType"]:
                return wc_schema

            # Remove auto-generated PK column
            wc_schema = list(wc_schema)
            wc_schema.remove(wc_pk)
            # Restore demoted-PK as PK again
            index = wc_schema.index(demoted_pk)
            wc_schema[index] = dict(wc_schema[index])
            wc_schema[index]["primaryKeyIndex"] = 0
            return Schema(wc_schema)

    def _is_builtin_crs(self, crs):
        auth_name, auth_code = crs_util.parse_authority(crs)
        return auth_name == "EPSG" and auth_code == "4326"

    def _find_by_name(self, schema_cols, name):
        return next((c for c in schema_cols if c["name"] == name), None)

    def _delete_meta(self, sess, dataset):
        table_name = dataset.table_name
        with self.session() as sess:
            self._delete_meta_metadata(sess, table_name)

            # FOREIGN KEY constraints are still active, so we delete in a particular order:
            for table in (GpkgTables.gpkg_geometry_columns, GpkgTables.gpkg_contents):
                sess.execute(
                    sa.delete(table).where(table.c.table_name == dataset.table_name)
                )

            # Delete CRS's that are no longer referenced (and are not built-in to GPKG).
            table = GpkgTables.gpkg_spatial_ref_sys
            sess.execute(
                sa.delete(table).where(
                    sa.not_(
                        sa.or_(
                            table.c.srs_id.in_(
                                sa.select(GpkgTables.gpkg_contents.c.srs_id)
                            ),
                            table.c.srs_id.in_([-1, 0, 4326]),
                        )
                    )
                )
            )

    def _delete_meta_metadata(self, sess, table_name):
        r = sess.execute(
            KartAdapter_GPKG.METADATA_QUERY.format(select="M.id"),
            {"table_name": table_name},
        )
        ids = [row[0] for row in r]
        if not ids:
            return

        table = GpkgTables.gpkg_metadata_reference
        sess.execute(sa.delete(table).where(table.c.md_file_id.in_(ids)))
        table = GpkgTables.gpkg_metadata
        sess.execute(sa.delete(table).where(table.c.id.in_(ids)))

    def _create_spatial_index_pre(self, sess, dataset):
        # Implementing only _create_spatial_index_pre:
        # gpkgAddSpatialIndex has to be called before writing any features,
        # since it only adds on-write triggers to update the index - it doesn't
        # add any pre-existing features to the index.

        # Generally, there shouldn't be an existing spatial index at this stage.
        # But if there is, we should clean it up and start over.
        self._drop_spatial_index(sess, dataset)

        L = logging.getLogger(f"{self.__class__.__qualname__}._create_spatial_index")
        geom_col = dataset.geom_column_name

        # Create the GeoPackage Spatial Index
        t0 = time.monotonic()
        L.debug("Creating spatial index for %s.%s", dataset.table_name, geom_col)

        sess.execute(
            "SELECT gpkgAddSpatialIndex(:table, :geom);",
            {"table": dataset.table_name, "geom": geom_col},
        )

        L.info("Created spatial index in %.1fs", time.monotonic() - t0)

    def _drop_spatial_index(self, sess, dataset):
        L = logging.getLogger(f"{self.__class__.__qualname__}._drop_spatial_index")
        geom_col = dataset.geom_column_name

        # Delete the GeoPackage Spatial Index
        t0 = time.monotonic()
        L.debug("Dropping spatial index for %s.%s", dataset.table_name, geom_col)

        rtree_table = f"rtree_{dataset.table_name}_{geom_col}"
        sess.execute(f"DROP TABLE IF EXISTS {self.quote(rtree_table)};")
        # For some reason, gpkg_extensions doesn't accurately preserve the case of these
        # fields, so we use the LIKE operator to make sure we delete the right entry.
        sess.execute(
            """
            DELETE FROM gpkg_extensions WHERE extension_name = 'gpkg_rtree_index'
            AND table_name LIKE :table_name AND column_name LIKE :column_name;
            """,
            {"table_name": dataset.table_name, "column_name": geom_col},
        )

        L.info("Dropped spatial index in %.1fs", time.monotonic() - t0)

    def _initialise_sequence(self, sess, dataset):
        start = dataset.feature_path_encoder.find_start_of_unassigned_range(dataset)
        if start:
            # Strangely, sqlite_sequence has no PK or unique constraints, so we just delete and then insert.
            sess.execute(
                "DELETE FROM sqlite_sequence WHERE name = :table_name;",
                {"table_name": dataset.table_name},
            )
            sess.execute(
                "INSERT INTO sqlite_sequence (name, seq) VALUES (:table_name, :seq)",
                {"table_name": dataset.table_name, "seq": start - 1},
            )

    def _sno_tracking_name(self, trigger_type, dataset):
        assert trigger_type in ("ins", "upd", "del", "ntbl")
        assert dataset is not None
        # This is how the triggers are named in Sno 0.8.0 and earlier.
        # Newer repos that use kart branding use _kart_tracking_name.
        return f"gpkg_sno_{dataset.table_name}_{trigger_type}"

    def _create_triggers(self, sess, dataset):
        table_identifier = self.table_identifier(dataset)
        pk_column = self.quote(dataset.primary_key)

        # Placeholders not allowed in CREATE TRIGGER - have to use text_with_inlined_params.
        sess.execute(
            text_with_inlined_params(
                f"""
                CREATE TRIGGER {self._quoted_tracking_name('ins', dataset)}
                   AFTER INSERT ON {table_identifier}
                BEGIN
                    INSERT OR REPLACE INTO {self.KART_TRACK} (table_name, pk)
                    VALUES (:table_name, NEW.{pk_column});
                END;
                """,
                {"table_name": dataset.table_name},
            )
        )

        sess.execute(
            text_with_inlined_params(
                f"""
                CREATE TRIGGER {self._quoted_tracking_name('upd', dataset)}
                   AFTER UPDATE ON {table_identifier}
                BEGIN
                    INSERT OR REPLACE INTO {self.KART_TRACK} (table_name, pk)
                    VALUES (:table_name1, NEW.{pk_column}), (:table_name2, OLD.{pk_column});
                END;
                """,
                {"table_name1": dataset.table_name, "table_name2": dataset.table_name},
            )
        )

        sess.execute(
            text_with_inlined_params(
                f"""
                CREATE TRIGGER {self._quoted_tracking_name('del', dataset)}
                   AFTER DELETE ON {table_identifier}
                BEGIN
                    INSERT OR REPLACE INTO {self.KART_TRACK} (table_name, pk)
                    VALUES (:table_name, OLD.{pk_column});
                END;
                """,
                {"table_name": dataset.table_name},
            )
        )

        sess.execute(
            text_with_inlined_params(
                f"""
                CREATE TRIGGER {self._quoted_tracking_name('ntbl', dataset)}
                   AFTER CREATE TABLE ON {self.GPKG_CONTENTS}
                BEGIN
                    INSERT INTO {self.KART_TRACK} (table_name, pk)
                    VALUES (:table_name, {pk_column});
                END;
                """,
                {"table_name": dataset.table_name},
            )
        )

    def _drop_triggers(self, sess, dataset):
        sess.execute(f"DROP TRIGGER {self._quoted_tracking_name('ins', dataset)}")
        sess.execute(f"DROP TRIGGER {self._quoted_tracking_name('upd', dataset)}")
        sess.execute(f"DROP TRIGGER {self._quoted_tracking_name('del', dataset)}")

    @contextlib.contextmanager
    def _suspend_triggers(self, sess, dataset):
        self._drop_triggers(sess, dataset)
        yield
        self._create_triggers(sess, dataset)

    def _is_schema_update_supported(self, schema_delta):
        if not schema_delta.old_value or not schema_delta.new_value:
            return False

        old_schema = Schema(schema_delta.old_value)
        new_schema = Schema(schema_delta.new_value)
        dt = old_schema.diff_type_counts(new_schema)
        # We do support name_updates, but we don't support any other type of schema update
        # - except by rewriting the entire table.
        dt.pop("name_updates")
        return sum(dt.values()) == 0

    def _apply_meta_title(self, sess, dataset, src_value, dest_value):
        table_name = dataset.table_name
        if self._identifier_already_used(sess, table_name, dest_value):
            # Prefix the identifier with the table name in case of conflict:
            dest_value = self._identifier_prefix(dataset) + dest_value
        sess.execute(
            """UPDATE gpkg_contents SET identifier = :identifier WHERE table_name = :table_name""",
            {"identifier": dest_value, "table_name": table_name},
        )

    def _apply_meta_description(self, sess, dataset, src_value, dest_value):
        sess.execute(
            """UPDATE gpkg_contents SET description = :description WHERE table_name = :table_name""",
            {"description": dest_value, "table_name": dataset.table_name},
        )

    def _apply_meta_schema_json(self, sess, dataset, src_value, dest_value):
        src_schema = Schema(src_value)
        dest_schema = Schema(dest_value)

        diff_types = src_schema.diff_types(dest_schema)
        name_updates = diff_types.pop("name_updates")
        if any(dt for dt in diff_types.values()):
            raise RuntimeError(
                f"This schema change not supported by update - should be drop + rewrite_full: {diff_types}"
            )

        for col_id in name_updates:
            src_name = src_schema[col_id].name
            dest_name = dest_schema[col_id].name
            sess.execute(
                f"""
                ALTER TABLE {self.table_identifier(dataset)}
                RENAME COLUMN {self.quote(src_name)} TO {self.quote(dest_name)}
                """
            )

    def _apply_meta_metadata_xml(self, sess, dataset, src_value, dest_value):
        table = dataset.table_name
        self._delete_meta_metadata(sess, table)
        if dest_value:
            gpkg_metadata = KartAdapter_GPKG.xml_to_gpkg_metadata(dest_value, table)
            gpkg_metadata_reference = KartAdapter_GPKG.xml_to_gpkg_metadata(
                dest_value, table, reference=True
            )
            self._write_meta_metadata(
                sess, table, gpkg_metadata, gpkg_metadata_reference
            )

    def _apply_meta_metadata_dataset_json(self, sess, dataset, src_value, dest_value):
        table = dataset.table_name
        self._delete_meta_metadata(sess, table)
        if dest_value:
            gpkg_metadata = KartAdapter_GPKG.json_to_gpkg_metadata(dest_value, table)
            gpkg_metadata_reference = KartAdapter_GPKG.json_to_gpkg_metadata(
                dest_value, table, reference=True
            )
            self._write_meta_metadata(
                sess, table, gpkg_metadata, gpkg_metadata_reference
            )

    def _update_last_write_time(self, sess, dataset, commit=None):
        self._update_gpkg_contents(sess, dataset, commit)

    def _get_geom_extent(self, sess, dataset, default=None):
        """Returns the envelope around the entire dataset as (min_x, min_y, max_x, max_y)."""
        # FIXME: Why doesn't Extent(geom) work here as an aggregate?
        geom_col = dataset.geom_column_name
        r = sess.execute(
            f"""
            WITH _E AS (
                SELECT Extent({self.quote(geom_col)}) AS extent FROM {self.table_identifier(dataset)}
            )
            SELECT ST_MinX(extent), ST_MinY(extent), ST_MaxX(extent), ST_MaxY(extent) FROM _E;
            """
        )
        result = r.fetchone()
        return default if result == (None, None, None, None) else result

    def _update_gpkg_contents(self, sess, dataset, commit=None):
        """
        Update the metadata for the given table in gpkg_contents to have the new bounding-box / last-updated timestamp.
        """
        if commit:
            change_time = datetime.utcfromtimestamp(commit.commit_time)
        else:
            change_time = datetime.utcnow()
        # GPKG Spec Req. 15:
        gpkg_change_time = change_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        geom_col = dataset.geom_column_name
        if geom_col is not None:
            min_x, min_y, max_x, max_y = self._get_geom_extent(
                sess, dataset, default=(None, None, None, None)
            )
            rc = sess.execute(
                """
                UPDATE gpkg_contents
                SET (last_change, min_x, min_y, max_x, max_y) = (:last_change, :min_x, :min_y, :max_x, :max_y)
                WHERE table_name=:table_name;
                """,
                {
                    "last_change": gpkg_change_time,
                    "min_x": min_x,
                    "min_y": min_y,
                    "max_x": max_x,
                    "max_y": max_y,
                    "table_name": dataset.table_name,
                },
            ).rowcount
        else:
            rc = sess.execute(
                """UPDATE gpkg_contents SET last_change=:last_change WHERE table_name=:table_name;""",
                {"last_change": gpkg_change_time, "table_name": dataset.table_name},
            ).rowcount
        assert rc == 1, f"gpkg_contents update: expected 1Δ, got {rc}"
