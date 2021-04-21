import contextlib
import logging
import os
import time
from datetime import datetime
from pathlib import Path

import click
from osgeo import gdal
import sqlalchemy as sa
from sqlalchemy.dialects.sqlite.base import SQLiteIdentifierPreparer
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import UserDefinedType


from . import gpkg_adapter, WorkingCopyStatus
from .base import BaseWorkingCopy
from .table_defs import GpkgTables, GpkgKartTables
from sno.exceptions import InvalidOperation
from sno.geometry import normalise_gpkg_geom
from sno.schema import Schema
from sno.sqlalchemy import text_with_inlined_params
from sno.sqlalchemy.create_engine import gpkg_engine


L = logging.getLogger("sno.working_copy.gpkg")


class WorkingCopy_GPKG(BaseWorkingCopy):
    """
    GPKG working copy implementation.

    Requirements:
    1. Can read and write to the filesystem at the specified path.
    """

    WORKING_COPY_TYPE_NAME = "GPKG"

    def __init__(self, repo, location):
        self.repo = repo
        self.path = self.location = location
        self.engine = gpkg_engine(self.full_path)
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
                return str(gpkg_path.resolve().relative_to(repo.workdir_path.resolve()))
            except ValueError:
                pass
        return str(gpkg_path)

    @property
    def full_path(self):
        """ Return a full absolute path to the working copy """
        return (self.repo.workdir_path / self.path).resolve()

    def _type_def_for_column_schema(self, col, dataset):
        if col.data_type == "geometry":
            # This user-defined GeometryType normalises GPKG geometry to the Kart V2 GPKG geometry.
            return GeometryType
        else:
            # Don't need to specify type information for other columns at present, since we just pass through the values.
            return None

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

        result |= WorkingCopyStatus.FILE_EXISTS
        with self.session() as sess:
            sno_table_count = sess.scalar(
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
            if sno_table_count or user_table_count:
                result |= WorkingCopyStatus.NON_EMPTY
            if sno_table_count == 2:
                result |= WorkingCopyStatus.INITIALISED
            if user_table_count:
                result |= WorkingCopyStatus.HAS_DATA

        if (
            (WorkingCopyStatus.INITIALISED & result)
            and check_if_dirty
            and self.is_dirty()
        ):
            result |= WorkingCopyStatus.DIRTY

        return result

    @contextlib.contextmanager
    def _ogr_sqlite_pragma(self, pragma_statement):
        pragma_name = pragma_statement.split("=")[0]
        if pragma_name in os.environ.get("OGR_SQLITE_PRAGMA"):
            yield
            return

        if "OGR_SQLITE_PRAGMA" in os.environ:
            orig_pragma = os.environ["OGR_SQLITE_PRAGMA"]
            os.environ["OGR_SQLITE_PRAGMA"] = f"{orig_pragma},{pragma_statement}"
            yield
            os.environ["OGR_SQLITE_PRAGMA"] = orig_pragma
        else:
            os.environ["OGR_SQLITE_PRAGMA"] = pragma_statement
            yield
            del os.environ["OGR_SQLITE_PRAGMA"]

    def create_and_initialise(self):
        # GDAL: Create GeoPackage
        # GDAL: Add metadata/etc
        with self._ogr_sqlite_pragma("journal_mode=WAL"):
            gdal_driver = gdal.GetDriverByName("GPKG")
            gdal_ds = gdal_driver.Create(str(self.full_path), 0, 0, 0, gdal.GDT_Unknown)
            del gdal_ds

        with self.session() as sess:
            # Remove placeholder stuff GDAL creates
            sess.execute(
                "DELETE FROM gpkg_geometry_columns WHERE table_name='ogr_empty_table';"
            )
            sess.execute(
                "DELETE FROM gpkg_contents WHERE table_name='ogr_empty_table';"
            )
            sess.execute("DROP TABLE IF EXISTS ogr_empty_table;")

            # Create metadata tables
            GpkgTables().create_all(sess)
            self.kart_tables.create_all(sess)

    def _create_table_for_dataset(self, sess, dataset):
        table_spec = gpkg_adapter.v2_schema_to_sqlite_spec(dataset)

        # GPKG requires an integer primary key for spatial tables, so we add it in if needed:
        if dataset.has_geometry and "PRIMARY KEY" not in table_spec:
            table_spec = (
                '"auto_int_pk" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,' + table_spec
            )

        sess.execute(
            f"""CREATE TABLE {self.table_identifier(dataset)} ({table_spec});"""
        )

    def _write_meta(self, sess, dataset):
        """
        Populate the following tables with data from this dataset:
        gpkg_contents, gpkg_geometry_columns, gpkg_spatial_ref_sys, gpkg_metadata, gpkg_metadata_reference
        """
        table_name = dataset.table_name
        gpkg_meta_items = dict(gpkg_adapter.all_gpkg_meta_items(dataset, table_name))
        gpkg_contents = gpkg_meta_items["gpkg_contents"]
        gpkg_contents["table_name"] = table_name

        # FIXME: find a better way to roundtrip identifiers
        identifier_prefix = f"{dataset.table_name}: "
        if not gpkg_contents["identifier"].startswith(identifier_prefix):
            gpkg_contents["identifier"] = (
                identifier_prefix + gpkg_contents["identifier"]
            )

        gpkg_geometry_columns = gpkg_meta_items.get("gpkg_geometry_columns")
        gpkg_spatial_ref_sys = gpkg_meta_items.get("gpkg_spatial_ref_sys")

        with self.session() as sess:
            # Update GeoPackage core tables
            if gpkg_spatial_ref_sys:
                sess.execute(
                    GpkgTables.gpkg_spatial_ref_sys.insert().prefix_with("OR REPLACE"),
                    gpkg_spatial_ref_sys,
                )

            # Our repo copy doesn't include all fields from gpkg_contents
            # but the default value for last_change (now), and NULL for {min_x,max_x,min_y,max_y}
            # should deal with the remaining fields.
            sess.execute(GpkgTables.gpkg_contents.insert(), gpkg_contents)

            if gpkg_geometry_columns:
                sess.execute(
                    GpkgTables.gpkg_geometry_columns.insert(), gpkg_geometry_columns
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

    def meta_items(self, dataset):
        """
        Extract all the metadata of this GPKG and convert to dataset V2 format.
        Note that the extracted schema will not be aligned to any existing schema
        - the generated column IDs are stable, but do not necessarily match the ones in the dataset.
        Calling Schema.align_* is required to find how the columns matches the existing schema.
        """
        with self.session() as sess:
            gpkg_meta_items = dict(
                gpkg_adapter.gpkg_meta_items_from_db(sess, dataset.table_name)
            )

        gpkg_name = os.path.basename(self.path)

        # Column IDs are generated deterministically from the column contents and the current state.
        # That way, they don't vary at random if the same command is run twice in a row, but
        # they will vary as the repo state changes so that we don't accidentally generate the same ID twice
        # for two unrelated columns.
        id_salt = f"{gpkg_name} {dataset.table_name} {self.get_db_tree()}"

        yield from gpkg_adapter.all_v2_meta_items(gpkg_meta_items, id_salt=id_salt)

    # Some types are approximated as text in GPKG - see super()._remove_hidden_meta_diffs
    @classmethod
    def try_align_schema_col(cls, old_col_dict, new_col_dict):
        old_type = old_col_dict["dataType"]
        new_type = new_col_dict["dataType"]

        # Some types have to be approximated as other types in GPKG, and they also lose any extra type info.
        if gpkg_adapter.APPROXIMATED_TYPES.get(old_type) == new_type:
            new_col_dict["dataType"] = new_type = old_type
            for key in gpkg_adapter.APPROXIMATED_TYPES_EXTRA_TYPE_INFO:
                new_col_dict[key] = old_col_dict.get(key)

        # GPKG primary keys have to be int64, so we approximate int8, int16, int32 primary keys as int64s.
        if old_type == "integer" and new_type == "integer":
            if new_col_dict.get("size") != old_col_dict.get("size"):
                if new_col_dict.get("primaryKeyIndex") is not None:
                    new_col_dict["size"] = old_col_dict.get("size")

        return new_type == old_type

    def _remove_hidden_meta_diffs(self, dataset, ds_meta_items, wc_meta_items):
        # Fix up anything we may have done to the primary key before calling super()
        if (
            dataset.has_geometry
            and "schema.json" in ds_meta_items
            and "schema.json" in wc_meta_items
        ):
            self._restore_approximated_primary_key(
                ds_meta_items["schema.json"], wc_meta_items["schema.json"]
            )

        super()._remove_hidden_meta_diffs(dataset, ds_meta_items, wc_meta_items)

    def _restore_approximated_primary_key(self, ds_schema, wc_schema):
        """
        GPKG requires that there is a primary key of type INTEGER (int64) in geospatial tables.
        If the dataset had a primary key of another type, then this will have been approximated.
        If the PK remains this same type when roundtripped, we remove this diff and treat it as the same.
        """
        ds_pk = self._find_pk(ds_schema)
        wc_pk = self._find_pk(wc_schema)
        if not ds_pk or not wc_pk:
            return

        if wc_pk["dataType"] != "integer":
            # This isn't a compliant GPKG that we would create, maybe the user edited it.
            # Keep the diff as it is.
            return

        if ds_pk["dataType"] == "integer":
            # Dataset PK type of int8, int16, int32 was approximated as int64.
            # Restore it to its original size
            if wc_pk.get("size") != ds_pk.get("size"):
                wc_pk["size"] = ds_pk.get("size")
        else:
            # Dataset PK has non-integer PK type, which would be approximated by demoting it to a non-PK
            # and adding another column of type INTEGER PK that is not found in the dataset.
            # If the working copy still has this structure, restore the original PK as a PK.
            demoted_pk = self._find_by_name(wc_schema, ds_pk["name"])
            if demoted_pk and demoted_pk["dataType"] == ds_pk["dataType"]:
                # Remove auto-generated PK column
                wc_schema.remove(wc_pk)
                # Restore demoted-PK as PK again
                demoted_pk["primaryKeyIndex"] = 0

    def _find_pk(self, schema_cols):
        return next((c for c in schema_cols if c.get("primaryKeyIndex") == 0), None)

    def _find_by_name(self, schema_cols, name):
        return next((c for c in schema_cols if c["name"] == name), None)

    def delete_meta(self, dataset):
        table_name = dataset.table_name
        with self.session() as sess:
            self._delete_meta_metadata(sess, table_name)
            # FOREIGN KEY constraints are still active, so we delete in a particular order:
            sess.execute(
                """DELETE FROM gpkg_geometry_columns WHERE table_name = :table_name;""",
                {"table_name": dataset.table_name},
            )
            sess.execute(
                """DELETE FROM gpkg_contents WHERE table_name = :table_name;""",
                {"table_name": dataset.table_name},
            )

    def _delete_meta_metadata(self, sess, table_name):
        r = sess.execute(
            gpkg_adapter.METADATA_QUERY.format(select="M.id"),
            {"table_name": table_name},
        )
        ids = [row[0] for row in r]
        if not ids:
            return

        sqls = (
            """DELETE FROM gpkg_metadata_reference WHERE md_file_id IN :ids;""",
            """DELETE FROM gpkg_metadata WHERE id IN :ids;""",
        )
        for sql in sqls:
            stmt = sa.text(sql).bindparams(sa.bindparam("ids", expanding=True))
            sess.execute(stmt, {"ids": ids})

    def _create_spatial_index_pre(self, sess, dataset):
        # Implementing only _create_spatial_index_pre:
        # gpkgAddSpatialIndex has to be called before writing any features,
        # since it only adds on-write triggers to update the index - it doesn't
        # add any pre-existing features to the index.

        L = logging.getLogger(f"{self.__class__.__qualname__}._create_spatial_index")
        geom_col = dataset.geom_column_name

        # Create the GeoPackage Spatial Index
        t0 = time.monotonic()
        L.debug("Creating spatial index for %s.%s", dataset.table_name, geom_col)

        sess.execute(
            "SELECT gpkgAddSpatialIndex(:table, :geom);",
            {"table": dataset.table_name, "geom": geom_col},
        )

        L.info("Created spatial index in %ss", time.monotonic() - t0)

    def _drop_spatial_index(self, sess, dataset):
        L = logging.getLogger(f"{self.__class__.__qualname__}._drop_spatial_index")
        geom_col = dataset.geom_column_name

        # Delete the GeoPackage Spatial Index
        t0 = time.monotonic()
        L.debug("Dropping spatial index for %s.%s", dataset.table_name, geom_col)

        rtree_table = f"rtree_{dataset.table_name}_{geom_col}"
        sess.execute(f"DROP TABLE IF EXISTS {self.quote(rtree_table)};")
        sess.execute(
            f"DELETE FROM gpkg_extensions WHERE (table_name, column_name, extension_name) = (:table_name, :column_name, 'gpkg_rtree_index')",
            {"table_name": dataset.table_name, "column_name": geom_col},
        )

        L.info("Dropped spatial index in %ss", time.monotonic() - t0)

    def _sno_tracking_name(self, trigger_type, dataset):
        assert trigger_type in ("ins", "upd", "del")
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

    def _drop_triggers(self, sess, dataset):
        sess.execute(f"DROP TRIGGER {self._quoted_tracking_name('ins', dataset)}")
        sess.execute(f"DROP TRIGGER {self._quoted_tracking_name('upd', dataset)}")
        sess.execute(f"DROP TRIGGER {self._quoted_tracking_name('del', dataset)}")

    @contextlib.contextmanager
    def _suspend_triggers(self, sess, dataset):
        self._drop_triggers(sess, dataset)
        yield
        self._create_triggers(sess, dataset)

    def _is_meta_update_supported(self, dataset_version, meta_diff):
        """
        Returns True if the given meta-diff is supported *without* dropping and rewriting the table.
        (Any meta change is supported - even in datasets v1 - if we drop and rewrite the table,
        but of course it is less efficient).
        meta_diff - DeltaDiff object containing the meta changes.
        """
        if not meta_diff:
            return True

        if dataset_version < 2:
            # Dataset1 doesn't support meta changes at all - except by rewriting the entire table.
            return False

        if "schema.json" not in meta_diff:
            return True

        schema_delta = meta_diff["schema.json"]
        if not schema_delta.old_value or not schema_delta.new_value:
            return False

        old_schema = Schema.from_column_dicts(schema_delta.old_value)
        new_schema = Schema.from_column_dicts(schema_delta.new_value)
        dt = old_schema.diff_type_counts(new_schema)
        # We do support name_updates, but we don't support any other type of schema update
        # - except by rewriting the entire table.
        dt.pop("name_updates")
        return sum(dt.values()) == 0

    def _apply_meta_title(self, sess, dataset, src_value, dest_value):
        # TODO - find a better way to roundtrip titles while keeping them unique
        table_name = dataset.table_name
        identifier = f"{table_name}: {dest_value}"
        sess.execute(
            """UPDATE gpkg_contents SET identifier = :identifier WHERE table_name = :table_name""",
            {"identifier": identifier, "table_name": table_name},
        )

    def _apply_meta_description(self, sess, dataset, src_value, dest_value):
        sess.execute(
            """UPDATE gpkg_contents SET description = :description WHERE table_name = :table_name""",
            {"description": dest_value, "table_name": dataset.table_name},
        )

    def _apply_meta_schema_json(self, sess, dataset, src_value, dest_value):
        src_schema = Schema.from_column_dicts(src_value)
        dest_schema = Schema.from_column_dicts(dest_value)

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
            gpkg_metadata = gpkg_adapter.xml_to_gpkg_metadata(dest_value, table)
            gpkg_metadata_reference = gpkg_adapter.xml_to_gpkg_metadata(
                dest_value, table, reference=True
            )
            self._write_meta_metadata(
                sess, table, gpkg_metadata, gpkg_metadata_reference
            )

    def _apply_meta_metadata_dataset_json(self, sess, dataset, src_value, dest_value):
        table = dataset.table_name
        self._delete_meta_metadata(sess, table)
        if dest_value:
            gpkg_metadata = gpkg_adapter.json_to_gpkg_metadata(dest_value, table)
            gpkg_metadata_reference = gpkg_adapter.json_to_gpkg_metadata(
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


class GeometryType(UserDefinedType):
    """UserDefinedType so that GPKG geometry is normalised to V2 format."""

    def result_processor(self, dialect, coltype):
        def process(gpkg_bytes):
            # Its possible in GPKG to put arbitrary values in columns, regardless of type.
            # We don't try to convert them here - we let the commit validation step report this as an error.
            if not isinstance(gpkg_bytes, bytes):
                return gpkg_bytes
            # We normalise geometries to avoid spurious diffs - diffs where nothing
            # of any consequence has changed (eg, only endianness has changed).
            # This includes setting the SRID to zero for each geometry so that we don't store a separate SRID per geometry,
            # but only one per column at most.
            return normalise_gpkg_geom(gpkg_bytes)

        return process
