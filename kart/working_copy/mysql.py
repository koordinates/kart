import contextlib
import logging
import time

import sqlalchemy as sa
from sqlalchemy.dialects.mysql.base import MySQLIdentifierPreparer
from sqlalchemy.sql.functions import Function
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import UserDefinedType
from sqlalchemy.dialects.mysql.types import DOUBLE

from .db_server import DatabaseServer_WorkingCopy
from .table_defs import MySqlKartTables
from kart import crs_util
from kart.geometry import Geometry
from kart.schema import Schema
from kart.sqlalchemy import separate_last_path_part, text_with_inlined_params
from kart.sqlalchemy.adapter.mysql import KartAdapter_MySql


class WorkingCopy_MySql(DatabaseServer_WorkingCopy):
    """
    MySQL working copy implementation.
    Unlike other database-servers (eg Postgresql, Microsoft SQL Server) - MySQL has no concept of a schema (where
    "schema" means a type of namespace, that exists within a database, that exists within a database server / cluster).
    So typically, a Kart manages a working copy by managing every table inside an entire db schema, ie:
    >>> postgresql://HOST[:PORT]/DBNAME/DBSCHEMA
    But in the case of a MySQL working copy, where schemas don't exist, Kart manages a working copy by managing
    every table inside entire database:
    >>> mysql://HOST[:PORT]/DBNAME

    Note that, for compatibility with other working copy implementations, self.db_schema (and escaped variant
    self.DB_SCHEMA) actually contain the database name in this implementation.

    Requirements:
    1. The MySQL server needs to exist
    2. The database user needs to be able to:
        - Create the specified database (unless it already exists).
        - Create, delete and alter tables and triggers in the specified database.
    """

    WORKING_COPY_TYPE_NAME = "MySQL"
    URI_SCHEME = "mysql"

    URI_FORMAT = "//HOST[:PORT]/DBNAME"
    INVALID_PATH_MESSAGE = "URI path must have one part - the database name"

    def __init__(self, repo, location):
        """
        uri: connection string of the form mysql://[user[:password]@][netloc][:port][/dbname][?param1=value1&...]
        """
        self.L = logging.getLogger(self.__class__.__qualname__)

        self.repo = repo
        self.uri = self.location = location

        self.check_valid_location(self.uri, repo)
        self.connect_uri, self.db_schema = separate_last_path_part(self.uri)

        self.adapter = KartAdapter_MySql
        self.engine = self.adapter.create_engine(self.connect_uri)
        self.sessionmaker = sessionmaker(bind=self.engine)
        self.preparer = MySQLIdentifierPreparer(self.engine.dialect)

        self.kart_tables = MySqlKartTables(self.db_schema, repo.is_kart_branded)

    def _create_table_for_dataset(self, sess, dataset):
        table_spec = self.adapter.v2_schema_to_sql_spec(dataset.schema, dataset)
        sess.execute(
            f"""CREATE TABLE IF NOT EXISTS {self.table_identifier(dataset)} ({table_spec});"""
        )
        sess.execute(
            f"ALTER TABLE {self.table_identifier(dataset)} COMMENT = :comment",
            {"comment": dataset.get_meta_item("title")},
        )

    def _type_def_for_column_schema(self, col, dataset=None):
        if col.data_type == "geometry":
            crs_name = col.extra_type_info.get("geometryCRS")
            crs_id = None
            if dataset is not None:
                crs_id = (
                    crs_util.get_identifier_int_from_dataset(dataset, crs_name) or 0
                )
            # This user-defined GeometryType adapts Kart's GPKG geometry to SQL Server's native geometry type.
            return GeometryType(crs_id)
        elif col.data_type == "boolean":
            return BooleanType
        elif col.data_type == "float" and col.extra_type_info.get("size") != 64:
            return FloatType
        elif col.data_type == "date":
            return DateType
        elif col.data_type == "time":
            return TimeType
        elif col.data_type == "timestamp":
            return TimestampType
        else:
            # Don't need to specify type information for other columns at present, since we just pass through the values.
            return None

    def _is_dataset_supported(self, dataset):
        return not any(
            self._is_unsupported_geometry_column(col)
            for col in dataset.schema.geometry_columns
        )

    def _is_unsupported_geometry_column(self, col):
        geometry_type = col.extra_type_info.get("geometryType", "geometry")
        return len(geometry_type.strip().split(" ")) > 1

    def _write_meta(self, sess, dataset):
        # The only metadata to write that is stored outside the table is custom CRS.
        for crs in KartAdapter_MySql.generate_mysql_spatial_ref_sys(dataset):
            existing_crs = sess.execute(
                """
                SELECT organization, definition FROM information_schema.st_spatial_reference_systems
                WHERE srs_id = :srs_id;
                """,
                crs,
            ).fetchone()

            if existing_crs:
                # Don't overwrite existing CRS definitions if they are built-ins. Doing so is an error in MYSQL:
                if existing_crs['ORGANIZATION'] == "EPSG":
                    continue
                # Don't try to replace a CRS if a matching one already exists - overwriting a CRS with an identical
                # CRS is a no-op, but one which requires certain permissions, so we avoid it if we can.
                if existing_crs['DEFINITION'] == crs['definition']:
                    continue
                # Don't replace a CRS definition if it is currently being referenced. Doing so is an error in MySQL.
                if sess.scalar(
                    "SELECT COUNT(*) FROM information_schema.st_geometry_columns WHERE srs_id = :srs_id;",
                    crs,
                ):
                    continue

            sess.execute(
                """
                CREATE OR REPLACE SPATIAL REFERENCE SYSTEM :srs_id
                ORGANIZATION :organization IDENTIFIED BY :org_id
                NAME :name DEFINITION :definition;
                """,
                crs,
            )

    def _delete_meta(self, sess, dataset):
        # The only metadata outside the table itself is CRS definitions. We don't delete them however, for 2 reasons:
        # 1. CRS definitions have global scope and we can't tell if we created them. Even if they're not being used
        # right now, somebody else might have created them and expect them to stay where they are until they are needed.
        # 2. We might need that CRS definition again in a minute (eg next time we switch branch) and we might lack
        # permissions to create or delete CRS definitions. Better to just leave things as-is.
        pass

    def _create_spatial_index_post(self, sess, dataset):
        # Only implemented as _create_spatial_index_post:
        # It is more efficient to write the features first, then index them all in bulk.

        # TODO - MYSQL-PART-2 - We can only create a spatial index if the geometry column is declared
        # not-null, but a datasets V2 schema doesn't distinguish between NULL and NOT NULL columns.
        # So we don't know if the user would rather have an index, or be able to store NULL values.
        return  # Find a fix.

        L = logging.getLogger(f"{self.__class__.__qualname__}._create_spatial_index")

        geom_col = dataset.geom_column_name

        L.debug("Creating spatial index for %s.%s", dataset.table_name, geom_col)
        t0 = time.monotonic()

        sess.execute(
            f"ALTER TABLE {self.table_identifier(dataset)} ADD SPATIAL INDEX({self.quote(geom_col)})"
        )

        L.info("Created spatial index in %ss", time.monotonic() - t0)

    def _drop_spatial_index(self, sess, dataset):
        # MySQL deletes the spatial index automatically when the table is deleted.
        pass

    def _sno_tracking_name(self, trigger_type, dataset=None):
        """Returns the sno-branded name of the trigger reponsible for populating the sno_track table."""
        assert dataset is None
        return f"_sno_track_{trigger_type}"

    def _create_triggers(self, sess, dataset):
        table_identifier = self.table_identifier(dataset)
        pk_column = self.quote(dataset.primary_key)

        sess.execute(
            text_with_inlined_params(
                f"""
                CREATE TRIGGER {self._quoted_tracking_name('ins', dataset)}
                    AFTER INSERT ON {table_identifier}
                FOR EACH ROW
                    REPLACE INTO {self.KART_TRACK} (table_name, pk)
                    VALUES (:table_name, NEW.{pk_column})
                """,
                {"table_name": dataset.table_name},
            )
        )
        sess.execute(
            text_with_inlined_params(
                f"""
                CREATE TRIGGER {self._quoted_tracking_name('upd', dataset)}
                    AFTER UPDATE ON {table_identifier}
                FOR EACH ROW
                    REPLACE INTO {self.KART_TRACK} (table_name, pk)
                    VALUES (:table_name1, OLD.{pk_column}), (:table_name2, NEW.{pk_column})
                """,
                {"table_name1": dataset.table_name, "table_name2": dataset.table_name},
            )
        )
        sess.execute(
            text_with_inlined_params(
                f"""
                CREATE TRIGGER {self._quoted_tracking_name('del', dataset)}
                    AFTER DELETE ON {table_identifier}
                FOR EACH ROW
                    REPLACE INTO {self.KART_TRACK} (table_name, pk)
                    VALUES (:table_name, OLD.{pk_column})
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

    @classmethod
    def try_align_schema_col(cls, old_col_dict, new_col_dict):
        old_type = old_col_dict["dataType"]
        new_type = new_col_dict["dataType"]

        # Some types have to be approximated as other types in MySQL
        if KartAdapter_MySql.APPROXIMATED_TYPES.get(old_type) == new_type:
            new_col_dict["dataType"] = new_type = old_type
            for key in KartAdapter_MySql.APPROXIMATED_TYPES_EXTRA_TYPE_INFO:
                new_col_dict[key] = old_col_dict.get(key)

        return new_type == old_type

    _UNSUPPORTED_META_ITEMS = (
        "description",
        "metadata/dataset.json",
        "metadata.xml",
    )

    def _remove_hidden_meta_diffs(self, dataset, ds_meta_items, wc_meta_items):
        super()._remove_hidden_meta_diffs(dataset, ds_meta_items, wc_meta_items)

        # Nowhere to put these in SQL Server WC
        for key in self._UNSUPPORTED_META_ITEMS:
            if key in ds_meta_items:
                del ds_meta_items[key]

        for key in ds_meta_items.keys() & wc_meta_items.keys():
            if not key.startswith("crs/"):
                continue
            old_crs = crs_util.mysql_compliant_wkt(ds_meta_items[key])
            new_crs = crs_util.mysql_compliant_wkt(wc_meta_items[key])
            if old_crs == new_crs:
                # Hide any diff caused by making the CRS MySQL compliant.
                del ds_meta_items[key]
                del wc_meta_items[key]

    def _is_builtin_crs(self, crs):
        auth_name, auth_code = crs_util.parse_authority(crs)
        return auth_name == "EPSG"

    def _is_schema_update_supported(self, schema_delta):
        if not schema_delta.old_value or not schema_delta.new_value:
            return False

        old_schema = Schema.from_column_dicts(schema_delta.old_value)
        new_schema = Schema.from_column_dicts(schema_delta.new_value)
        dt = old_schema.diff_type_counts(new_schema)

        # We support deletes, name_updates, and type_updates -
        # but we don't support any other type of schema update except by rewriting the entire table.
        dt.pop("deletes")
        dt.pop("name_updates")
        dt.pop("type_updates")
        return sum(dt.values()) == 0

    def _apply_meta_title(self, sess, dataset, src_value, dest_value):
        sess.execute(
            f"ALTER TABLE {self.table_identifier(dataset)} COMMENT = :comment",
            {"comment": dest_value},
        )

    def _apply_meta_schema_json(self, sess, dataset, src_value, dest_value):
        src_schema = Schema.from_column_dicts(src_value)
        dest_schema = Schema.from_column_dicts(dest_value)

        diff_types = src_schema.diff_types(dest_schema)

        deletes = diff_types.pop("deletes")
        name_updates = diff_types.pop("name_updates")
        type_updates = diff_types.pop("type_updates")

        if any(dt for dt in diff_types.values()):
            raise RuntimeError(
                f"This schema change not supported by update - should be drop + re-write_full: {diff_types}"
            )

        table = dataset.table_name
        for col_id in deletes:
            src_name = src_schema[col_id].name
            sess.execute(
                f"""
                ALTER TABLE {self.table_identifier(table)}
                DROP COLUMN {self.quote(src_name)};"""
            )

        for col_id in name_updates:
            src_name = src_schema[col_id].name
            dest_name = dest_schema[col_id].name
            sess.execute(
                f"""
                ALTER TABLE {self.table_identifier(table)}
                RENAME COLUMN {self.quote(src_name)} TO {self.quote(dest_name)};
                """
            )

        for col_id in type_updates:
            col = dest_schema[col_id]
            dest_spec = KartAdapter_MySql.v2_column_schema_to_mysql_spec(col, dataset)
            sess.execute(
                f"""ALTER TABLE {self.table_identifier(table)} MODIFY {dest_spec};"""
            )


class GeometryType(UserDefinedType):
    """UserDefinedType so that V2 geometry is adapted to MySQL binary format."""

    # In Kart, all geometries are stored as WKB with axis-order=long-lat - since this is the GPKG
    # standard, and a Kart geometry is a normalised GPKG geometry. MySQL has to be explicitly told
    # that this is the ordering we use in WKB, since MySQL would otherwise expect lat-long ordering
    # as specified by ISO 19128:2005.
    AXIS_ORDER = "axis-order=long-lat"

    def __init__(self, crs_id):
        self.crs_id = crs_id

    def bind_processor(self, dialect):
        # 1. Writing - Python layer - convert Kart geometry to WKB
        return lambda geom: geom.to_wkb()

    def bind_expression(self, bindvalue):
        # 2. Writing - SQL layer - wrap in call to ST_GeomFromWKB to convert WKB to MySQL binary.
        return Function(
            "ST_GeomFromWKB", bindvalue, self.crs_id, self.AXIS_ORDER, type_=self
        )

    def column_expression(self, col):
        # 3. Reading - SQL layer - wrap in call to ST_AsBinary() to convert MySQL binary to WKB.
        return Function("ST_AsBinary", col, self.AXIS_ORDER, type_=self)

    def result_processor(self, dialect, coltype):
        # 4. Reading - Python layer - convert WKB to Kart geometry.
        return lambda wkb: Geometry.from_wkb(wkb)


class BooleanType(UserDefinedType):
    # UserDefinedType to read booleans. They are stored in MySQL as Bits but we read them back as bools.
    def result_processor(self, dialect, coltype):
        # Reading - Python layer - convert bytes to boolean.
        def process(b):
            i = int.from_bytes(b, "big") if b is not None else None
            return bool(i) if i in (0, 1) else i

        return process


class DateType(UserDefinedType):
    # UserDefinedType to read Dates as text. They are stored in MySQL as Dates but we read them back as text.
    def column_expression(self, col):
        # Reading - SQL layer - convert date to string in ISO8601.
        # https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html
        return Function("DATE_FORMAT", col, "%Y-%m-%d", type_=self)


class FloatType(UserDefinedType):
    # UserDefinedType to read floats as doubles. For some reason, floats they are rounded so they keep
    # even less than single-float precision if we read them as floats.
    def column_expression(self, col):
        # Reading - SQL layer - convert date to string in ISO8601.
        # https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html
        return sa.cast(col, DOUBLE)


class TimeType(UserDefinedType):
    # UserDefinedType to read Times as text. They are stored in MySQL as Times but we read them back as text.
    def column_expression(self, col):
        # Reading - SQL layer - convert timestamp to string in ISO8601.
        # https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html
        return Function("DATE_FORMAT", col, "%H:%i:%S", type_=self)


class TimestampType(UserDefinedType):
    """
    UserDefinedType to read Timestamps as text. They are stored in MySQL as Timestamps but we read them back as text.
    """

    def bind_processor(self, dialect):
        # 1. Writing - Python layer - remove timezone specifier - MySQL can't read timezone specifiers.
        # MySQL requires instead that the timezone is set in the database session (see create_engine.py)
        return lambda timestamp: timestamp.rstrip("Z")

    def column_expression(self, col):
        # 2. Reading - SQL layer - convert timestamp to string in ISO8601 with Z as the timezone specifier.
        # https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html
        return Function("DATE_FORMAT", col, "%Y-%m-%dT%H:%i:%SZ", type_=self)
