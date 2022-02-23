import os

from pysqlite3 import dbapi2 as sqlite

import sqlalchemy
from kart import spatialite_path
from sqlalchemy.dialects.sqlite.base import (SQLiteDialect,
                                             SQLiteIdentifierPreparer)

from .base import BaseDb


class Db_GPKG(BaseDb):
    """Functionality for using sqlalchemy to connect to a GPKG database."""

    GPKG_CACHE_SIZE_MiB = 200

    preparer = SQLiteIdentifierPreparer(SQLiteDialect())

    @classmethod
    def create_engine(cls, path, **kwargs):
        def _on_connect(pysqlite_conn, connection_record):
            pysqlite_conn.isolation_level = None
            pysqlite_conn.enable_load_extension(True)
            pysqlite_conn.load_extension(spatialite_path)
            pysqlite_conn.enable_load_extension(False)
            dbcur = pysqlite_conn.cursor()
            dbcur.execute("SELECT EnableGpkgMode();")
            dbcur.execute("PRAGMA foreign_keys = ON;")
            dbcur.execute(f"PRAGMA cache_size = -{cls.GPKG_CACHE_SIZE_MiB * 1024};")

        path = os.path.expanduser(path)
        engine = sqlalchemy.create_engine(f"sqlite:///{path}", module=sqlite, **kwargs)
        sqlalchemy.event.listen(engine, "connect", _on_connect)
        return engine

    @classmethod
    def list_tables(cls, sess, db_schema=None):
        if db_schema is not None:
            raise RuntimeError("GPKG files don't have a db_schema")

        gpkg_contents_exists = sess.scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='gpkg_contents';",
        )
        if gpkg_contents_exists:
            r = sess.execute(
                """
                SELECT SM.name, GC.identifier FROM sqlite_master SM
                LEFT OUTER JOIN gpkg_contents GC ON GC.table_name = SM.name
                WHERE SM.type='table'
                AND SM.name NOT LIKE 'sqlite%' AND SM.name NOT LIKE 'gpkg%' and SM.name NOT LIKE 'rtree%' and SM.name != 'ogr_empty_table'
                ORDER BY SM.name;
                """
            )
            return {row["name"]: row["identifier"] for row in r}

        r = sess.execute(
            """
            SELECT name FROM sqlite_master SM WHERE type='table'
            AND name NOT LIKE 'sqlite%' AND name NOT LIKE 'gpkg%' and name NOT LIKE 'rtree%' AND name != 'ogr_empty_table'
            ORDER BY name;
            """
        )
        return {row["name"]: None for row in r}

    @classmethod
    def pk_name(cls, sess, db_schema=None, table=None):
        """ Find the primary key for a GeoPackage table """

        # Requirement 150:
        # A feature table or view SHALL have a column that uniquely identifies the
        # row. For a feature table, the column SHOULD be a primary key. If there
        # is no primary key column, the first column SHALL be of type INTEGER and
        # SHALL contain unique values for each row.

        if db_schema is not None:
            raise RuntimeError("GPKG files don't have a db_schema")

        r = sess.execute(f"PRAGMA table_info({cls.quote(table)});")
        fields = []
        for field in r:
            if field["pk"]:
                return field["name"]
            fields.append(field)

        if fields[0]["type"].upper() == "INTEGER":
            return fields[0]["name"]
        else:
            raise RuntimeError("No valid GeoPackage primary key field found")

    @classmethod
    def pk_names(cls, sess, db_schema=None, table=None):
        # GPKG only ever has one primary key.
        return [cls.pk_names(sess, db_schema, table)]
