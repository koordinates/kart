import psycopg2

import sqlalchemy
from sqlalchemy.dialects.postgresql.base import PGDialect, PGIdentifierPreparer

from .base import BaseDb


class Db_Postgis(BaseDb):
    """Functionality for using sqlalchemy to connect to a PostGIS database."""

    preparer = PGIdentifierPreparer(PGDialect())

    @classmethod
    def create_engine(cls, pgurl):
        def _on_checkout(dbapi_connection, connection_record, connection_proxy):
            with dbapi_connection.cursor() as dbcur:
                dbcur.execute("SET timezone='UTC';")
                dbcur.execute("SET intervalstyle='iso_8601';")
                # don't drop precision from floats near the edge of their supported range
                dbcur.execute("SET extra_float_digits = 3;")

        pgurl = cls._append_query_to_url(pgurl, {"fallback_application_name": "kart"})

        engine = sqlalchemy.create_engine(
            pgurl, module=psycopg2, poolclass=cls._pool_class()
        )
        sqlalchemy.event.listen(engine, "checkout", _on_checkout)

        return engine

    @classmethod
    def list_tables(cls, sess, db_schema=None):
        if db_schema is not None:
            name_clause = "c.relname"
            schema_clause = "n.nspname = :db_schema"
            params = {"db_schema": db_schema}
        else:
            name_clause = "format('%s.%s', n.nspname, c.relname)"
            schema_clause = "n.nspname NOT IN ('information_schema', 'pg_catalog', 'tiger', 'topology')"
            params = {}

        r = sess.execute(
            sqlalchemy.text(
                f"""
                SELECT {name_clause} as name, obj_description(c.oid, 'pg_class') as title
                FROM pg_catalog.pg_class c
                    INNER JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relkind IN ('r', 'v') AND {schema_clause}
                AND c.relname NOT LIKE '_kart_%'
                AND c.oid NOT IN (
                    SELECT d.objid
                    FROM pg_catalog.pg_extension AS e
                        INNER JOIN pg_catalog.pg_depend AS d ON (d.refobjid = e.oid)
                    WHERE d.deptype = 'e'
                    AND e.extname = 'postgis'
                )
                ORDER BY {name_clause};
                """
            ),
            params,
        )
        return {row["name"]: row["title"] for row in r}

    @classmethod
    def db_schema_searchpath(cls, sess):
        return sess.scalar("SELECT current_schemas(true);")
