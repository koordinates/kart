import sqlalchemy
from sqlalchemy.dialects.postgresql.base import PGIdentifierPreparer, PGDialect
import psycopg2

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
            r = sess.execute(
                sqlalchemy.text(
                    """
                    SELECT table_name as name,
                    obj_description(format('%s.%s', table_schema, table_name)::regclass::oid, 'pg_class') as title
                    FROM information_schema.tables WHERE table_schema = :db_schema
                    ORDER BY name;
                    """
                ),
                {"db_schema": db_schema},
            )
        else:
            r = sess.execute(
                sqlalchemy.text(
                    """
                    SELECT format('%s.%s', table_schema, table_name) as name,
                    obj_description(format('%s.%s', table_schema, table_name)::regclass::oid, 'pg_class') as title
                    FROM information_schema.tables
                    WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'tiger', 'topology')
                    ORDER BY name;
                    """
                )
            )

        return {row['name']: row['title'] for row in r}

    @classmethod
    def db_schema_searchpath(cls, sess):
        return sess.scalar("SELECT current_schemas(true);")
