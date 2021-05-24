from binascii import unhexlify

import sqlalchemy
from sqlalchemy.dialects.postgresql.base import PGIdentifierPreparer
import psycopg2
from psycopg2.extensions import Binary, new_type, register_adapter, register_type

from kart.geometry import Geometry
from .base import BaseDb


# Set up timestamps:
def _adapt_timestamp_from_pg(t, dbcur):
    if t is None:
        return t
    # Output timestamps in the same variant of ISO 8601 required by GPKG.
    return str(t).replace(" ", "T").replace("+00", "Z")


# See https://github.com/psycopg/psycopg2/blob/master/psycopg/typecast_builtins.c
TIMESTAMP_OID = 1114
TIMESTAMP = new_type((TIMESTAMP_OID,), "TIMESTAMP", _adapt_timestamp_from_pg)
psycopg2.extensions.register_type(TIMESTAMP)

TIMESTAMPTZ_OID = 1184
TIMESTAMPTZ = new_type((TIMESTAMPTZ_OID,), "TIMESTAMPTZ", _adapt_timestamp_from_pg)
psycopg2.extensions.register_type(TIMESTAMPTZ)


# PostGIS set-up - blobs:
def _adapt_binary_from_pg(b, dbcur):
    return unhexlify(b[2:]) if b is not None else None


BINARY_OID = 17
BINARY = new_type((BINARY_OID,), "BINARY", _adapt_binary_from_pg)
psycopg2.extensions.register_type(BINARY)


# PostGIS set-up - strings:
# We mostly want data out of the DB as strings, just as happens in GPKG.
# Then we can serialise it using MessagePack.


def _adapt_to_string(v, dbcur):
    return str(v) if v is not None else None


ADAPT_TO_STR_TYPES = {
    1082: "DATE",
    1083: "TIME",
    1266: "TIME",
    704: "INTERVAL",
    1186: "INTERVAL",
    1700: "DECIMAL",
}

for oid in ADAPT_TO_STR_TYPES:
    t = new_type((oid,), ADAPT_TO_STR_TYPES[oid], _adapt_to_string)
    psycopg2.extensions.register_type(t)


# PostGIS set-up - geometry:


def _adapt_geometry_to_pg(g):
    return Binary(g.to_ewkb())


def _adapt_geometry_from_pg(g, dbcur):
    return Geometry.from_hex_ewkb(g)


# We can register_adapter, but we still have to register_type below:
register_adapter(Geometry, _adapt_geometry_to_pg)


class Db_Postgis(BaseDb):
    """Functionality for using sqlalchemy to connect to a PostGIS database."""

    @classmethod
    def create_engine(cls, pgurl):
        def _on_connect(psycopg2_conn, connection_record):
            with psycopg2_conn.cursor() as dbcur:
                dbcur.execute("SELECT oid FROM pg_type WHERE typname='geometry';")
                # Unlike the other set-up, this must be done with an actual connection open -
                # otherwise we don't know the geometry_oid (or if the geometry type exists at all).
                r = dbcur.fetchone()
                if r:
                    geometry_type = new_type(
                        (r[0],), "GEOMETRY", _adapt_geometry_from_pg
                    )
                    register_type(geometry_type, psycopg2_conn)

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
        sqlalchemy.event.listen(engine, "connect", _on_connect)
        sqlalchemy.event.listen(engine, "checkout", _on_checkout)

        return engine

    @classmethod
    def create_preparer(cls, engine):
        return PGIdentifierPreparer(engine.dialect)
