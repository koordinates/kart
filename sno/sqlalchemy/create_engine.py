import os
from pathlib import Path
import re
import socket
from urllib.parse import urlsplit, urlunsplit, urlencode, parse_qs


import sqlalchemy
from pysqlite3 import dbapi2 as sqlite
import psycopg2
from psycopg2.extensions import Binary, new_type, register_adapter, register_type


from sno import spatialite_path, is_windows
from sno.geometry import Geometry
from sno.exceptions import NotFound, NO_DRIVER


def gpkg_engine(path):
    def _on_connect(pysqlite_conn, connection_record):
        pysqlite_conn.isolation_level = None
        pysqlite_conn.enable_load_extension(True)
        pysqlite_conn.load_extension(spatialite_path)
        pysqlite_conn.enable_load_extension(False)
        dbcur = pysqlite_conn.cursor()
        dbcur.execute("SELECT EnableGpkgMode();")
        dbcur.execute("PRAGMA foreign_keys = ON;")
        dbcur.execute("PRAGMA journal_mode = TRUNCATE;")  # faster

    engine = sqlalchemy.create_engine(f"sqlite:///{path}", module=sqlite)
    sqlalchemy.event.listen(engine, "connect", _on_connect)
    return engine


# PostGIS set-up - timestamps:


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


def postgis_engine(pgurl):
    def _on_connect(psycopg2_conn, connection_record):
        dbcur = psycopg2_conn.cursor()
        dbcur.execute("SET timezone='UTC';")
        dbcur.execute("SET intervalstyle='iso_8601';")
        dbcur.execute("SELECT oid FROM pg_type WHERE typname='geometry';")
        # Unlike the other set-up, this must be done with an actual connection open -
        # otherwise we don't know the geometry_oid (or if the geometry type exists at all).
        r = dbcur.fetchone()
        if r:
            geometry_type = new_type((r[0],), "GEOMETRY", _adapt_geometry_from_pg)
            register_type(geometry_type, psycopg2_conn)

    pgurl = _append_query_to_url(pgurl, {"fallback_application_name": "sno"})

    engine = sqlalchemy.create_engine(pgurl, module=psycopg2)
    sqlalchemy.event.listen(engine, "connect", _on_connect)

    return engine


CANONICAL_SQL_SERVER_SCHEME = "mssql"
INTERNAL_SQL_SERVER_SCHEME = "mssql+pyodbc"


def sqlserver_engine(msurl):
    url = urlsplit(msurl)
    if url.scheme != CANONICAL_SQL_SERVER_SCHEME:
        raise ValueError("Expecting mssql://")

    # SQL server driver is fussy - doesn't like localhost, prefers 127.0.0.1
    url_netloc = re.sub(r"\blocalhost\b", _replace_with_localhost, url.netloc)

    url_query = _append_to_query(
        url.query, {"driver": get_sqlserver_driver(), "Application Name": "sno"}
    )

    msurl = urlunsplit(
        [INTERNAL_SQL_SERVER_SCHEME, url_netloc, url.path, url_query, ""]
    )

    engine = sqlalchemy.create_engine(msurl)
    return engine


def _find_odbc_inst_ini():
    # Locating the odbcinst.ini file (used by SQL Server via pyodbc via unixODBC).
    # At least on Linux, unixODBC seems not to look in /etc/odbcinst.ini by default -
    # which is a pity, since that's most likely where it is.
    # We can make it look in the right place by setting an environment variable.

    # Don't do this on Windows, don't do this is the user already set these variables.
    if is_windows or "ODBCSYSINI" in os.environ or "ODBCINSTINI" in os.environ:
        return

    # Look for odbcinst.ini in the following places:
    search_path = [
        Path.home() / ".odbcinst.ini",
        Path.home() / "Library/ODBC/odbcinst.ini",
        Path("/etc/odbcinst.ini"),
        Path("/Library/ODBC/odbcinst.ini"),
    ]
    for p in search_path:
        if p.exists():
            # Found one - use that one. Set the env variables to point at it.
            os.environ["ODBCSYSINI"] = str(p.parent)
            os.environ["ODBCINSTINI"] = str(p.name)
            return
    # Didn't find anything. Just leave the environment as-is.


def get_odbc_drivers():
    """Returns a list of names of all ODBC drivers."""

    _find_odbc_inst_ini()  # This must be called before loading pyodbc drivers.

    import pyodbc

    return pyodbc.drivers()


def get_sqlserver_driver():
    """Return the name of the SQL Server driver."""
    drivers = get_odbc_drivers()
    mssql_drivers = [
        d for d in drivers if re.search("SQL Server", d, flags=re.IGNORECASE)
    ]
    if not mssql_drivers:
        URL = "https://docs.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server"
        raise NotFound(
            f"ODBC Driver for SQL Server is required but was not found.\nSee {URL}",
            exit_code=NO_DRIVER,
        )
    return sorted(mssql_drivers)[-1]  # Latest driver


def _replace_with_localhost(*args, **kwargs):
    return socket.gethostbyname("localhost")


def _append_query_to_url(uri, new_query_dict):
    url = urlsplit(uri)
    url_query = _append_to_query(url.query, new_query_dict)
    return urlunsplit([url.scheme, url.netloc, url.path, url_query, ""])


def _append_to_query(existing_query, new_query_dict):
    query_dict = parse_qs(existing_query)
    # ignore new keys if they're already set in the querystring
    return urlencode({**new_query_dict, **query_dict}, doseq=True)
