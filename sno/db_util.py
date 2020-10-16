from psycopg2.sql import Identifier, SQL

from . import gpkg


# Utilities for dealing with different database drivers.


def execute_insert_dict(dbcur, table, record, gpkg_funcs=None, pg_funcs=None):
    """
    Insert the given record into the given table.
    dbcur - Database cursor.
    table - the name of a table, or fully qualified name in the form f"{schema}.{name}"
    record - the record to insert, as a dict keyed by column name.
    gpkg_funcs, pg_funcs - extra functions needed to adapt the types to fit the columns.
    """
    if is_postgis(dbcur):
        # Table can be a fully-qualified name, including the schema.
        sql = SQL("INSERT INTO {} ({}) VALUES ({});").format(
            Identifier(*table.split(".")),
            SQL(",").join([Identifier(k) for k in record]),
            SQL(",").join(_postgis_placeholders(record, pg_funcs)),
        )
    else:
        sql = f"""
        INSERT INTO {table}
            ({','.join([gpkg.ident(k) for k in record])})
        VALUES
            ({','.join(_gpkg_placeholders(record, gpkg_funcs))});
        """

    dbcur.execute(sql, tuple(record.values()))
    return changes_rowcount(dbcur)


def _gpkg_placeholders(record, gpkg_funcs=None):
    """
    Returns ['?', '?', '?', ...] - where the nunber of '?' returned is len(record).
    gpkg_funcs - a dict keyed by index to override some of the placeholders, eg:
        {1: "GeomFromEWKT(?)"}
    """
    result = ["?"] * len(record)
    if gpkg_funcs:
        for index, placeholder in gpkg_funcs.items():
            result[index] = placeholder
    return result


def _postgis_placeholders(record, pg_funcs=None):
    """
    Returns ['%s', '%s', '%s', ...] where the number of '%s' returned is len(record).
    pg_funcs - a dict keyed by index to override some of the placeholders, eg:
        {1: "SetSRID(%s, 4326)"}
    """

    result = [SQL("%s")] * len(record)
    if pg_funcs:
        for index, placeholder in pg_funcs.items():
            result[index] = SQL(placeholder)
    return result


def changes_rowcount(dbcur):
    """Returns the number of rows that the last .execute*() affected."""
    # PEP 0249 specifies .rowcount, but it not everyone support it:
    if hasattr(dbcur, "rowcount"):
        return dbcur.rowcount
    # Otherwise, this works for GPKG / sqlite:
    return dbcur.getconnection().changes()


def is_postgis(dbcur):
    return type(dbcur).__module__.startswith("psycopg2")
