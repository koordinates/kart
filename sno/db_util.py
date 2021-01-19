from psycopg2.sql import Identifier, SQL

from . import gpkg


# Utilities for dealing with different database drivers.


def execute_insert_dict(dbcur, table, record):
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
            SQL(",").join([SQL("%s")] * len(record)),
        )
    else:
        sql = f"""
        INSERT INTO {table}
            ({','.join([gpkg.ident(k) for k in record])})
        VALUES
            ({','.join(["?"] * len(record))});
        """

    dbcur.execute(sql, tuple(record.values()))
    return changes_rowcount(dbcur)


def changes_rowcount(dbcur):
    """Returns the number of rows that the last .execute*() affected."""
    # PEP 0249 specifies .rowcount, but it not everyone support it:
    if hasattr(dbcur, "rowcount"):
        return dbcur.rowcount
    # Otherwise, this works for GPKG / sqlite:
    return dbcur.getconnection().changes()


def is_postgis(dbcur):
    return type(dbcur).__module__.startswith("psycopg2")
