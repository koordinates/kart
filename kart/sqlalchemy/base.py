import os
import re
import socket
from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit

import sqlalchemy as sa
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.pool import NullPool
from sqlalchemy.sql.compiler import IdentifierPreparer


class BaseDb:
    """Base functionality common to all types of sqlalchemy databases that we support."""

    # Subclasses should override with a specific IdentifierPreparer, or identifier quoting will not work properly.
    preparer = IdentifierPreparer(DefaultDialect())

    @classmethod
    def create_engine(cls, spec):
        """Create an engine for connecting to the database specified."""
        raise NotImplementedError()

    @classmethod
    def quote(cls, identifier):
        """Conditionally quote the given identifier (ie, if it is a keyword or contains reserved characters."""
        # Subclasses should override cls.preparer with a specific IdentifierPreparer.
        return cls.preparer.quote(identifier)

    @classmethod
    def quote_table(cls, table_name, db_schema=None):
        return cls.preparer.format_table(sa.table(table_name, schema=db_schema))

    @classmethod
    def list_tables(cls, sess, db_schema=None):
        """
        Find all the user tables (not system tables) in the database (or in a specific db_schema).
        Returns a dict of {table_name: table_title}
        """
        raise NotImplementedError()

    @classmethod
    def db_schema_searchpath(cls, sess):
        """Returns a list of the db_schemas that the connection is configured to search in by default."""
        raise NotImplementedError()

    @classmethod
    def _pool_class(cls):
        # Ordinarily, sqlalchemy engine's maintain a pool of connections ready to go.
        # When running tests, we run lots of kart commands, and each command creates an engine, and each engine
        # maintains a pool. This can quickly exhaust the database's allowed connection limit.
        # One fix would be to share engines between kart commands run during tests that are connecting to the same DB.
        # But this fix is simpler for now: disable the pool during testing.
        return NullPool if "PYTEST_CURRENT_TEST" in os.environ else None

    @classmethod
    def _append_query_to_url(cls, uri, new_query_dict):
        url = urlsplit(uri)
        url_query = cls._append_to_query(url.query, new_query_dict)
        return urlunsplit([url.scheme, url.netloc, url.path, url_query, ""])

    @classmethod
    def _append_to_query(cls, existing_query, new_query_dict):
        query_dict = parse_qs(existing_query)
        # ignore new keys if they're already set in the querystring
        return urlencode({**new_query_dict, **query_dict}, doseq=True)

    @classmethod
    def drop_all_in_schema(cls, sess, db_schema):
        """Drops all tables, routines, and sequences in schema db_schema."""
        for thing in ("table", "routine", "sequence"):
            cls._drop_things_in_schema(cls, sess, db_schema, thing)

    def _drop_things_in_schema(cls, sess, db_schema, thing):
        r = sess.execute(
            sa.text(
                f"SELECT {thing}_name FROM information_schema.{thing}s WHERE {thing}_schema=:db_schema;"
            ),
            {"db_schema": db_schema},
        )
        thing_identifiers = ", ".join(
            (cls.quote_table(row[0], db_schema=db_schema) for row in r)
        )
        if thing_identifiers:
            sess.execute(f"DROP {thing} IF EXISTS {thing_identifiers};")
