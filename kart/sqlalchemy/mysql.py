from urllib.parse import urlsplit, urlunsplit

import sqlalchemy
from sqlalchemy.dialects.mysql.base import (MySQLDialect,
                                            MySQLIdentifierPreparer)

from .base import BaseDb


class Db_MySql(BaseDb):
    """Functionality for using sqlalchemy to connect to a MySQL database."""

    CANONICAL_SCHEME = "mysql"
    INTERNAL_SCHEME = "mysql+pymysql"

    preparer = MySQLIdentifierPreparer(MySQLDialect())

    @classmethod
    def create_engine(cls, msurl):
        def _on_checkout(mysql_conn, connection_record, connection_proxy):
            dbcur = mysql_conn.cursor()
            # +00:00 is UTC, but unlike UTC, it works even without a timezone DB.
            dbcur.execute("SET time_zone='+00:00';")
            dbcur.execute("SET sql_mode = 'ANSI_QUOTES';")

        url = urlsplit(msurl)
        if url.scheme != cls.CANONICAL_SCHEME:
            raise ValueError("Expecting mysql://")
        url_path = url.path or "/"  # Empty path doesn't work with non-empty query.
        url_query = cls._append_to_query(url.query, {"program_name": "kart"})
        msurl = urlunsplit([cls.INTERNAL_SCHEME, url.netloc, url_path, url_query, ""])

        engine = sqlalchemy.create_engine(msurl, poolclass=cls._pool_class())
        sqlalchemy.event.listen(engine, "checkout", _on_checkout)

        return engine

    @classmethod
    def list_tables(cls, sess, db_schema=None):
        # TODO - include titles.
        if db_schema is not None:
            r = sess.execute(
                sqlalchemy.text(
                    """
                    SELECT TABLE_NAME
                    FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = :db_schema
                    ORDER BY TABLE_NAME;
                    """
                ),
                {"db_schema": db_schema},
            )
            return {row['TABLE_NAME']: None for row in r}
        else:
            r = sess.execute(
                sqlalchemy.text(
                    """
                    SELECT TABLE_SCHEMA, TABLE_NAME
                    FROM INFORMATION_SCHEMA.TABLES
                    ORDER BY TABLE_SCHEMA, TABLE_NAME;
                    """
                )
            )
            return {f"{row['TABLE_SCHEMA']}.{row['TABLE_NAME']}": None for row in r}

    @classmethod
    def drop_all_in_schema(cls, sess, db_schema):
        """Drops all tables and routines in schema db_schema."""
        for thing in ("table", "routine"):
            cls._drop_things_in_schema(cls, sess, db_schema, thing)
