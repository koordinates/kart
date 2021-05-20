from urllib.parse import urlsplit, urlunsplit

import sqlalchemy

from .base import BaseDb


class Db_MySql(BaseDb):
    """Functionality for using sqlalchemy to connect to a MySQL database."""

    CANONICAL_SCHEME = "mysql"
    INTERNAL_SCHEME = "mysql+pymysql"

    @classmethod
    def create_engine(cls, msurl):
        def _on_checkout(mysql_conn, connection_record, connection_proxy):
            dbcur = mysql_conn.cursor()
            dbcur.execute("SET time_zone='UTC';")
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
