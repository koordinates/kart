import os

from pysqlite3 import dbapi2 as sqlite

import sqlalchemy


def sqlite_engine(path):
    """
    An engine for non-spatial, non-GPKG sqlite databases.
    """

    def _on_connect(pysqlite_conn, connection_record):
        pysqlite_conn.isolation_level = None
        dbcur = pysqlite_conn.cursor()
        dbcur.execute("PRAGMA foreign_keys = ON;")

    path = os.path.expanduser(path)
    engine = sqlalchemy.create_engine(f"sqlite:///{path}", module=sqlite)
    sqlalchemy.event.listen(engine, "connect", _on_connect)
    return engine
