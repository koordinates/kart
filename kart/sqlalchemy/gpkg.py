import logging


import sqlalchemy
from pysqlite3 import dbapi2 as sqlite

from kart import spatialite_path
from .base import BaseDb


class Db_GPKG(BaseDb):
    """Functionality for using sqlalchemy to connect to a GPKG database."""

    GPKG_CACHE_SIZE_MiB = 200

    L = logging.getLogger("kart.sqlalchemy.Db_GPKG")

    @classmethod
    def create_engine(cls, path):
        def _on_connect(pysqlite_conn, connection_record):
            pysqlite_conn.isolation_level = None
            pysqlite_conn.enable_load_extension(True)
            pysqlite_conn.load_extension(spatialite_path)
            pysqlite_conn.enable_load_extension(False)
            dbcur = pysqlite_conn.cursor()
            dbcur.execute("SELECT EnableGpkgMode();")
            dbcur.execute("PRAGMA foreign_keys = ON;")
            dbcur.execute(f"PRAGMA cache_size = -{cls.GPKG_CACHE_SIZE_MiB * 1024};")

        engine = sqlalchemy.create_engine(f"sqlite:///{path}", module=sqlite)
        sqlalchemy.event.listen(engine, "connect", _on_connect)
        return engine
