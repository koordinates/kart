import sqlalchemy

from sno import spatialite_path


def gpkg_engine(path):
    def _on_connect(pysqlite_conn, connection_record):
        pysqlite_conn.isolation_level = None
        pysqlite_conn.enable_load_extension(True)
        pysqlite_conn.load_extension(spatialite_path)
        pysqlite_conn.enable_load_extension(False)
        pysqlite_conn.cursor().execute("SELECT EnableGpkgMode();")
        pysqlite_conn.cursor().execute("PRAGMA foreign_keys = ON;")
        pysqlite_conn.cursor().execute("PRAGMA journal_mode = TRUNCATE;")  # faster

    engine = sqlalchemy.create_engine(f"sqlite:///{path}")
    sqlalchemy.event.listen(engine, "connect", _on_connect)
    return engine
