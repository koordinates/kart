import pytest

from kart.sqlalchemy.gpkg import Db_GPKG

H = pytest.helpers.helpers()


def test_gpkg_engine(data_working_copy):
    with data_working_copy("points") as (repo_path, wc_path):
        engine = Db_GPKG.create_engine(wc_path)
        with engine.connect() as db:
            r = db.execute(f"SELECT * FROM {H.POINTS.LAYER} LIMIT 1;")
            assert r.fetchone() is not None
