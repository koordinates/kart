import pytest

from kart.sqlalchemy.create_engine import gpkg_engine

H = pytest.helpers.helpers()


def test_gpkg_engine(data_working_copy):
    with data_working_copy("points") as (repo_path, wc_path):

        engine = gpkg_engine(wc_path)
        with engine.connect() as db:
            r = db.execute(f"SELECT * FROM {H.POINTS.LAYER} LIMIT 1;")
            assert r.fetchone() is not None
