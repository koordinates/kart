import json

from osgeo import gdal

import pytest

from kart.tabular.ogr_import_source import postgres_url_to_ogr_conn_str
from kart.repo import KartRepo


def _dataset_col_types(dataset):
    cols = {}
    for col in dataset.schema:
        col = dict(col)
        col.pop("id")
        cols[col.pop("name")] = col
    return cols


@pytest.fixture
def postgres_table_with_types(postgis_db):
    with postgis_db.connect() as conn:
        conn.execute(
            """
                DROP TABLE IF EXISTS typoes;
                CREATE TABLE typoes (
                    bigant_pk BIGINT PRIMARY KEY,
                    bigant BIGINT,
                    smallant SMALLINT,
                    regularant INTEGER,
                    tumeric20_0 NUMERIC(20,0),
                    tumeric5_5 NUMERIC(5,5),
                    reel REAL,
                    dubble DOUBLE PRECISION,
                    tumeric99_0 numeric(99,0),
                    tumeric4_0 numeric(4,0),
                    tumeric numeric,
                    techs varchar,
                    techs10 varchar(100)
                );
                """
        )
    yield
    with postgis_db.connect() as conn:
        conn.execute(
            """
            DROP TABLE IF EXISTS typoes;
            """
        )


def test_postgis_import_various_field_types(
    postgis_db, tmp_path, postgres_table_with_types, cli_runner
):
    # Using postgres here because it has the best type preservation

    r = cli_runner.invoke(["init", str(tmp_path / "repo1")])
    assert r.exit_code == 0, r.stderr
    r = cli_runner.invoke(
        [
            "-C",
            str(tmp_path / "repo1"),
            "import",
            postgis_db.original_url,
            "typoes",
        ],
    )

    assert r.exit_code == 0, r.stderr
    repo = KartRepo(tmp_path / "repo1")
    dataset = repo.datasets()["typoes"]

    cols = _dataset_col_types(dataset)

    assert cols == {
        "bigant_pk": {"dataType": "integer", "primaryKeyIndex": 0, "size": 64},
        "bigant": {"dataType": "integer", "size": 64},
        "reel": {"dataType": "float", "size": 32},
        "dubble": {"dataType": "float", "size": 64},
        "smallant": {"dataType": "integer", "size": 16},
        "regularant": {"dataType": "integer", "size": 32},
        "tumeric": {"dataType": "numeric"},
        "tumeric20_0": {"dataType": "numeric", "precision": 20},
        "tumeric4_0": {"dataType": "numeric", "precision": 4},
        "tumeric5_5": {"dataType": "numeric", "precision": 5, "scale": 5},
        "tumeric99_0": {"dataType": "numeric", "precision": 99},
        "techs": {"dataType": "text"},
        "techs10": {"dataType": "text", "length": 100},
    }

    # Now generate a DBF file, and try again from there.
    ogr_conn_str = postgres_url_to_ogr_conn_str(postgis_db.original_url)
    gdal.VectorTranslate(
        str(tmp_path / "typoes.dbf"),
        ogr_conn_str,
        format="ESRI Shapefile",
        layers=["typoes"],
    )

    r = cli_runner.invoke(["init", str(tmp_path / "repo2")])
    assert r.exit_code == 0, r.stderr
    r = cli_runner.invoke(
        [
            "-C",
            str(tmp_path / "repo2"),
            "import",
            str(tmp_path / "typoes.dbf"),
            "typoes",
        ],
    )

    assert r.exit_code == 0, r.stderr
    repo = KartRepo(tmp_path / "repo2")
    dataset = repo.datasets()["typoes"]

    cols = _dataset_col_types(dataset)
    assert cols == {
        "auto_pk": {"dataType": "integer", "primaryKeyIndex": 0, "size": 64},
        "bigant": {"dataType": "integer", "size": 64},
        "regularant": {"dataType": "integer", "size": 32},
        "smallant": {"dataType": "integer", "size": 32},
        "dubble": {"dataType": "float", "size": 64},
        "techs": {"dataType": "text", "length": 80},
        "techs10": {"dataType": "text", "length": 100},
        "tumeric20_": {"dataType": "numeric", "precision": 20, "scale": 0},
        "tumeric4_0": {"dataType": "numeric", "precision": 4, "scale": 0},
        "tumeric5_5": {"dataType": "numeric", "precision": 5, "scale": 5},
        "tumeric99_": {"dataType": "numeric", "precision": 99, "scale": 0},
        # These two types conversions are regrettable, but unavoidable as we are using OGR.
        "reel": {"dataType": "float", "size": 64},
        "tumeric": {"dataType": "float", "size": 64},
    }


def test_list_postgres_tables(postgis_db, postgres_table_with_types, cli_runner):
    r = cli_runner.invoke(["import", "--list", postgis_db.original_url, "-ojson"])
    assert r.exit_code == 0, r.stderr
    tables = json.loads(r.stdout)["kart.tables/v1"].keys()
    assert "public.typoes" in tables
    # These tables are intentionally absent - the user doesn't want to import them:
    assert "public.geography_columns" not in tables
    assert "public.geometry_columns" not in tables
    assert "public.spatial_ref_sys" not in tables

    # Note that there could be other tables already present since this is the public namespace.
    # TODO: maybe we shouldn't be putting test tables in the public namespace, even temporarily.

    r = cli_runner.invoke(
        [
            "import",
            "--list",
            postgis_db.original_url + "/public",
            "-ojson",
        ]
    )
    assert r.exit_code == 0, r.stderr
    tables = json.loads(r.stdout)["kart.tables/v1"].keys()

    assert "typoes" in tables
    assert "geography_columns" not in tables
    assert "geometry_columns" not in tables
    assert "spatial_ref_sys" not in tables
