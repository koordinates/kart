import os

from osgeo import gdal

from kart.ogr_import_source import postgres_url_to_ogr_conn_str
from kart.repo import KartRepo


def _dataset_col_types(dataset):
    cols = {}
    for col in dataset.schema.to_column_dicts():
        col = col.copy()
        col.pop("id")
        cols[col.pop("name")] = col
    return cols


def test_import_various_field_types(tmp_path, postgis_db, cli_runner):
    # Using postgres here because it has the best type preservation
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
                    flote FLOAT,
                    dubble DOUBLE PRECISION,
                    tumeric99_0 numeric(99,0),
                    tumeric4_0 numeric(4,0),
                    tumeric numeric,
                    techs varchar,
                    techs10 varchar(100)
                );
                """
        )

    r = cli_runner.invoke(["init", str(tmp_path / "repo1")])
    assert r.exit_code == 0, r.stderr
    r = cli_runner.invoke(
        [
            "-C",
            str(tmp_path / "repo1"),
            "import",
            os.environ["KART_POSTGRES_URL"],
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
        "dubble": {"dataType": "float", "size": 64},
        "smallant": {"dataType": "integer", "size": 16},
        "regularant": {"dataType": "integer", "size": 32},
        "tumeric20_0": {"dataType": "numeric", "precision": 20, "scale": 0},
        "tumeric4_0": {"dataType": "numeric", "precision": 4, "scale": 0},
        "tumeric5_5": {"dataType": "numeric", "precision": 5, "scale": 5},
        "tumeric99_0": {"dataType": "numeric", "precision": 99, "scale": 0},
        "techs": {"dataType": "text"},
        "techs10": {"dataType": "text", "length": 100},
        # these two are regrettable but currently unavoidable;
        # ogr treats both floats and unqualified numerics as Real(0.0),
        # so they're indistinguishable from doubles.
        "tumeric": {"dataType": "float", "size": 64},
        "flote": {"dataType": "float", "size": 64},
    }

    # Now generate a DBF file, and try again from there.
    ogr_conn_str = postgres_url_to_ogr_conn_str(os.environ["KART_POSTGRES_URL"])
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
        "FID": {"dataType": "integer", "primaryKeyIndex": 0, "size": 64},
        "bigant": {"dataType": "integer", "size": 64},
        "regularant": {"dataType": "integer", "size": 32},
        "smallant": {"dataType": "integer", "size": 32},
        "dubble": {"dataType": "float", "size": 64},
        "flote": {"dataType": "float", "size": 64},
        "techs": {"dataType": "text", "length": 80},
        "techs10": {"dataType": "text", "length": 100},
        "tumeric": {"dataType": "float", "size": 64},
        "tumeric20_": {"dataType": "numeric", "precision": 20, "scale": 0},
        "tumeric4_0": {"dataType": "numeric", "precision": 4, "scale": 0},
        "tumeric5_5": {"dataType": "numeric", "precision": 5, "scale": 5},
        "tumeric99_": {"dataType": "numeric", "precision": 99, "scale": 0},
    }
