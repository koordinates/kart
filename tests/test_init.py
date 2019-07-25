import re
import sqlite3

import pytest

import pygit2


H = pytest.helpers.helpers()


@pytest.mark.slow
@pytest.mark.parametrize(
    "archive,gpkg,table",
    [
        pytest.param(
            "gpkg-points", "nz-pa-points-topo-150k.gpkg", H.POINTS_LAYER, id="points"
        ),
        pytest.param(
            "gpkg-polygons",
            "nz-waca-adjustments.gpkg",
            H.POLYGONS_LAYER,
            id="polygons-pk",
        ),
        pytest.param(
            "gpkg-au-census",
            "census2016_sdhca_ot_short.gpkg",
            "census2016_sdhca_ot_ra_short",
            id="au-ra-short",
        ),
        pytest.param("gpkg-spec", "sample1_2.gpkg", "counties", id="spec-counties"),
        pytest.param(
            "gpkg-spec", "sample1_2.gpkg", "countiestbl", id="spec-counties-table"
        ),
    ],
)
def test_import_geopackage(archive, gpkg, table, data_archive, tmp_path, cli_runner):
    """ Import the GeoPackage (eg. `kx-foo-layer.gpkg`) into a Snowdrop repository. """
    with data_archive(archive) as data:
        # list tables
        repo_path = tmp_path / "data.snow"
        r = cli_runner.invoke(["import-gpkg", f"--list-tables", data / gpkg])
        assert r.exit_code == 0, r
        lines = r.stdout.splitlines()
        assert len(lines) >= 2
        assert lines[0] == f"GeoPackage tables in '{data / gpkg}':"
        assert any(re.match(fr"^{table}\s+- ", l) for l in lines[1:])

        # successful import
        r = cli_runner.invoke(
            [f"--repo={repo_path}", "import-gpkg", data / gpkg, table]
        )
        assert r.exit_code == 0, r
        assert (repo_path / "HEAD").exists()

        repo = pygit2.Repository(str(repo_path))
        assert repo.is_bare
        assert not repo.is_empty

        assert repo.head.name == "refs/heads/master"
        assert repo.head.shorthand == "master"

        # has a single commit
        assert len([c for c in repo.walk(repo.head.target)]) == 1

        # existing
        r = cli_runner.invoke(
            [f"--repo={repo_path}", "import-gpkg", data / gpkg, table]
        )
        assert r.exit_code == 1, r
        assert "Looks like you already have commits in this repository" in r.stdout


def test_import_geopackage_errors(data_archive, tmp_path, cli_runner):
    with data_archive("gpkg-points") as data:
        # missing/bad table name
        repo_path = tmp_path / "data2.snow"
        r = cli_runner.invoke(
            [
                f"--repo={repo_path}",
                "import-gpkg",
                data / "nz-pa-points-topo-150k.gpkg",
                "some-layer-that-doesn't-exist",
            ]
        )
        assert r.exit_code == 2, r
        assert "Table 'some-layer-that-doesn't-exist' not found in gpkg_contents"

        # Not a GeoPackage
        db = sqlite3.connect(str(tmp_path / "a.gpkg"))
        with db:
            db.execute("CREATE TABLE mytable (pk INT NOT NULL PRIMARY KEY, val TEXT);")

        # not a GeoPackage
        repo_path = tmp_path / "data3.snow"
        r = cli_runner.invoke(
            [f"--repo={repo_path}", "import-gpkg", tmp_path / "a.gpkg", "mytable"]
        )
        assert r.exit_code == 2, r
        assert "a.gpkg' doesn't appear to be a valid GeoPackage" in r.stdout
