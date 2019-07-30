import re
import sqlite3

import pytest

import pygit2


H = pytest.helpers.helpers()

GPKG_IMPORTS = (
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


@pytest.mark.slow
@pytest.mark.parametrize(*GPKG_IMPORTS)
def test_import_geopackage(archive, gpkg, table, data_archive, tmp_path, cli_runner):
    """ Import the GeoPackage (eg. `kx-foo-layer.gpkg`) into a Snowdrop repository. """
    with data_archive(archive) as data:
        # list tables
        repo_path = tmp_path / "data.snow"
        r = cli_runner.invoke(["import-gpkg", f"--list-tables", data / gpkg])
        assert r.exit_code == 1, r
        lines = r.stdout.splitlines()
        assert len(lines) >= 2
        assert lines[0] == '"import-gpkg" is deprecated and will be removed in future, use "init" instead'
        assert lines[1] == f"GeoPackage tables in '{gpkg}':"
        assert any(re.match(fr"^{table}\s+- ", l) for l in lines[2:])

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

        # has no working copy
        wc = (repo_path / f"{repo_path.stem}.gpkg")
        assert not wc.exists()

        # existing
        r = cli_runner.invoke(
            [f"--repo={repo_path}", "import-gpkg", data / gpkg, table]
        )
        assert r.exit_code == 2, r
        assert re.search(r"^Error: Invalid value for directory: \".*\" isn't empty", r.stdout, re.MULTILINE)


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
        assert "Feature/Attributes table 'some-layer-that-doesn't-exist' not found in gpkg_contents" in r.stdout

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


@pytest.mark.slow
@pytest.mark.parametrize(*GPKG_IMPORTS)
def test_init_import_list(archive, gpkg, table, data_archive, tmp_path, cli_runner, chdir, geopackage):
    with data_archive(archive) as data:
        # list tables
        r = cli_runner.invoke(["init", "--import", f"gPkG:{data / gpkg}"])
        assert r.exit_code == 1, r
        lines = r.stdout.splitlines()
        assert len(lines) >= 2
        assert lines[0] == f"GeoPackage tables in '{gpkg}':"
        assert any(re.match(fr"^{table}\s+- ", l) for l in lines[1:])


@pytest.mark.slow
@pytest.mark.parametrize(*GPKG_IMPORTS)
def test_init_import(archive, gpkg, table, data_archive, tmp_path, cli_runner, chdir, geopackage):
    """ Import the GeoPackage (eg. `kx-foo-layer.gpkg`) into a Snowdrop repository. """
    with data_archive(archive) as data:
        # list tables
        repo_path = tmp_path / "data.snow"
        repo_path.mkdir()

        with chdir(repo_path):
            r = cli_runner.invoke(
                ["init", "--import", f"gpkg:{data / gpkg}:{table}"]
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

            # working copy exists
            wc = (repo_path / f"{repo_path.stem}.gpkg")
            assert wc.exists() and wc.is_file()
            print("workingcopy at", wc)

            assert repo.config["kx.workingcopy"] == f"GPKG:{wc.name}:{table}"

            db = geopackage(wc)
            nrows = db.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
            assert nrows > 0

            wc_tree_id = db.execute(
                "SELECT value FROM __kxg_meta WHERE table_name=? AND key='tree';", [table]
            ).fetchone()[0]
            assert wc_tree_id == repo.head.peel(pygit2.Tree).hex


@pytest.mark.slow
def test_init_import_errors(data_archive, tmp_path, cli_runner):
    gpkg = "census2016_sdhca_ot_short.gpkg"
    table = "census2016_sdhca_ot_ra_short"

    with data_archive("gpkg-au-census") as data:
        # list tables
        repo_path = tmp_path / "data.snow"
        repo_path.mkdir()

        r = cli_runner.invoke(["init", "--import", f"fred:thingz"])
        assert r.exit_code == 2, r
        assert 'invalid prefix: "FRED" (choose from GPKG)' in r.stdout

        r = cli_runner.invoke(["init", "--import", f"gpkg:thingz.gpkg"])
        assert r.exit_code == 2, r
        assert 'File "thingz.gpkg" does not exist.' in r.stdout

        r = cli_runner.invoke(["init", "--import", f"gpkg:{data/gpkg}:no-existey"])
        assert r.exit_code == 2, r
        assert "Feature/Attributes table 'no-existey' not found in gpkg_contents" in r.stdout

        r = cli_runner.invoke(["init", "--import", f"gpkg:{data/gpkg}:{table}"])
        assert r.exit_code == 2, r
        assert 'name should end in .snow' in r.stdout

        # not empty
        (repo_path / 'a.file').touch()
        r = cli_runner.invoke(["init", "--import", f"gpkg:{data/gpkg}:{table}", repo_path])
        assert r.exit_code == 2, r
        assert "isn't empty" in r.stdout

        # import
        repo_path = tmp_path / "data2.snow"
        repo_path.mkdir()

        r = cli_runner.invoke(
            ["init", "--import", f"gpkg:{data / gpkg}:{table}", repo_path, "--no-checkout"]
        )
        assert r.exit_code == 0, r
        assert (repo_path / "HEAD").exists()
        assert not (repo_path / f"{repo_path.stem}.gpkg").exists()

        # existing repo/dir
        r = cli_runner.invoke(
            ["init", "--import", f"gpkg:{data / gpkg}:{table}", repo_path]
        )
        assert r.exit_code == 2, r
        assert "isn't empty" in r.stdout


@pytest.mark.xfail(reason="not implemented")
def test_init_empty(tmp_path, cli_runner, chdir):
    """ TODO: Create an empty Snowdrop repository. """
    repo_path = tmp_path / "data.snow"
    repo_path.mkdir()

    r = cli_runner.invoke(
        ["init", repo_path]
    )
    assert r.exit_code == 0, r
