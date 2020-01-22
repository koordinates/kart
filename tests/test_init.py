import re
import sqlite3

import pytest

import pygit2

from sno.working_copy import WorkingCopy

H = pytest.helpers.helpers()

# also in test_structure.py
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
    """ Import the GeoPackage (eg. `kx-foo-layer.gpkg`) into a Sno repository. """
    with data_archive(archive) as data:
        # list tables
        repo_path = tmp_path / "data.sno"
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
        repo_path = tmp_path / "data2.sno"
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
        repo_path = tmp_path / "data3.sno"
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
    """ Import the GeoPackage (eg. `kx-foo-layer.gpkg`) into a Sno repository. """
    with data_archive(archive) as data:
        # list tables
        repo_path = tmp_path / "data.sno"
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

            assert repo.config["sno.workingcopy.version"] == "1"
            assert repo.config["sno.workingcopy.path"] == f"{wc.name}"

            db = geopackage(wc)
            assert H.row_count(db, table) > 0

            wc_tree_id = db.execute(
                """SELECT value FROM ".sno-meta" WHERE table_name='*' AND key='tree';"""
            ).fetchone()[0]
            assert wc_tree_id == repo.head.peel(pygit2.Tree).hex

            H.verify_gpkg_extent(db, table)


def test_init_import_name_clash(data_archive, cli_runner, geopackage):
    """ Import the GeoPackage into a Sno repository of the same name, and checkout a working copy of the same name. """
    with data_archive("gpkg-editing") as data:
        r = cli_runner.invoke(
            ["init", "--import", f"GPKG:editing.gpkg:editing", "editing"]
        )
        repo_path = data / "editing"

        assert r.exit_code == 0, r
        assert (repo_path / "HEAD").exists()

        repo = pygit2.Repository(str(repo_path))
        assert repo.is_bare
        assert not repo.is_empty

        # working copy exists
        wc = (repo_path / f"editing.gpkg")
        assert wc.exists() and wc.is_file()
        print("workingcopy at", wc)

        assert repo.config["sno.workingcopy.version"] == "1"
        assert repo.config["sno.workingcopy.path"] == "editing.gpkg"

        db = geopackage(wc)
        wc_rowcount = H.row_count(db, "editing")
        assert wc_rowcount > 0

        wc_tree_id = db.execute(
            """SELECT value FROM ".sno-meta" WHERE table_name='*' AND key='tree';"""
        ).fetchone()[0]
        assert wc_tree_id == repo.head.peel(pygit2.Tree).hex

        # make sure we haven't stuffed up the original file
        db = geopackage("editing.gpkg")
        assert db.execute("SELECT 1 FROM sqlite_master WHERE name='.sno-meta';").fetchall() == []
        source_rowcount = db.execute("SELECT COUNT(*) FROM editing;").fetchone()[0]
        assert source_rowcount == wc_rowcount


@pytest.mark.slow
def test_init_import_errors(data_archive, tmp_path, cli_runner):
    gpkg = "census2016_sdhca_ot_short.gpkg"
    table = "census2016_sdhca_ot_ra_short"

    with data_archive("gpkg-au-census") as data:
        # list tables
        repo_path = tmp_path / "data.sno"
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

        # not empty
        (repo_path / 'a.file').touch()
        r = cli_runner.invoke(["init", "--import", f"gpkg:{data/gpkg}:{table}", repo_path])
        assert r.exit_code == 2, r
        assert "isn't empty" in r.stdout

        # import
        repo_path = tmp_path / "data2.sno"
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


def test_init_empty(tmp_path, cli_runner, chdir):
    """ Create an empty Sno repository. """
    repo_path = tmp_path / "data.sno"
    repo_path.mkdir()

    # empty dir
    r = cli_runner.invoke(
        ["init", repo_path]
    )
    assert r.exit_code == 0, r
    assert (repo_path / 'HEAD').exists()

    # makes dir tree
    repo_path = tmp_path / 'foo' / 'bar' / 'wiz.sno'
    r = cli_runner.invoke(
        ["init", repo_path]
    )
    assert r.exit_code == 0, r
    assert (repo_path / 'HEAD').exists()

    # current dir
    repo_path = tmp_path / "planet.sno"
    repo_path.mkdir()
    with chdir(repo_path):
        r = cli_runner.invoke(
            ["init"]
        )
        assert r.exit_code == 0, r
        assert (repo_path / 'HEAD').exists()

    # dir isn't empty
    repo_path = tmp_path / 'tree'
    repo_path.mkdir()
    (repo_path / 'a.file').touch()
    r = cli_runner.invoke(
        ["init", repo_path]
    )
    assert r.exit_code == 2, r
    assert not (repo_path / 'HEAD').exists()

    # current dir isn't empty
    with chdir(repo_path):
        r = cli_runner.invoke(
            ["init"]
        )
        assert r.exit_code == 2, r
        assert not (repo_path / 'HEAD').exists()


@pytest.mark.slow
def test_init_import_alt_names(data_archive, tmp_path, cli_runner, chdir, geopackage):
    """ Import the GeoPackage (eg. `kx-foo-layer.gpkg`) into a Sno repository. """
    repo_path = tmp_path / "data.sno"
    repo_path.mkdir()

    r = cli_runner.invoke(
        ["init", repo_path]
    )
    assert r.exit_code == 0, r

    ARCHIVE_PATHS = (
        ("gpkg-points", "nz-pa-points-topo-150k.gpkg", "nz_pa_points_topo_150k", "pa_sites"),
        ("gpkg-polygons", "nz-waca-adjustments.gpkg", "nz_waca_adjustments", "misc/waca"),
        ("gpkg-polygons", "nz-waca-adjustments.gpkg", "nz_waca_adjustments", "other/waca2"),
    )

    for archive, source_gpkg, source_table, import_path in ARCHIVE_PATHS:
        with data_archive(archive) as source_path:
            with chdir(repo_path):
                r = cli_runner.invoke(
                    ["import", f"GPKG:{source_path / source_gpkg}:{source_table}", import_path]
                )
                assert r.exit_code == 0, r

    with chdir(repo_path):
        r = cli_runner.invoke(
            ["checkout", "--path=wc.gpkg", "HEAD"]
        )
        assert r.exit_code == 0, r

        # working copy exists
        db = geopackage("wc.gpkg")

        expected_tables = set(a[3].replace("/", "__") for a in ARCHIVE_PATHS)
        db_tables = set(r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table';"))
        assert expected_tables <= db_tables

        for gpkg_t in ('gpkg_contents', 'gpkg_geometry_columns', 'gpkg_metadata_reference'):
            table_list = set(r[0] for r in db.execute(f"SELECT DISTINCT table_name FROM {gpkg_t};"))
            assert expected_tables >= table_list, gpkg_t

        r = cli_runner.invoke(
            ["diff"]
        )
        assert r.exit_code == 0, r
        assert r.stdout.splitlines() == []


@pytest.mark.slow
def test_init_import_home_resolve(data_archive, tmp_path, cli_runner, chdir, monkeypatch):
    """ Import from a ~-specified gpkg path """
    repo_path = tmp_path / "data.sno"
    repo_path.mkdir()

    r = cli_runner.invoke(
        ["init", repo_path]
    )
    assert r.exit_code == 0, r

    with data_archive("gpkg-points") as source_path:
        with chdir(repo_path):
            monkeypatch.setenv("HOME", str(source_path))

            r = cli_runner.invoke(
                ["import", f"GPKG:~/nz-pa-points-topo-150k.gpkg:nz_pa_points_topo_150k"]
            )
            assert r.exit_code == 0, r


@pytest.mark.slow
def test_import_existing_wc(data_archive, data_working_copy, geopackage, cli_runner, insert, tmp_path, request, chdir):
    """ Import a new dataset into a repo with an existing working copy. Dataset should get checked out """
    with data_working_copy("points") as (repo_path, wcdb):
        repo = pygit2.Repository(str(repo_path))
        db = geopackage(wcdb)
        wc = WorkingCopy.open(repo)

        with data_archive("gpkg-polygons") as source_path, chdir(repo_path):
            r = cli_runner.invoke(
                ["import", f"GPKG:{source_path / 'nz-waca-adjustments.gpkg'}:{H.POLYGONS_LAYER}"]
            )
            assert r.exit_code == 0, r

        assert H.row_count(db, "nz_waca_adjustments") > 0

        head_tree = repo.head.peel(pygit2.Tree)
        wc_tree_id = db.execute(
            """SELECT value FROM ".sno-meta" WHERE table_name='*' AND key='tree';"""
        ).fetchone()[0]
        assert wc_tree_id == head_tree.hex
        assert wc.assert_db_tree_match(head_tree)

        r = cli_runner.invoke(
            ["status"]
        )
        assert r.exit_code == 0, r
        assert r.stdout.splitlines()[-1] == "Nothing to commit, working copy clean"

        with db:
            dbcur = db.cursor()
            dbcur.execute("DELETE FROM nz_waca_adjustments WHERE rowid IN (SELECT rowid FROM nz_waca_adjustments ORDER BY id LIMIT 10);")
            assert dbcur.rowcount == 10

        with data_archive("gpkg-polygons") as source_path, chdir(repo_path):
            r = cli_runner.invoke(
                ["import", f"GPKG:{source_path / 'nz-waca-adjustments.gpkg'}:{H.POLYGONS_LAYER}", "waca2"]
            )
            assert r.exit_code == 0, r

        assert H.row_count(db, "waca2") > 0

        head_tree = repo.head.peel(pygit2.Tree)
        wc_tree_id = db.execute(
            """SELECT value FROM ".sno-meta" WHERE table_name='*' AND key='tree';"""
        ).fetchone()[0]
        assert wc_tree_id == head_tree.hex
        assert wc.assert_db_tree_match(head_tree)

        r = cli_runner.invoke(
            ["status"]
        )
        assert r.exit_code == 0, r
        assert r.stdout.splitlines()[-2:] == ['  nz_waca_adjustments/', '    deleted:   10 features']
