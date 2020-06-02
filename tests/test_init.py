import re

import pytest
import pygit2

from sno.working_copy import WorkingCopy
from sno.exceptions import (
    INVALID_ARGUMENT,
    INVALID_OPERATION,
    NO_IMPORT_SOURCE,
    NO_TABLE,
)

H = pytest.helpers.helpers()

# also in test_structure.py
GPKG_IMPORTS = (
    "archive,gpkg,table",
    [
        pytest.param(
            "gpkg-points", "nz-pa-points-topo-150k.gpkg", H.POINTS.LAYER, id="points"
        ),
        pytest.param(
            "gpkg-polygons",
            "nz-waca-adjustments.gpkg",
            H.POLYGONS.LAYER,
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
def test_init_import_single_table_source(data_archive_readonly, tmp_path, cli_runner):
    with data_archive_readonly("gpkg-points") as data:
        r = cli_runner.invoke(
            [
                "init",
                "--import",
                data / "nz-pa-points-topo-150k.gpkg",
                tmp_path / "emptydir",
            ]
        )
        # You don't have to specify a table if there's only one.
        assert r.exit_code == 0, r
        lines = r.stdout.splitlines()
        assert len(lines) >= 2
        assert "to nz_pa_points_topo_150k/ ..." in lines[1]
        assert "Commit: " in lines[-1]


@pytest.mark.slow
def test_init_import_custom_message(data_archive_readonly, tmp_path, cli_runner, chdir):
    with data_archive_readonly("gpkg-points") as data:
        r = cli_runner.invoke(
            [
                "init",
                "-m",
                "Custom message",
                "--import",
                data / "nz-pa-points-topo-150k.gpkg",
                tmp_path / "emptydir",
            ]
        )
        assert r.exit_code == 0, r
        with chdir(tmp_path / "emptydir"):
            r = cli_runner.invoke(["log", "-1"])
        assert r.exit_code == 0, r
        assert 'Custom message' in r.stdout


def test_init_import_table_with_prompt(data_archive_readonly, tmp_path, cli_runner):
    with data_archive_readonly("gpkg-au-census") as data:
        r = cli_runner.invoke(
            [
                "init",
                "--import",
                data / "census2016_sdhca_ot_short.gpkg",
                tmp_path / "emptydir",
            ],
            input="census2016_sdhca_ot_ced_short\n",
        )
        # Table was specified interactively via prompt
        assert r.exit_code == 0, r
        assert "Tables found:" in r.stdout
        assert (
            "  census2016_sdhca_ot_ced_short - census2016_sdhca_ot_ced_short"
            in r.stdout
        )
        assert "to census2016_sdhca_ot_ced_short/ ..." in r.stdout
        assert "Commit: " in r.stdout


def test_init_import_table_with_prompt_with_no_input(
    data_archive_readonly, tmp_path, cli_runner
):
    with data_archive_readonly("gpkg-au-census") as data:
        r = cli_runner.invoke(
            [
                "init",
                "--import",
                data / "census2016_sdhca_ot_short.gpkg",
                tmp_path / "emptydir",
            ],
        )
        # Table was specified interactively via prompt
        assert r.exit_code == NO_TABLE, r
        assert "Tables found:" in r.stdout
        assert (
            "  census2016_sdhca_ot_ced_short - census2016_sdhca_ot_ced_short"
            in r.stdout
        )
        assert "Invalid value for --table: No table specified" in r.stderr


def test_init_import_table_ogr_types(data_archive_readonly, tmp_path, cli_runner):
    with data_archive_readonly("types") as data:
        repo_path = tmp_path / "repo"
        r = cli_runner.invoke(["init", "--import", data / "types.gpkg", repo_path],)
        assert r.exit_code == 0, r

        # There's a bunch of wacky types in here, let's check them
        repo = pygit2.Repository(str(repo_path))
        wc = WorkingCopy.open(repo)
        with wc.session() as db:
            table_info = [
                dict(row) for row in db.cursor().execute("PRAGMA table_info('types');")
            ]
        assert table_info == [
            {
                'cid': 0,
                'name': 'fid',
                'type': 'INTEGER',
                'notnull': 1,
                'dflt_value': None,
                'pk': 1,
            },
            {
                'cid': 1,
                'name': 'int16',
                'type': 'SMALLINT',
                'notnull': 0,
                'dflt_value': None,
                'pk': 0,
            },
            {
                'cid': 2,
                'name': 'int32',
                'type': 'MEDIUMINT',
                'notnull': 0,
                'dflt_value': None,
                'pk': 0,
            },
            {
                'cid': 3,
                'name': 'int64',
                'type': 'INTEGER',
                'notnull': 0,
                'dflt_value': None,
                'pk': 0,
            },
            {
                'cid': 4,
                'name': 'boolean',
                'type': 'BOOLEAN',
                'notnull': 0,
                'dflt_value': None,
                'pk': 0,
            },
            {
                'cid': 5,
                'name': 'double',
                'type': 'REAL',
                'notnull': 0,
                'dflt_value': None,
                'pk': 0,
            },
            {
                'cid': 6,
                'name': 'float32',
                'type': 'FLOAT',
                'notnull': 0,
                'dflt_value': None,
                'pk': 0,
            },
            {
                'cid': 7,
                'name': 'string',
                'type': 'TEXT',
                'notnull': 0,
                'dflt_value': None,
                'pk': 0,
            },
            {
                'cid': 8,
                'name': 'blob',
                'type': 'BLOB',
                'notnull': 0,
                'dflt_value': None,
                'pk': 0,
            },
            {
                'cid': 9,
                'name': 'date',
                'type': 'DATE',
                'notnull': 0,
                'dflt_value': None,
                'pk': 0,
            },
            {
                'cid': 10,
                'name': 'datetime',
                'type': 'DATETIME',
                'notnull': 0,
                'dflt_value': None,
                'pk': 0,
            },
            {
                'cid': 11,
                'name': 'time',
                'type': 'TEXT',
                'notnull': 0,
                'dflt_value': None,
                'pk': 0,
            },
        ]


@pytest.mark.slow
@pytest.mark.parametrize(*GPKG_IMPORTS)
def test_init_import(
    archive, gpkg, table, data_archive, tmp_path, cli_runner, chdir, geopackage
):
    """ Import the GeoPackage (eg. `kx-foo-layer.gpkg`) into a Sno repository. """
    with data_archive(archive) as data:
        # list tables
        repo_path = tmp_path / "data.sno"
        repo_path.mkdir()

        with chdir(repo_path):
            r = cli_runner.invoke(
                ["init", "--import", f"gpkg:{data / gpkg}", f"--table={table}"]
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
            wc = repo_path / f"{repo_path.stem}.gpkg"
            assert wc.exists() and wc.is_file()
            print("workingcopy at", wc)

            assert repo.config["sno.workingcopy.version"] == "1"
            assert repo.config["sno.workingcopy.path"] == f"{wc.name}"

            db = geopackage(wc)
            assert H.row_count(db, table) > 0

            wc_tree_id = (
                db.cursor()
                .execute(
                    """SELECT value FROM ".sno-meta" WHERE table_name='*' AND key='tree';"""
                )
                .fetchone()[0]
            )
            assert wc_tree_id == repo.head.peel(pygit2.Tree).hex

            H.verify_gpkg_extent(db, table)


def test_init_import_name_clash(data_archive, cli_runner, geopackage):
    """ Import the GeoPackage into a Sno repository of the same name, and checkout a working copy of the same name. """
    with data_archive("gpkg-editing") as data:
        r = cli_runner.invoke(
            ["init", "--import", f"GPKG:editing.gpkg", "--table=editing", "editing"]
        )
        repo_path = data / "editing"

        assert r.exit_code == 0, r
        assert (repo_path / "HEAD").exists()

        repo = pygit2.Repository(str(repo_path))
        assert repo.is_bare
        assert not repo.is_empty

        # working copy exists
        wc = repo_path / f"editing.gpkg"
        assert wc.exists() and wc.is_file()
        print("workingcopy at", wc)

        assert repo.config["sno.workingcopy.version"] == "1"
        assert repo.config["sno.workingcopy.path"] == "editing.gpkg"

        db = geopackage(wc)
        dbcur = db.cursor()
        wc_rowcount = H.row_count(db, "editing")
        assert wc_rowcount > 0

        wc_tree_id = dbcur.execute(
            """SELECT value FROM ".sno-meta" WHERE table_name='*' AND key='tree';"""
        ).fetchone()[0]
        assert wc_tree_id == repo.head.peel(pygit2.Tree).hex

        # make sure we haven't stuffed up the original file
        dbo = geopackage("editing.gpkg")
        dbocur = dbo.cursor()
        dbocur.execute("SELECT 1 FROM sqlite_master WHERE name='.sno-meta';")
        assert dbocur.fetchall() == []
        source_rowcount = dbocur.execute("SELECT COUNT(*) FROM editing;").fetchone()[0]
        assert source_rowcount == wc_rowcount


@pytest.mark.slow
def test_init_import_errors(data_archive, tmp_path, chdir, cli_runner):
    gpkg = "census2016_sdhca_ot_short.gpkg"
    table = "census2016_sdhca_ot_ra_short"

    with data_archive("gpkg-au-census") as data:
        repo_path = tmp_path / "data.sno"
        repo_path.mkdir()
        with chdir(repo_path):
            r = cli_runner.invoke(["init", "--import", f"fred:thingz"])
            assert r.exit_code == NO_IMPORT_SOURCE, r
            assert "fred:thingz' doesn't appear to be valid" in r.stderr

            r = cli_runner.invoke(["init", "--import", f"gpkg:thingz.gpkg"])
            assert r.exit_code == NO_IMPORT_SOURCE, r
            assert "Couldn't find 'thingz.gpkg'" in r.stderr

            r = cli_runner.invoke(
                ["init", "--import", f"gpkg:{data/gpkg}", f"--table=no-existey"]
            )
            assert r.exit_code == NO_TABLE, r
            assert "Invalid value for --table: Table 'no-existey' not found" in r.stderr

            # not empty
            (repo_path / "a.file").touch()
            r = cli_runner.invoke(
                ["init", "--import", f"gpkg:{data/gpkg}", f"--table={table}", repo_path]
            )
            assert r.exit_code == INVALID_OPERATION, r
            assert "isn't empty" in r.stderr


def test_init_empty(tmp_path, cli_runner, chdir):
    """ Create an empty Sno repository. """
    repo_path = tmp_path / "data.sno"
    repo_path.mkdir()

    # empty dir
    r = cli_runner.invoke(["init", repo_path])
    assert r.exit_code == 0, r
    assert (repo_path / "HEAD").exists()

    # makes dir tree
    repo_path = tmp_path / "foo" / "bar" / "wiz.sno"
    r = cli_runner.invoke(["init", repo_path])
    assert r.exit_code == 0, r
    assert (repo_path / "HEAD").exists()

    # current dir
    repo_path = tmp_path / "planet.sno"
    repo_path.mkdir()
    with chdir(repo_path):
        r = cli_runner.invoke(["init"])
        assert r.exit_code == 0, r
        assert (repo_path / "HEAD").exists()

    # dir isn't empty
    repo_path = tmp_path / "tree"
    repo_path.mkdir()
    (repo_path / "a.file").touch()
    r = cli_runner.invoke(["init", repo_path])
    assert r.exit_code == INVALID_OPERATION, r
    assert not (repo_path / "HEAD").exists()

    # current dir isn't empty
    with chdir(repo_path):
        r = cli_runner.invoke(["init"])
        assert r.exit_code == INVALID_OPERATION, r
        assert not (repo_path / "HEAD").exists()


@pytest.mark.slow
def test_init_import_alt_names(data_archive, tmp_path, cli_runner, chdir, geopackage):
    """ Import the GeoPackage (eg. `kx-foo-layer.gpkg`) into a Sno repository. """
    repo_path = tmp_path / "data.sno"
    repo_path.mkdir()

    r = cli_runner.invoke(["init", repo_path])
    assert r.exit_code == 0, r

    ARCHIVE_PATHS = (
        (
            "gpkg-points",
            "nz-pa-points-topo-150k.gpkg",
            "nz_pa_points_topo_150k",
            "pa_sites",
        ),
        (
            "gpkg-polygons",
            "nz-waca-adjustments.gpkg",
            "nz_waca_adjustments",
            "misc/waca",
        ),
        (
            "gpkg-polygons",
            "nz-waca-adjustments.gpkg",
            "nz_waca_adjustments",
            "other/waca2",
        ),
    )

    for archive, source_gpkg, source_table, import_path in ARCHIVE_PATHS:
        with data_archive(archive) as source_path:
            with chdir(repo_path):
                r = cli_runner.invoke(
                    [
                        "import",
                        f"GPKG:{source_path / source_gpkg}",
                        f"--table={source_table}:{import_path}",
                    ]
                )
                assert r.exit_code == 0, r

    with chdir(repo_path):
        r = cli_runner.invoke(["checkout", "--path=wc.gpkg", "HEAD"])
        assert r.exit_code == 0, r

        # working copy exists
        db = geopackage("wc.gpkg")
        dbcur = db.cursor()

        expected_tables = set(a[3].replace("/", "__") for a in ARCHIVE_PATHS)
        db_tables = set(
            r[0]
            for r in dbcur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        )
        assert expected_tables <= db_tables

        for gpkg_t in (
            "gpkg_contents",
            "gpkg_geometry_columns",
            "gpkg_metadata_reference",
        ):
            table_list = set(
                r[0]
                for r in dbcur.execute(f"SELECT DISTINCT table_name FROM {gpkg_t};")
            )
            assert expected_tables >= table_list, gpkg_t

        r = cli_runner.invoke(["diff"])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines() == []


@pytest.mark.slow
def test_init_import_home_resolve(
    data_archive, tmp_path, cli_runner, chdir, monkeypatch
):
    """ Import from a ~-specified gpkg path """
    repo_path = tmp_path / "data.sno"
    repo_path.mkdir()

    r = cli_runner.invoke(["init", repo_path])
    assert r.exit_code == 0, r

    with data_archive("gpkg-points") as source_path:
        with chdir(repo_path):
            monkeypatch.setenv("HOME", str(source_path))

            r = cli_runner.invoke(
                [
                    "import",
                    "GPKG:~/nz-pa-points-topo-150k.gpkg",
                    "--table=nz_pa_points_topo_150k",
                ]
            )
            assert r.exit_code == 0, r


@pytest.mark.slow
def test_import_existing_wc(
    data_archive,
    data_working_copy,
    geopackage,
    cli_runner,
    insert,
    tmp_path,
    request,
    chdir,
):
    """ Import a new dataset into a repo with an existing working copy. Dataset should get checked out """
    with data_working_copy("points") as (repo_path, wcdb):
        with data_archive("gpkg-polygons") as source_path, chdir(repo_path):
            r = cli_runner.invoke(
                [
                    "import",
                    f"GPKG:{source_path / 'nz-waca-adjustments.gpkg'}",
                    f"--table={H.POLYGONS.LAYER}",
                ]
            )
            assert r.exit_code == 0, r

        repo = pygit2.Repository(str(repo_path))
        wc = WorkingCopy.open(repo)
        db = geopackage(wcdb)

        assert H.row_count(db, "nz_waca_adjustments") > 0

        head_tree = repo.head.peel(pygit2.Tree)
        with db:
            dbcur = db.cursor()
            dbcur.execute(
                """SELECT value FROM ".sno-meta" WHERE table_name='*' AND key='tree';"""
            )
            wc_tree_id = dbcur.fetchone()[0]
        assert wc_tree_id == head_tree.hex
        assert wc.assert_db_tree_match(head_tree)

        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines()[-1] == "Nothing to commit, working copy clean"

        with db:
            dbcur = db.cursor()
            dbcur.execute(
                "DELETE FROM nz_waca_adjustments WHERE rowid IN (SELECT rowid FROM nz_waca_adjustments ORDER BY id LIMIT 10);"
            )
            dbcur.execute("SELECT changes()")
            assert dbcur.fetchone()[0] == 10

        with data_archive("gpkg-polygons") as source_path, chdir(repo_path):
            r = cli_runner.invoke(
                [
                    "import",
                    f"GPKG:{source_path / 'nz-waca-adjustments.gpkg'}",
                    f"--table={H.POLYGONS.LAYER}:waca2",
                ]
            )
            assert r.exit_code == 0, r

        assert H.row_count(db, "waca2") > 0

        head_tree = repo.head.peel(pygit2.Tree)
        with db:
            dbcur = db.cursor()
            dbcur.execute(
                """SELECT value FROM ".sno-meta" WHERE table_name='*' AND key='tree';"""
            )
            wc_tree_id = dbcur.fetchone()[0]
        assert wc_tree_id == head_tree.hex
        assert wc.assert_db_tree_match(head_tree)

        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines()[-2:] == [
            "  nz_waca_adjustments/",
            "    deleted:   10 features",
        ]
