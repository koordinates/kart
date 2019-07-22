import hashlib
import os
import re
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest  # noqa

import pygit2

from snowdrop import cli

""" Simple integration/E2E tests """

# Test Dataset (gpkg-points / points.snow)
POINTS_LAYER = "nz_pa_points_topo_150k"
POINTS_LAYER_PK = "fid"
POINTS_INSERT = f"""
    INSERT INTO {POINTS_LAYER}
                    (fid, geom, t50_fid, name_ascii, macronated, name)
                VALUES
                    (:fid, AsGPB(GeomFromEWKT(:geom)), :t50_fid, :name_ascii, :macronated, :name);
"""
POINTS_RECORD = {
    "fid": 9999,
    "geom": "POINT(0 0)",
    "t50_fid": 9_999_999,
    "name_ascii": "Te Motu-a-kore",
    "macronated": False,
    "name": "Te Motu-a-kore",
}
POINTS_HEAD_SHA = 'd1bee0841307242ad7a9ab029dc73c652b9f74f3'

# Test Dataset (gpkg-polygons / polygons.snow)
POLYGONS_LAYER = "nz_waca_adjustments"
POLYGONS_LAYER_PK = "id"
POLYGONS_INSERT = f"""
    INSERT INTO {POLYGONS_LAYER}
                    (id, geom, date_adjusted, survey_reference, adjusted_nodes)
                VALUES
                    (:id, AsGPB(GeomFromEWKT(:geom)), :date_adjusted, :survey_reference, :adjusted_nodes);
"""
POLYGONS_RECORD = {
    "id": 9_999_999,
    "geom": "POLYGON((0 0, 0 0.001, 0.001 0.001, 0.001 0, 0 0))",
    "date_adjusted": "2019-07-05T13:04:00+01:00",
    "survey_reference": "Null Island™ 🗺",
    "adjusted_nodes": 123,
}
POLYGONS_HEAD_SHA = '1c3bb605b91c7a7d2d149cb545dcd0e2ee3df14b'

# Test Dataset (gpkg-spec / table.snow)

TABLE_LAYER = "countiestbl"
TABLE_LAYER_PK = "OBJECTID"
TABLE_INSERT = f"""
    INSERT INTO {TABLE_LAYER}
                    (OBJECTID, NAME, STATE_NAME, STATE_FIPS, CNTY_FIPS, FIPS, AREA, POP1990, POP2000, POP90_SQMI, Shape_Leng, Shape_Area)
                VALUES
                    (:OBJECTID, :NAME, :STATE_NAME, :STATE_FIPS, :CNTY_FIPS, :FIPS, :AREA, :POP1990, :POP2000, :POP90_SQMI, :Shape_Leng, :Shape_Area);
"""
TABLE_RECORD = {
    "OBJECTID": 9999,
    "NAME": "Lake of the Gruffalo",
    "STATE_NAME": "Minnesota",
    "STATE_FIPS": "27",
    "CNTY_FIPS": "077",
    "FIPS": "27077",
    "AREA": 1784.0634,
    "POP1990": 4076,
    "POP2000": 4651,
    "POP90_SQMI": 2,
    "Shape_Leng": 4.05545998243992,
    "Shape_Area": 0.565449933741451,
}
TABLE_HEAD_SHA = 'e4e9cfae9fe05945bacbfc45d8ea250cdf68b55e'


def _last_change_time(db):
    """
    Get the last change time from the GeoPackage DB.
    This is the same as the commit time.
    """
    return db.execute(
        f"SELECT last_change FROM gpkg_contents WHERE table_name=?;", [POINTS_LAYER]
    ).fetchone()[0]


def _clear_working_copy(repo_path="."):
    """ Delete any existing working copy & associated config """
    repo = pygit2.Repository(repo_path)
    if "kx.workingcopy" in repo.config:
        print(f"Deleting existing working copy: {repo.config['kx.workingcopy']}")
        fmt, working_copy, layer = repo.config["kx.workingcopy"].split(":")
        working_copy = Path(working_copy)
        if working_copy.exists():
            working_copy.unlink()
        del repo.config["kx.workingcopy"]


def _db_table_hash(db, table, pk=None):
    """ Calculate a SHA1 hash of the contents of a SQLite table """
    if pk is None:
        pk = "ROWID"

    sql = f"SELECT * FROM {table} ORDER BY {pk};"
    r = db.execute(sql)
    h = hashlib.sha1()
    for row in r:
        h.update("🔸".join(repr(col) for col in row).encode("utf-8"))
    return h.hexdigest()


def _git_graph(request, message, count=10, *paths):
    """ Print a pretty graph of recent git revisions """
    cmd = ["git", "log", "--all", "--decorate", "--oneline", "--graph", f"--max-count={count}"]

    # total hackery to figure out whether we're _actually_ in a terminal
    try:
        cm = request.config.pluginmanager.getplugin("capturemanager")
        fd = cm._global_capturing.in_.targetfd_save
        if os.isatty(fd):
            cmd += ["--color=always"]
    except Exception:
        pass

    print(f"{message}:")
    subprocess.check_call(cmd + list(paths))


@pytest.fixture
def insert(request, cli_runner):
    def func(db, layer=None, commit=True, reset_index=None):
        if reset_index is not None:
            func.index = reset_index

        if layer is None:
            # autodetect
            layer = db.execute("SELECT table_name FROM gpkg_contents WHERE table_name IN (?,?,?) LIMIT 1",
                               [POINTS_LAYER, POLYGONS_LAYER, TABLE_LAYER]
                               ).fetchone()[0]

        if layer == POINTS_LAYER:
            rec = POINTS_RECORD.copy()
            pk_field = POINTS_LAYER_PK
            sql = POINTS_INSERT
            pk_start = 98000
        elif layer == POLYGONS_LAYER:
            rec = POLYGONS_RECORD.copy()
            pk_field = POLYGONS_LAYER_PK
            sql = POLYGONS_INSERT
            pk_start = 98000
        elif layer == TABLE_LAYER:
            rec = TABLE_RECORD.copy()
            pk_field = TABLE_LAYER_PK
            sql = TABLE_INSERT
            pk_start = 98000
        else:
            raise NotImplementedError(f"Layer {layer}")

        # th
        new_pk = pk_start + func.index
        rec[pk_field] = new_pk

        with db:
            cur = db.cursor()
            cur.execute(sql, rec)
            assert cur.rowcount == 1
            func.inserted_fids.append(new_pk)

        func.index += 1

        if commit:
            r = cli_runner.invoke(["commit", "-m", f"commit-{func.index}"])
            assert r.exit_code == 0, r

            commit_id = r.stdout.splitlines()[-1].split(": ")[1]
            return commit_id
        else:
            return new_pk

    func.index = 0
    func.inserted_fids = []

    return func


@pytest.mark.slow
@pytest.mark.parametrize("archive,gpkg,table", [
    pytest.param('gpkg-points', 'nz-pa-points-topo-150k.gpkg', POINTS_LAYER, id='points'),
    pytest.param('gpkg-polygons', 'nz-waca-adjustments.gpkg', POLYGONS_LAYER, id='polygons-pk'),
    pytest.param('gpkg-au-census', 'census2016_sdhca_ot_short.gpkg', 'census2016_sdhca_ot_ra_short', id='au-ra-short'),
    pytest.param('gpkg-spec', 'sample1_2.gpkg', 'counties', id='spec-counties'),
    pytest.param('gpkg-spec', 'sample1_2.gpkg', 'countiestbl', id='spec-counties-table'),
])
def test_import_geopackage(archive, gpkg, table, data_archive, tmp_path, cli_runner):
    """ Import the GeoPackage (eg. `kx-foo-layer.gpkg`) into a Snowdrop repository. """
    with data_archive(archive) as data:
        # list tables
        repo_path = tmp_path / "data.snow"
        r = cli_runner.invoke(
            [
                "import-gpkg",
                f"--list-tables",
                data / gpkg,
            ]
        )
        assert r.exit_code == 0, r
        lines = r.stdout.splitlines()
        assert len(lines) >= 2
        assert lines[0] == f'GeoPackage tables in \'{data / gpkg}\':'
        assert any(re.match(fr"^{table}\s+- ", l) for l in lines[1:])

        # successful import
        r = cli_runner.invoke(
            [
                f"--repo={repo_path}",
                "import-gpkg",
                data / gpkg,
                table,
            ]
        )
        assert r.exit_code == 0, r
        assert (repo_path / "HEAD").exists()

        repo = pygit2.Repository(str(repo_path))
        assert repo.is_bare
        assert not repo.is_empty

        assert repo.head.name == 'refs/heads/master'
        assert repo.head.shorthand == 'master'

        # has a single commit
        assert len([c for c in repo.walk(repo.head.target)]) == 1

        # existing
        r = cli_runner.invoke(
            [
                f"--repo={repo_path}",
                "import-gpkg",
                data / gpkg,
                table,
            ]
        )
        assert r.exit_code == 1, r
        assert 'Looks like you already have commits in this repository' in r.stdout


def test_import_geopackage_errors(data_archive, tmp_path, cli_runner):
    with data_archive("gpkg-points") as data:
        # missing/bad table name
        repo_path = tmp_path / "data2.snow"
        r = cli_runner.invoke(
            [
                f"--repo={repo_path}",
                "import-gpkg",
                data / 'nz-pa-points-topo-150k.gpkg',
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
            [
                f"--repo={repo_path}",
                "import-gpkg",
                tmp_path / "a.gpkg",
                "mytable",
            ]
        )
        assert r.exit_code == 2, r
        assert "a.gpkg' doesn't appear to be a valid GeoPackage" in r.stdout


@pytest.mark.parametrize("archive,table,commit_sha", [
    pytest.param('points.snow', POINTS_LAYER, POINTS_HEAD_SHA, id='points'),
    pytest.param('polygons.snow', POLYGONS_LAYER, POLYGONS_HEAD_SHA, id='polygons-pk'),
    pytest.param('table.snow', TABLE_LAYER, TABLE_HEAD_SHA, id='table'),
])
def test_checkout_workingcopy(archive, table, commit_sha, data_archive, tmp_path, cli_runner, geopackage):
    """ Checkout a working copy to edit """
    with data_archive(archive) as repo_path:
        _clear_working_copy()

        wc = tmp_path / f"{table}.gpkg"
        r = cli_runner.invoke(
            ["checkout", f"--layer={table}", f"--working-copy={wc}"]
        )
        assert r.exit_code == 0, r
        lines = r.stdout.splitlines()
        assert re.match(fr"Checkout {table}@HEAD to .+ as GPKG \.\.\.$", lines[0])
        assert re.match(fr"Commit: {commit_sha} Tree: [a-f\d]{{40}}$", lines[1])

        assert wc.exists()
        db = geopackage(wc)
        nrows = db.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
        assert nrows > 0

        repo = pygit2.Repository(str(repo_path))
        assert repo.is_bare

        assert repo.head.name == 'refs/heads/master'
        assert repo.head.shorthand == 'master'

        wc_tree_id = db.execute("SELECT value FROM __kxg_meta WHERE table_name=? AND key='tree';", [table]).fetchone()[0]
        assert wc_tree_id == repo.head.peel(pygit2.Tree).hex


@pytest.mark.parametrize("archive,table", [
    pytest.param('points.snow', POINTS_LAYER, id='points'),
    pytest.param('polygons.snow', POLYGONS_LAYER, id='polygons-pk'),
    pytest.param('table.snow', TABLE_LAYER, id='table'),
])
def test_diff(archive, table, data_working_copy, geopackage, cli_runner):
    """ diff the working copy against the repository (no index!) """
    with data_working_copy(archive) as (repo, wc):
        # empty
        r = cli_runner.invoke(["diff"])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines() == []

        # make some changes
        db = geopackage(wc)
        with db:
            cur = db.cursor()
            if table == POINTS_LAYER:
                cur.execute(POINTS_INSERT, POINTS_RECORD)
                assert cur.rowcount == 1
                cur.execute(f"UPDATE {POINTS_LAYER} SET fid=9998 WHERE fid=1;")
                assert cur.rowcount == 1
                cur.execute(
                    f"UPDATE {POINTS_LAYER} SET name='test', t50_fid=NULL WHERE fid=2;"
                )
                assert cur.rowcount == 1
                cur.execute(f"DELETE FROM {POINTS_LAYER} WHERE fid=3;")
                assert cur.rowcount == 1

            elif table == POLYGONS_LAYER:
                cur.execute(POLYGONS_INSERT, POLYGONS_RECORD)
                assert cur.rowcount == 1
                cur.execute(f"UPDATE {POLYGONS_LAYER} SET id=9998 WHERE id=1424927;")
                assert cur.rowcount == 1
                cur.execute(
                    f"UPDATE {POLYGONS_LAYER} SET survey_reference='test', date_adjusted='2019-01-01T00:00:00Z' WHERE id=1443053;"
                )
                assert cur.rowcount == 1
                cur.execute(f"DELETE FROM {POLYGONS_LAYER} WHERE id=1452332;")
                assert cur.rowcount == 1

            elif table == TABLE_LAYER:
                cur.execute(TABLE_INSERT, TABLE_RECORD)
                assert cur.rowcount == 1
                cur.execute(f"UPDATE {TABLE_LAYER} SET \"OBJECTID\"=9998 WHERE OBJECTID=1;")
                assert cur.rowcount == 1
                cur.execute(
                    f"UPDATE {TABLE_LAYER} SET name='test', POP2000=9867 WHERE OBJECTID=2;"
                )
                assert cur.rowcount == 1
                cur.execute(f'DELETE FROM {TABLE_LAYER} WHERE "OBJECTID"=3;')
                assert cur.rowcount == 1

            else:
                raise NotImplementedError(f"table={table}")

        r = cli_runner.invoke(["diff"])
        assert r.exit_code == 0, r
        if table == POINTS_LAYER:
            assert r.stdout.splitlines() == [
                "--- 2bad8ad5-97aa-4910-9e6c-c6e8e692700d",
                "-                                      fid = 3",
                "-                                     geom = POINT(...)",
                "-                               macronated = N",
                "-                                     name = Tauwhare Pa",
                "-                               name_ascii = Tauwhare Pa",
                "-                                  t50_fid = 2426273",
                "+++ {new feature}",
                "+                                      fid = 9999",
                "+                                     geom = POINT(...)",
                "+                                  t50_fid = 9999999",
                "+                               name_ascii = Te Motu-a-kore",
                "+                               macronated = 0",
                "+                                     name = Te Motu-a-kore",
                "--- 7416b7ab-992d-4595-ab96-39a186f5968a",
                "+++ 7416b7ab-992d-4595-ab96-39a186f5968a",
                "-                                      fid = 1",
                "+                                      fid = 9998",
                "--- 905a8ae1-8346-4b42-8646-42385beac87f",
                "+++ 905a8ae1-8346-4b42-8646-42385beac87f",
                "                                       fid = 2",
                "-                                     name = ␀",
                "+                                     name = test",
                "-                                  t50_fid = 2426272",
                "+                                  t50_fid = ␀",
            ]
        elif table == POLYGONS_LAYER:
            assert r.stdout.splitlines() == [
                "--- a015ce37-666c-4f90-889b-6c035300dc59",
                "-                           adjusted_nodes = 558",
                "-                            date_adjusted = 2011-06-07T15:22:58Z",
                "-                                     geom = MULTIPOLYGON(...)",
                "-                                       id = 1452332",
                "-                         survey_reference = ␀",
                "+++ {new feature}",
                "+                                       id = 9999999",
                "+                                     geom = POLYGON(...)",
                "+                            date_adjusted = 2019-07-05T13:04:00+01:00",
                "+                         survey_reference = Null Island™ 🗺",
                "+                           adjusted_nodes = 123",
                "--- a7bdd03b-2aa9-41f8-91a7-abb5d5ec621b",
                "+++ a7bdd03b-2aa9-41f8-91a7-abb5d5ec621b",
                "-                                       id = 1424927",
                "+                                       id = 9998",
                "--- ec360eb3-f57e-47d0-bda5-a0a02231ff6c",
                "+++ ec360eb3-f57e-47d0-bda5-a0a02231ff6c",
                "                                        id = 1443053",
                "-                            date_adjusted = 2011-05-10T12:09:10Z",
                "+                            date_adjusted = 2019-01-01T00:00:00Z",
                "-                         survey_reference = ␀",
                "+                         survey_reference = test",
            ]
        elif table == TABLE_LAYER:
            assert r.stdout.splitlines() == [
                "--- b8e60439-89e2-4432-8fa3-189e2f7813e4",
                "-                                     AREA = 2529.9794",
                "-                                CNTY_FIPS = 065",
                "-                                     FIPS = 53065",
                "-                                     NAME = Stevens",
                "-                                 OBJECTID = 3",
                "-                                  POP1990 = 30948.0",
                "-                                  POP2000 = 40652.0",
                "-                               POP90_SQMI = 12",
                "-                               STATE_FIPS = 53",
                "-                               STATE_NAME = Washington",
                "-                               Shape_Area = 0.7954858988987561",
                "-                               Shape_Leng = 4.876296245235406",
                "+++ {new feature}",
                "+                                 OBJECTID = 9999",
                "+                                     NAME = Lake of the Gruffalo",
                "+                               STATE_NAME = Minnesota",
                "+                               STATE_FIPS = 27",
                "+                                CNTY_FIPS = 077",
                "+                                     FIPS = 27077",
                "+                                     AREA = 1784.0634",
                "+                                  POP1990 = 4076.0",
                "+                                  POP2000 = 4651.0",
                "+                               POP90_SQMI = 2",
                "+                               Shape_Leng = 4.05545998243992",
                "+                               Shape_Area = 0.565449933741451",
                "--- 326581a9-766f-48ad-a30b-0fd8dddd5f83",
                "+++ 326581a9-766f-48ad-a30b-0fd8dddd5f83",
                "                                  OBJECTID = 2",
                "-                                     NAME = Ferry",
                "+                                     NAME = test",
                "-                                  POP2000 = 7199.0",
                "+                                  POP2000 = 9867.0",
                "--- bd2d37e1-1183-433b-98d0-076b5cb1b5be",
                "+++ bd2d37e1-1183-433b-98d0-076b5cb1b5be",
                "-                                 OBJECTID = 1",
                "+                                 OBJECTID = 9998",
            ]


@pytest.mark.parametrize("archive,layer", [
    pytest.param('points.snow', POINTS_LAYER, id='points'),
    pytest.param('polygons.snow', POLYGONS_LAYER, id='polygons-pk'),
    pytest.param('table.snow', TABLE_LAYER, id='table'),
])
def test_commit(archive, layer, data_working_copy, geopackage, cli_runner):
    """ commit outstanding changes from the working copy """
    with data_working_copy(archive) as (repo, wc):
        # empty
        r = cli_runner.invoke(["commit", "-m", "test-commit-0"])
        assert r.exit_code == 1, r
        assert r.stdout.splitlines() == ['Error: No changes to commit']

        # make some changes
        db = geopackage(wc)
        with db:
            cur = db.cursor()
            if layer == POINTS_LAYER:
                cur.execute(POINTS_INSERT, POINTS_RECORD)
                assert cur.rowcount == 1
                cur.execute(f"UPDATE {POINTS_LAYER} SET fid=9998 WHERE fid=1;")
                assert cur.rowcount == 1
                cur.execute(f"UPDATE {POINTS_LAYER} SET name='test' WHERE fid=2;")
                assert cur.rowcount == 1
                cur.execute(f"DELETE FROM {POINTS_LAYER} WHERE fid IN (3,30,31,32,33);")
                assert cur.rowcount == 5
                pk_del = 3
            elif layer == POLYGONS_LAYER:
                cur.execute(POLYGONS_INSERT, POLYGONS_RECORD)
                assert cur.rowcount == 1
                cur.execute(f"UPDATE {POLYGONS_LAYER} SET id=9998 WHERE id=1424927;")
                assert cur.rowcount == 1
                cur.execute(f"UPDATE {POLYGONS_LAYER} SET survey_reference='test' WHERE id=1443053;")
                assert cur.rowcount == 1
                cur.execute(f"DELETE FROM {POLYGONS_LAYER} WHERE id IN (1452332, 1456853, 1456912, 1457297, 1457355);")
                assert cur.rowcount == 5
                pk_del = 1452332
            elif layer == TABLE_LAYER:
                cur.execute(TABLE_INSERT, TABLE_RECORD)
                assert cur.rowcount == 1
                cur.execute(f"UPDATE {TABLE_LAYER} SET OBJECTID=9998 WHERE OBJECTID=1;")
                assert cur.rowcount == 1
                cur.execute(f"UPDATE {TABLE_LAYER} SET name='test' WHERE OBJECTID=2;")
                assert cur.rowcount == 1
                cur.execute(f"DELETE FROM {TABLE_LAYER} WHERE OBJECTID IN (3,30,31,32,33);")
                assert cur.rowcount == 5
                pk_del = 3
            else:
                raise NotImplementedError(f"layer={layer}")

        fk_del = cur.execute(
            f"SELECT feature_key FROM __kxg_map WHERE table_name=? AND feature_id=?;",
            [layer, pk_del]
        ).fetchone()[0]
        print("deleted fid={pk_del}, feature_key={fk_del}")

        r = cli_runner.invoke(["commit", "-m", "test-commit-1"])
        assert r.exit_code == 0, r
        commit_id = r.stdout.splitlines()[-1].split(": ")[1]
        print("commit:", commit_id)

        r = pygit2.Repository(str(repo))
        assert str(r.head.target) == commit_id

        tree = r.head.peel(pygit2.Tree)
        assert f"{layer}/features/{fk_del[:4]}/{fk_del}/geom" not in tree

        change_count = cur.execute(
            "SELECT COUNT(*) FROM __kxg_map WHERE table_name=? AND state!=0;",
            [layer]
        ).fetchone()[0]
        assert change_count == 0, "Changes still listed in __kxg_map"

        del_map_record = cur.execute(
            "SELECT 1 FROM __kxg_map WHERE table_name=? AND feature_key=?;",
            [layer, fk_del]
        ).fetchone()
        assert del_map_record is None, "Deleted feature still in __kxg_map"

        map_count, feature_count = cur.execute(
            f"""
                SELECT
                    (SELECT COUNT(*) FROM __kxg_map WHERE table_name=?) AS map_count,
                    (SELECT COUNT(*) FROM {layer}) AS feature_count;
            """,
            [layer]
        ).fetchone()
        print("map_count=", map_count, "feature_count=", feature_count)
        assert map_count == feature_count

        r = cli_runner.invoke(["diff"])
        assert r.exit_code == 0, r
        assert r.stdout == ''


def test_log(data_archive, cli_runner):
    """ review commit history """
    with data_archive("points.snow"):
        r = cli_runner.invoke(["log"])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines() == [
            "commit d1bee0841307242ad7a9ab029dc73c652b9f74f3",
            "Author: Robert Coup <robert@coup.net.nz>",
            "Date:   Thu Jun 20 15:28:33 2019 +0100",
            "",
            "    Improve naming on Coromandel East coast",
            "",
            "commit edd5a4b02a7d2ce608f1839eea5e3a8ddb874e00",
            "Author: Robert Coup <robert@coup.net.nz>",
            "Date:   Tue Jun 11 12:03:58 2019 +0100",
            "",
            "    Import from nz-pa-points-topo-150k.gpkg",
        ]


def test_show(data_archive, cli_runner):
    """ review commit history """
    with data_archive("points.snow"):
        r = cli_runner.invoke(["show"])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines() == [
            "commit d1bee0841307242ad7a9ab029dc73c652b9f74f3",
            "Author: Robert Coup <robert@coup.net.nz>",
            "Date:   Thu Jun 20 15:28:33 2019 +0100",
            "",
            "    Improve naming on Coromandel East coast",
        ]


def test_tag(data_working_copy, cli_runner):
    """ review commit history """
    with data_working_copy("points.snow") as (repo_dir, wc):
        # create a tag
        r = cli_runner.invoke(["tag", "version1"])
        assert r.exit_code == 0, r

        repo = pygit2.Repository(str(repo_dir))
        assert 'refs/tags/version1' in repo.references
        ref = repo.lookup_reference_dwim('version1')
        assert ref.target.hex == POINTS_HEAD_SHA


def test_push(data_archive, tmp_path, cli_runner):
    with data_archive("points.snow"):
        subprocess.run(["git", "init", "--bare", tmp_path], check=True)

        r = cli_runner.invoke(["remote", "add", "myremote", tmp_path])
        assert r.exit_code == 0, r

        r = cli_runner.invoke(["push", "--set-upstream", "myremote", "master"])
        assert r.exit_code == 0, r


def test_checkout_detached(data_working_copy, cli_runner, geopackage):
    """ Checkout a working copy to edit """
    with data_working_copy("points.snow") as (repo_dir, wc):
        db = geopackage(wc)
        assert _last_change_time(db) == "2019-06-20T14:28:33.000000Z"

        # checkout the previous commit
        r = cli_runner.invoke(["checkout", "edd5a4b02a7d2ce608f1839eea5e3a8ddb874e00"])
        assert r.exit_code == 0, r
        assert _last_change_time(db) == "2019-06-11T11:03:58.000000Z"

        repo = pygit2.Repository(str(repo_dir))
        assert repo.head.name == 'HEAD'
        assert repo.head_is_detached
        assert repo.head.target.hex == 'edd5a4b02a7d2ce608f1839eea5e3a8ddb874e00'


def test_checkout_references(data_working_copy, cli_runner, geopackage, tmp_path):
    with data_working_copy("points.snow") as (repo_dir, wc):
        db = geopackage(wc)
        repo = pygit2.Repository(str(repo_dir))

        # create a tag
        repo.create_reference('refs/tags/version1', repo.head.target)

        subprocess.run(["git", "init", "--bare", tmp_path], check=True)

        r = cli_runner.invoke(["remote", "add", "myremote", tmp_path])
        assert r.exit_code == 0, r

        r = cli_runner.invoke(["push", "myremote", "master"])
        assert r.exit_code == 0, r

        def r_head():
            return (repo.head.name, repo.head.target.hex)

        # checkout the HEAD commit
        r = cli_runner.invoke(["checkout", "HEAD"])
        assert r.exit_code == 0, r
        assert _last_change_time(db) == "2019-06-20T14:28:33.000000Z"
        assert not repo.head_is_detached
        assert r_head() == ('refs/heads/master', POINTS_HEAD_SHA)

        # checkout the HEAD-but-1 commit
        r = cli_runner.invoke(["checkout", "HEAD~1"])
        assert r.exit_code == 0, r
        assert _last_change_time(db) == "2019-06-11T11:03:58.000000Z"
        assert repo.head_is_detached
        assert r_head() == ('HEAD', 'edd5a4b02a7d2ce608f1839eea5e3a8ddb874e00')

        # checkout the master HEAD via branch-name
        r = cli_runner.invoke(["checkout", "master"])
        assert r.exit_code == 0, r
        assert _last_change_time(db) == "2019-06-20T14:28:33.000000Z"
        assert not repo.head_is_detached
        assert r_head() == ('refs/heads/master', POINTS_HEAD_SHA)

        # checkout a short-sha commit
        r = cli_runner.invoke(["checkout", "edd5a4b"])
        assert r.exit_code == 0, r
        assert _last_change_time(db) == "2019-06-11T11:03:58.000000Z"
        assert repo.head_is_detached
        assert r_head() == ('HEAD', 'edd5a4b02a7d2ce608f1839eea5e3a8ddb874e00')

        # checkout the master HEAD via refspec
        r = cli_runner.invoke(["checkout", "refs/heads/master"])
        assert r.exit_code == 0, r
        assert _last_change_time(db) == "2019-06-20T14:28:33.000000Z"
        assert not repo.head_is_detached
        assert r_head() == ('refs/heads/master', POINTS_HEAD_SHA)

        # checkout the tag
        r = cli_runner.invoke(["checkout", "version1"])
        assert r.exit_code == 0, r
        assert _last_change_time(db) == "2019-06-20T14:28:33.000000Z"
        assert repo.head_is_detached
        assert r_head() == ('HEAD', POINTS_HEAD_SHA)

        # checkout the remote branch
        r = cli_runner.invoke(["checkout", "myremote/master"])
        assert r.exit_code == 0, r
        assert _last_change_time(db) == "2019-06-20T14:28:33.000000Z"
        assert repo.head_is_detached
        assert r_head() == ('HEAD', POINTS_HEAD_SHA)


@pytest.mark.parametrize("archive,layer", [
    pytest.param('points.snow', POINTS_LAYER, id='points'),
    pytest.param('polygons.snow', POLYGONS_LAYER, id='polygons-pk'),
    pytest.param('table.snow', TABLE_LAYER, id='table'),
])
@pytest.mark.parametrize("via", [
    pytest.param('reset', id='via-reset'),
    pytest.param('checkout', id='via-checkout')
])
def test_working_copy_reset(archive, layer, via, data_working_copy, cli_runner, geopackage):
    """
    Check that we reset any working-copy changes correctly before doing any new checkout

    We can do this via `snow reset` or `snow checkout --force HEAD`
    """
    if layer == POINTS_LAYER:
        pk_field = POINTS_LAYER_PK
        rec = POINTS_RECORD
        sql = POINTS_INSERT
        del_pk = 5
        upd_field = 't50_fid'
        upd_field_value = 888888
        upd_pk_range = (10, 15)
        id_chg_pk = 20
    elif layer == POLYGONS_LAYER:
        pk_field = POLYGONS_LAYER_PK
        rec = POLYGONS_RECORD
        sql = POLYGONS_INSERT
        del_pk = 1456912
        upd_field = 'survey_reference'
        upd_field_value = 'test'
        upd_pk_range = (1459750, 1460312)
        id_chg_pk = 1460583
    elif layer == TABLE_LAYER:
        pk_field = TABLE_LAYER_PK
        rec = TABLE_RECORD
        sql = TABLE_INSERT
        del_pk = 5
        upd_field = 'name'
        upd_field_value = 'test'
        upd_pk_range = (10, 15)
        id_chg_pk = 20
    else:
        raise NotImplementedError(f"layer={layer}")

    with data_working_copy(archive, force_new=True) as (repo_dir, wc):
        db = geopackage(wc)

        h_before = _db_table_hash(db, layer, pk_field)

        with db:
            cur = db.cursor()
            cur.execute(sql, rec)
            assert cur.rowcount == 1
            cur.execute(f"DELETE FROM {layer} WHERE {pk_field} < {del_pk};")
            assert cur.rowcount == 4
            cur.execute(
                f"UPDATE {layer} SET {upd_field} = ? WHERE {pk_field}>=? AND {pk_field}<?;",
                [upd_field_value, upd_pk_range[0], upd_pk_range[1]]
            )
            assert cur.rowcount == 5
            cur.execute(f"UPDATE {layer} SET {pk_field}=? WHERE {pk_field}=?;", [9998, id_chg_pk])
            assert cur.rowcount == 1

            change_count = db.execute(
                "SELECT COUNT(*) FROM __kxg_map WHERE state != 0"
            ).fetchone()[0]
            assert change_count == (1 + 4 + 5 + 1)

        if via == 'reset':
            # using `snow reset`
            r = cli_runner.invoke(["reset"])
            assert r.exit_code == 0, r
        elif via == 'checkout':
            # using `snow checkout --force`

            # this should error
            r = cli_runner.invoke(["checkout", "HEAD"])
            assert r.exit_code == 1, r

            change_count = db.execute(
                "SELECT COUNT(*) FROM __kxg_map WHERE state != 0"
            ).fetchone()[0]
            assert change_count == (1 + 4 + 5 + 1)

            # do again with --force
            r = cli_runner.invoke(["checkout", "--force", "HEAD"])
            assert r.exit_code == 0, r
        else:
            raise NotImplementedError(f"via={via}")

        change_count = db.execute(
            "SELECT COUNT(*) FROM __kxg_map WHERE state != 0"
        ).fetchone()[0]
        assert change_count == 0

        h_after = _db_table_hash(db, layer, pk_field)
        if h_before != h_after:
            r = db.execute(f"SELECT {pk_field} FROM {layer} WHERE {pk_field}=?;", [rec[pk_field]])
            if r.fetchone():
                print("E: Newly inserted row is still there ({pk_field}={rec[pk_field]})")
            r = db.execute(f"SELECT COUNT(*) FROM {layer} WHERE {pk_field} < ?;", [del_pk])
            if r.fetchone()[0] != 4:
                print("E: Deleted rows {pk_field}<{del_pk} still missing")
            r = db.execute(
                f"SELECT COUNT(*) FROM {layer} WHERE {upd_field} = ?;", [upd_field_value]
            )
            if r.fetchone()[0] != 0:
                print("E: Updated rows not reset")
            r = db.execute(f"SELECT {pk_field} FROM {layer} WHERE {pk_field} = 9998;")
            if r.fetchone():
                print("E: Updated pk row is still there ({pk_field}={id_chg_pk} -> 9998)")
            r = db.execute(f"SELECT {pk_field} FROM {layer} WHERE {pk_field} = ?;", [id_chg_pk])
            if not r.fetchone():
                print("E: Updated pk row is missing ({pk_field}={id_chg_pk})")

        assert h_before == h_after


def test_version(cli_runner):
    r = cli_runner.invoke(["--version"])
    assert r.exit_code == 0, r
    assert re.match(
        r"^Project Snowdrop v(\d.\d.*?)\nGDAL v\d\.\d+\.\d+.*?\nPyGit2 v\d\.\d+\.\d+[^;]*; Libgit2 v\d\.\d+\.\d+.*$",
        r.stdout,
    )


def test_cli_help():
    click_app = cli.cli
    for name, cmd in click_app.commands.items():
        assert cmd.help, f"`{name}` command has no help text"


def test_clone(data_archive, tmp_path, cli_runner, chdir):
    with data_archive("points.snow") as remote_path:
        with chdir(tmp_path):
            r = cli_runner.invoke(["clone", remote_path])

            repo_path = tmp_path / "points.snow"
            assert repo_path.is_dir()

        subprocess.check_call(
            ["git", "-C", str(repo_path), "config", "--local", "--list"]
        )

        repo = pygit2.Repository(str(repo_path))
        assert repo.is_bare
        assert not repo.is_empty
        assert repo.head.name == "refs/heads/master"

        branch = repo.branches.local[repo.head.shorthand]
        assert branch.is_checked_out()
        assert branch.is_head()
        assert branch.upstream_name == "refs/remotes/origin/master"

        assert len(repo.remotes) == 1
        remote = repo.remotes["origin"]
        assert remote.url == str(remote_path)
        assert remote.fetch_refspecs == ["+refs/heads/*:refs/remotes/origin/*"]


def test_geopackage_locking_edit(
    data_working_copy, geopackage, cli_runner, monkeypatch
):
    with data_working_copy("points.snow") as (repo, wc):
        db = geopackage(wc)

        is_checked = False
        orig_func = cli._diff_feature_to_dict

        def _wrap(*args, **kwargs):
            nonlocal is_checked
            if not is_checked:
                with pytest.raises(
                    sqlite3.OperationalError, match=r"database is locked"
                ):
                    db.execute("UPDATE gpkg_context SET table_name=table_name;")
                is_checked = True

            return orig_func(*args, **kwargs)

        monkeypatch.setattr(cli, "_diff_feature_to_dict", _wrap)

        r = cli_runner.invoke(["checkout", "edd5a4b"])
        assert r.exit_code == 0, r
        assert is_checked

        assert _last_change_time(db) == "2019-06-11T11:03:58.000000Z"


def test_fsck(data_working_copy, geopackage, cli_runner):
    with data_working_copy("points.snow") as (repo, wc):
        db = geopackage(wc)

        r = cli_runner.invoke(["fsck"])
        assert r.exit_code == 0, r

        # introduce a feature mismatch
        assert db.execute(f"SELECT COUNT(*) FROM {POINTS_LAYER};").fetchone()[0] == 2143
        assert db.execute(f"SELECT COUNT(*) FROM __kxg_map;").fetchone()[0] == 2143

        with db:
            db.execute(f"UPDATE {POINTS_LAYER} SET name='fred' WHERE fid=1;")
            db.execute("UPDATE __kxg_map SET state=0 WHERE feature_id=1;")

        assert db.execute(f"SELECT COUNT(*) FROM {POINTS_LAYER};").fetchone()[0] == 2143
        assert db.execute(f"SELECT COUNT(*) FROM __kxg_map;").fetchone()[0] == 2143

        r = cli_runner.invoke(["fsck"])
        assert r.exit_code == 1, r

        r = cli_runner.invoke(["fsck", "--reset-layer"])
        assert r.exit_code == 0, r

        assert db.execute(f"SELECT COUNT(*) FROM {POINTS_LAYER};").fetchone()[0] == 2143
        assert db.execute(f"SELECT COUNT(*) FROM __kxg_map;").fetchone()[0] == 2143

        r = cli_runner.invoke(["fsck"])
        assert r.exit_code == 0, r


def test_checkout_branch(data_working_copy, geopackage, cli_runner, tmp_path):
    with data_working_copy("points.snow") as (repo_path, wc):
        db = geopackage(wc)

        # creating a new branch with existing name errors
        r = cli_runner.invoke(["checkout", "-b", "master"])
        assert r.exit_code == 2, r
        assert r.stdout.splitlines()[-1].endswith("A branch named 'master' already exists.")

        subprocess.run(["git", "init", "--bare", tmp_path], check=True)
        r = cli_runner.invoke(["remote", "add", "myremote", tmp_path])
        assert r.exit_code == 0, r

        r = cli_runner.invoke(["push", "--set-upstream", "myremote", "master"])
        assert r.exit_code == 0, r

        # new branch
        r = cli_runner.invoke(["checkout", "-b", "foo"])
        assert r.exit_code == 0, r

        repo = pygit2.Repository(str(repo_path))
        assert repo.head.name == "refs/heads/foo"
        assert 'foo' in repo.branches
        assert repo.head.peel(pygit2.Commit).hex == POINTS_HEAD_SHA

        # make some changes
        db = geopackage(wc)
        with db:
            cur = db.cursor()
            cur.execute(POINTS_INSERT, POINTS_RECORD)
            assert cur.rowcount == 1

        r = cli_runner.invoke(["commit", "-m", "test1"])
        assert r.exit_code == 0, r

        assert repo.head.peel(pygit2.Commit).hex != POINTS_HEAD_SHA

        r = cli_runner.invoke(["checkout", "master"])
        assert r.exit_code == 0, r

        assert repo.head.name == "refs/heads/master"
        assert repo.head.peel(pygit2.Commit).hex == POINTS_HEAD_SHA

        # new branch from remote
        r = cli_runner.invoke(["checkout", "-b", "test99", "myremote/master"])
        assert r.exit_code == 0, r
        assert repo.head.name == "refs/heads/test99"
        assert 'test99' in repo.branches
        assert repo.head.peel(pygit2.Commit).hex == POINTS_HEAD_SHA
        branch = repo.branches['test99']
        assert branch.upstream_name == 'refs/remotes/myremote/master'


@pytest.mark.parametrize("archive", [
    pytest.param('points.snow', id='points'),
    pytest.param('polygons.snow', id='polygons-pk'),
    pytest.param('table.snow', id='table'),
])
def test_merge_fastforward(archive, data_working_copy, geopackage, cli_runner, insert, request):
    with data_working_copy("points.snow") as (repo_path, wc):
        repo = pygit2.Repository(str(repo_path))
        # new branch
        r = cli_runner.invoke(["checkout", "-b", "changes"])
        assert r.exit_code == 0, r
        assert repo.head.name == 'refs/heads/changes'

        h = repo.head.target.hex

        # make some changes
        db = geopackage(wc)
        insert(db)
        insert(db)
        commit_id = insert(db)

        _git_graph(request, "pre-merge")
        assert repo.head.target.hex == commit_id

        r = cli_runner.invoke(["checkout", "master"])
        assert r.exit_code == 0, r
        assert repo.head.target.hex != commit_id

        r = cli_runner.invoke(["merge", "--ff-only", "changes"])
        assert r.exit_code == 0, r

        _git_graph(request, "post-merge")

        assert repo.head.name == "refs/heads/master"
        assert repo.head.target.hex == commit_id
        c = repo.head.peel(pygit2.Commit)
        assert len(c.parents) == 1
        assert c.parents[0].parents[0].parents[0].hex == h


@pytest.mark.parametrize("archive", [
    pytest.param('points.snow', id='points'),
    pytest.param('polygons.snow', id='polygons-pk'),
    pytest.param('table.snow', id='table'),
])
def test_merge_fastforward_noff(archive, data_working_copy, geopackage, cli_runner, insert, request):
    with data_working_copy(archive) as (repo_path, wc):
        repo = pygit2.Repository(str(repo_path))
        # new branch
        r = cli_runner.invoke(["checkout", "-b", "changes"])
        assert r.exit_code == 0, r
        assert repo.head.name == 'refs/heads/changes'

        h = repo.head.target.hex

        # make some changes
        db = geopackage(wc)
        insert(db)
        insert(db)
        commit_id = insert(db)

        _git_graph(request, "pre-merge")
        assert repo.head.target.hex == commit_id

        r = cli_runner.invoke(["checkout", "master"])
        assert r.exit_code == 0, r
        assert repo.head.target.hex != commit_id

        # force creation of a merge commit
        r = cli_runner.invoke(["merge", "--no-ff", "changes"])
        assert r.exit_code == 0, r

        _git_graph(request, "post-merge")

        merge_commit_id = r.stdout.splitlines()[-2].split(": ")[1]

        assert repo.head.name == "refs/heads/master"
        assert repo.head.target.hex == merge_commit_id
        c = repo.head.peel(pygit2.Commit)
        assert len(c.parents) == 2
        assert c.parents[0].hex == h
        assert c.parents[1].hex == commit_id
        assert c.message == "Merge 'changes'"


@pytest.mark.parametrize("archive,layer,pk_field", [
    pytest.param('points.snow', POINTS_LAYER, POINTS_LAYER_PK, id='points'),
    pytest.param('polygons.snow', POLYGONS_LAYER, POLYGONS_LAYER_PK, id='polygons-pk'),
    pytest.param('table.snow', TABLE_LAYER, TABLE_LAYER_PK, id='table'),
])
def test_merge_true(archive, layer, pk_field, data_working_copy, geopackage, cli_runner, insert, request):
    with data_working_copy(archive) as (repo_path, wc):
        repo = pygit2.Repository(str(repo_path))
        # new branch
        r = cli_runner.invoke(["checkout", "-b", "changes"])
        assert r.exit_code == 0, r
        assert repo.head.name == 'refs/heads/changes'

        h = repo.head.target.hex

        # make some changes
        db = geopackage(wc)
        insert(db)
        insert(db)
        b_commit_id = insert(db)
        assert repo.head.target.hex == b_commit_id

        r = cli_runner.invoke(["checkout", "master"])
        assert r.exit_code == 0, r
        assert repo.head.target.hex != b_commit_id
        m_commit_id = insert(db)
        _git_graph(request, "pre-merge-master")

        # fastforward merge should fail
        r = cli_runner.invoke(["merge", "--ff-only", "changes"])
        assert r.exit_code == 1, r
        assert r.stdout.splitlines()[-1] == "Can't resolve as a fast-forward merge and --ff-only specified"

        r = cli_runner.invoke(["merge", "--ff", "changes"])
        assert r.exit_code == 0, r
        _git_graph(request, "post-merge")

        merge_commit_id = r.stdout.splitlines()[-2].split(": ")[1]

        assert repo.head.name == "refs/heads/master"
        assert repo.head.target.hex == merge_commit_id
        c = repo.head.peel(pygit2.Commit)
        assert len(c.parents) == 2
        assert c.parents[0].hex == m_commit_id
        assert c.parents[1].hex == b_commit_id
        assert c.parents[0].parents[0].hex == h
        assert c.message == "Merge 'changes'"

        # check the database state
        num_inserts = len(insert.inserted_fids)
        r = db.execute(f"SELECT COUNT(*) FROM {layer} WHERE {pk_field} IN ({','.join(['?']*num_inserts)});", insert.inserted_fids)
        assert r.fetchone()[0] == num_inserts


def test_fetch(data_archive, data_working_copy, geopackage, cli_runner, insert, tmp_path, request):
    with data_working_copy("points.snow") as (path1, wc):
        subprocess.run(["git", "init", "--bare", tmp_path], check=True)

        r = cli_runner.invoke(["remote", "add", "myremote", tmp_path])
        assert r.exit_code == 0, r

        db = geopackage(wc)
        commit_id = insert(db)

        r = cli_runner.invoke(["push", "--set-upstream", "myremote", "master"])
        assert r.exit_code == 0, r

    with data_working_copy("points.snow") as (path2, wc):
        repo = pygit2.Repository(str(path2))
        h = repo.head.target.hex

        r = cli_runner.invoke(["remote", "add", "myremote", tmp_path])
        assert r.exit_code == 0, r

        r = cli_runner.invoke(["fetch", "myremote"])
        assert r.exit_code == 0, r

        _git_graph(request, "post-fetch")

        assert repo.head.name == "refs/heads/master"
        assert repo.head.target.hex == h

        remote_branch = repo.lookup_reference_dwim("myremote/master")
        assert remote_branch.target.hex == commit_id

        fetch_head = repo.lookup_reference("FETCH_HEAD")
        assert fetch_head.target.hex == commit_id

        # merge
        r = cli_runner.invoke(["merge", "myremote/master"])
        assert r.exit_code == 0, r

        assert repo.head.name == "refs/heads/master"
        assert repo.head.target.hex == commit_id
        commit = repo.head.peel(pygit2.Commit)
        assert len(commit.parents) == 1
        assert commit.parents[0].hex == h


def test_pull(data_archive, data_working_copy, geopackage, cli_runner, insert, tmp_path, request, chdir):
    with data_working_copy("points.snow") as (path1, wc1), data_working_copy("points.snow") as (path2, wc2):
        with chdir(path1):
            subprocess.run(["git", "init", "--bare", tmp_path], check=True)
            r = cli_runner.invoke(["remote", "add", "origin", tmp_path])
            assert r.exit_code == 0, r

            r = cli_runner.invoke(["push", "--set-upstream", "origin", "master"])
            assert r.exit_code == 0, r

        with chdir(path2):
            r = cli_runner.invoke(["remote", "add", "origin", tmp_path])
            assert r.exit_code == 0, r

            r = cli_runner.invoke(["fetch", "origin"])
            assert r.exit_code == 0, r

            r = cli_runner.invoke(["branch", "--set-upstream-to=origin/master"])
            assert r.exit_code == 0, r

        with chdir(path1):
            db = geopackage(wc1)
            commit_id = insert(db)

            r = cli_runner.invoke(["push"])
            assert r.exit_code == 0, r

        with chdir(path2):
            repo = pygit2.Repository(str(path2))
            h = repo.head.target.hex

            r = cli_runner.invoke(["pull"])
            assert r.exit_code == 0, r

            _git_graph(request, "post-pull")

            remote_branch = repo.lookup_reference_dwim("origin/master")
            assert remote_branch.target.hex == commit_id

            assert repo.head.name == "refs/heads/master"
            assert repo.head.target.hex == commit_id
            commit = repo.head.peel(pygit2.Commit)
            assert len(commit.parents) == 1
            assert commit.parents[0].hex == h

            # pull again / no-op
            r = cli_runner.invoke(["branch", "--unset-upstream"])
            assert r.exit_code == 0, r

            r = cli_runner.invoke(["pull"])
            assert r.exit_code == 0, r
            assert repo.head.target.hex == commit_id


def test_status(data_archive, data_working_copy, geopackage, cli_runner, insert, tmp_path, request):
    with data_working_copy("points.snow") as (path1, wc):
        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines() == [
            "On branch master",
            "",
            "Nothing to commit, working copy clean"
        ]

        r = cli_runner.invoke(["checkout", "HEAD~1"])
        assert r.exit_code == 0, r

        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines() == [
            "HEAD detached at edd5a4b",
            "",
            "Nothing to commit, working copy clean"
        ]

        r = cli_runner.invoke(["checkout", "master"])
        assert r.exit_code == 0, r

        subprocess.run(["git", "init", "--bare", tmp_path], check=True)

        r = cli_runner.invoke(["remote", "add", "myremote", tmp_path])
        assert r.exit_code == 0, r

        db = geopackage(wc)
        insert(db)

        r = cli_runner.invoke(["push", "--set-upstream", "myremote", "master"])
        assert r.exit_code == 0, r

        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines() == [
            "On branch master",
            "Your branch is up to date with 'myremote/master'.",
            "",
            "Nothing to commit, working copy clean"
        ]

    with data_working_copy("points.snow") as (path2, wc):
        db = geopackage(wc)

        r = cli_runner.invoke(["remote", "add", "myremote", tmp_path])
        assert r.exit_code == 0, r

        r = cli_runner.invoke(["fetch", "myremote"])
        assert r.exit_code == 0, r

        r = cli_runner.invoke(["branch", "--set-upstream-to=myremote/master"])
        assert r.exit_code == 0, r

        _git_graph(request, "post-fetch")

        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines() == [
            "On branch master",
            "Your branch is behind 'myremote/master' by 1 commit, and can be fast-forwarded.",
            '  (use "snow pull" to update your local branch)',
            "",
            "Nothing to commit, working copy clean"
        ]

        # local commit
        insert(db, reset_index=100)

        _git_graph(request, "post-commit")

        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines() == [
            "On branch master",
            "Your branch and 'myremote/master' have diverged,",
            'and have 1 and 1 different commits each, respectively.',
            '  (use "snow pull" to merge the remote branch into yours)',
            "",
            "Nothing to commit, working copy clean"
        ]

        r = cli_runner.invoke(["merge", "myremote/master"])
        assert r.exit_code == 0, r

        _git_graph(request, "post-merge")

        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines() == [
            "On branch master",
            "Your branch is ahead of 'myremote/master' by 2 commits.",
            '  (use "snow push" to publish your local commits)',
            "",
            "Nothing to commit, working copy clean"
        ]

        # local edits
        with db:
            insert(db, commit=False)
            db.execute(f"DELETE FROM {POINTS_LAYER} WHERE fid <= 2;")
            db.execute(f"UPDATE {POINTS_LAYER} SET name='test0' WHERE fid <= 5;")

        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines() == [
            "On branch master",
            "Your branch is ahead of 'myremote/master' by 2 commits.",
            '  (use "snow push" to publish your local commits)',
            "",
            "Changes in working copy:",
            '  (use "snow commit" to commit)',
            '  (use "snow reset" to discard changes)',
            "",
            "    modified:   3 features",
            "    new:        1 feature",
            "    deleted:    2 features",
        ]


def test_workingcopy_set_path(data_working_copy, cli_runner, tmp_path):
    with data_working_copy("points.snow") as (repo_path, wc):
        repo = pygit2.Repository(str(repo_path))

        r = cli_runner.invoke(["workingcopy-set-path", "/thingz.gpkg"])
        assert r.exit_code == 2, r

        # relative path 1
        new_path = Path("new-thingz.gpkg")
        wc.rename(new_path)
        r = cli_runner.invoke(["workingcopy-set-path", new_path])
        assert r.exit_code == 0, r
        wc = new_path

        assert repo.config['kx.workingcopy'] == f"GPKG:{new_path}:{POINTS_LAYER}"

        # relative path 2
        new_path = Path("other-thingz.gpkg")
        wc.rename(new_path)
        r = cli_runner.invoke(["workingcopy-set-path", Path("../points.snow") / new_path])
        assert r.exit_code == 0, r
        wc = new_path

        assert repo.config['kx.workingcopy'] == f"GPKG:{new_path}:{POINTS_LAYER}"

        # abs path
        new_path = tmp_path / "thingz.gpkg"
        wc.rename(new_path)
        r = cli_runner.invoke(["workingcopy-set-path", new_path])
        assert r.exit_code == 0, r

        assert repo.config['kx.workingcopy'] == f"GPKG:{new_path}:{POINTS_LAYER}"
