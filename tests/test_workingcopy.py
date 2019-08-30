import re
import sqlite3
import subprocess
from pathlib import Path

import pytest

import pygit2

import snowdrop.checkout


H = pytest.helpers.helpers()


@pytest.mark.parametrize(
    "archive,table,commit_sha",
    [
        pytest.param("points.snow", H.POINTS_LAYER, H.POINTS_HEAD_SHA, id="points"),
        pytest.param(
            "polygons.snow", H.POLYGONS_LAYER, H.POLYGONS_HEAD_SHA, id="polygons-pk"
        ),
        pytest.param("table.snow", H.TABLE_LAYER, H.TABLE_HEAD_SHA, id="table"),
    ],
)
def test_checkout_workingcopy(
    archive, table, commit_sha, data_archive, tmp_path, cli_runner, geopackage
):
    """ Checkout a working copy to edit """
    with data_archive(archive) as repo_path:
        H.clear_working_copy()

        wc = tmp_path / f"{table}.gpkg"
        r = cli_runner.invoke(["checkout", f"--layer={table}", f"--working-copy={wc}"])
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

        assert repo.head.name == "refs/heads/master"
        assert repo.head.shorthand == "master"

        wc_tree_id = db.execute(
            "SELECT value FROM __kxg_meta WHERE table_name=? AND key='tree';", [table]
        ).fetchone()[0]
        assert wc_tree_id == repo.head.peel(pygit2.Tree).hex


def test_checkout_detached(data_working_copy, cli_runner, geopackage):
    """ Checkout a working copy to edit """
    with data_working_copy("points.snow") as (repo_dir, wc):
        db = geopackage(wc)
        assert H.last_change_time(db) == "2019-06-20T14:28:33.000000Z"

        # checkout the previous commit
        r = cli_runner.invoke(["checkout", "edd5a4b02a7d2ce608f1839eea5e3a8ddb874e00"])
        assert r.exit_code == 0, r
        assert H.last_change_time(db) == "2019-06-11T11:03:58.000000Z"

        repo = pygit2.Repository(str(repo_dir))
        assert repo.head.name == "HEAD"
        assert repo.head_is_detached
        assert repo.head.target.hex == "edd5a4b02a7d2ce608f1839eea5e3a8ddb874e00"


def test_checkout_references(data_working_copy, cli_runner, geopackage, tmp_path):
    with data_working_copy("points.snow") as (repo_dir, wc):
        db = geopackage(wc)
        repo = pygit2.Repository(str(repo_dir))

        # create a tag
        repo.create_reference("refs/tags/version1", repo.head.target)

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
        assert H.last_change_time(db) == "2019-06-20T14:28:33.000000Z"
        assert not repo.head_is_detached
        assert r_head() == ("refs/heads/master", H.POINTS_HEAD_SHA)

        # checkout the HEAD-but-1 commit
        r = cli_runner.invoke(["checkout", "HEAD~1"])
        assert r.exit_code == 0, r
        assert H.last_change_time(db) == "2019-06-11T11:03:58.000000Z"
        assert repo.head_is_detached
        assert r_head() == ("HEAD", "edd5a4b02a7d2ce608f1839eea5e3a8ddb874e00")

        # checkout the master HEAD via branch-name
        r = cli_runner.invoke(["checkout", "master"])
        assert r.exit_code == 0, r
        assert H.last_change_time(db) == "2019-06-20T14:28:33.000000Z"
        assert not repo.head_is_detached
        assert r_head() == ("refs/heads/master", H.POINTS_HEAD_SHA)

        # checkout a short-sha commit
        r = cli_runner.invoke(["checkout", "edd5a4b"])
        assert r.exit_code == 0, r
        assert H.last_change_time(db) == "2019-06-11T11:03:58.000000Z"
        assert repo.head_is_detached
        assert r_head() == ("HEAD", "edd5a4b02a7d2ce608f1839eea5e3a8ddb874e00")

        # checkout the master HEAD via refspec
        r = cli_runner.invoke(["checkout", "refs/heads/master"])
        assert r.exit_code == 0, r
        assert H.last_change_time(db) == "2019-06-20T14:28:33.000000Z"
        assert not repo.head_is_detached
        assert r_head() == ("refs/heads/master", H.POINTS_HEAD_SHA)

        # checkout the tag
        r = cli_runner.invoke(["checkout", "version1"])
        assert r.exit_code == 0, r
        assert H.last_change_time(db) == "2019-06-20T14:28:33.000000Z"
        assert repo.head_is_detached
        assert r_head() == ("HEAD", H.POINTS_HEAD_SHA)

        # checkout the remote branch
        r = cli_runner.invoke(["checkout", "myremote/master"])
        assert r.exit_code == 0, r
        assert H.last_change_time(db) == "2019-06-20T14:28:33.000000Z"
        assert repo.head_is_detached
        assert r_head() == ("HEAD", H.POINTS_HEAD_SHA)


def test_checkout_branch(data_working_copy, geopackage, cli_runner, tmp_path):
    with data_working_copy("points.snow") as (repo_path, wc):
        db = geopackage(wc)

        # creating a new branch with existing name errors
        r = cli_runner.invoke(["checkout", "-b", "master"])
        assert r.exit_code == 2, r
        assert r.stdout.splitlines()[-1].endswith(
            "A branch named 'master' already exists."
        )

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
        assert "foo" in repo.branches
        assert repo.head.peel(pygit2.Commit).hex == H.POINTS_HEAD_SHA

        # make some changes
        db = geopackage(wc)
        with db:
            cur = db.cursor()
            cur.execute(H.POINTS_INSERT, H.POINTS_RECORD)
            assert cur.rowcount == 1

        r = cli_runner.invoke(["commit", "-m", "test1"])
        assert r.exit_code == 0, r

        assert repo.head.peel(pygit2.Commit).hex != H.POINTS_HEAD_SHA

        r = cli_runner.invoke(["checkout", "master"])
        assert r.exit_code == 0, r

        assert repo.head.name == "refs/heads/master"
        assert repo.head.peel(pygit2.Commit).hex == H.POINTS_HEAD_SHA

        # new branch from remote
        r = cli_runner.invoke(["checkout", "-b", "test99", "myremote/master"])
        assert r.exit_code == 0, r
        assert repo.head.name == "refs/heads/test99"
        assert "test99" in repo.branches
        assert repo.head.peel(pygit2.Commit).hex == H.POINTS_HEAD_SHA
        branch = repo.branches["test99"]
        assert branch.upstream_name == "refs/remotes/myremote/master"


@pytest.mark.parametrize(
    "archive,layer",
    [
        pytest.param("points.snow", H.POINTS_LAYER, id="points"),
        pytest.param("polygons.snow", H.POLYGONS_LAYER, id="polygons-pk"),
        pytest.param("table.snow", H.TABLE_LAYER, id="table"),
    ],
)
@pytest.mark.parametrize(
    "via",
    [
        pytest.param("reset", id="via-reset"),
        pytest.param("checkout", id="via-checkout"),
    ],
)
def test_working_copy_reset(
    archive, layer, via, data_working_copy, cli_runner, geopackage
):
    """
    Check that we reset any working-copy changes correctly before doing any new checkout

    We can do this via `snow reset` or `snow checkout --force HEAD`
    """
    if layer == H.POINTS_LAYER:
        pk_field = H.POINTS_LAYER_PK
        rec = H.POINTS_RECORD
        sql = H.POINTS_INSERT
        del_pk = 5
        upd_field = "t50_fid"
        upd_field_value = 888_888
        upd_pk_range = (10, 15)
        id_chg_pk = 20
    elif layer == H.POLYGONS_LAYER:
        pk_field = H.POLYGONS_LAYER_PK
        rec = H.POLYGONS_RECORD
        sql = H.POLYGONS_INSERT
        del_pk = 1_456_912
        upd_field = "survey_reference"
        upd_field_value = "test"
        upd_pk_range = (1_459_750, 1_460_312)
        id_chg_pk = 1_460_583
    elif layer == H.TABLE_LAYER:
        pk_field = H.TABLE_LAYER_PK
        rec = H.TABLE_RECORD
        sql = H.TABLE_INSERT
        del_pk = 5
        upd_field = "name"
        upd_field_value = "test"
        upd_pk_range = (10, 15)
        id_chg_pk = 20
    else:
        raise NotImplementedError(f"layer={layer}")

    with data_working_copy(archive, force_new=True) as (repo_dir, wc):
        db = geopackage(wc)

        h_before = H.db_table_hash(db, layer, pk_field)
        with db:
            cur = db.cursor()
            try:
                cur.execute(sql, rec)
            except sqlite3.OperationalError:
                print(sql, rec)
                raise
            assert cur.rowcount == 1

            cur.execute(f"DELETE FROM {layer} WHERE {pk_field} < {del_pk};")
            assert cur.rowcount == 4
            cur.execute(
                f"UPDATE {layer} SET {upd_field} = ? WHERE {pk_field}>=? AND {pk_field}<?;",
                [upd_field_value, upd_pk_range[0], upd_pk_range[1]],
            )
            assert cur.rowcount == 5
            cur.execute(
                f"UPDATE {layer} SET {pk_field}=? WHERE {pk_field}=?;",
                [9998, id_chg_pk],
            )
            assert cur.rowcount == 1

            change_count = db.execute(
                "SELECT COUNT(*) FROM __kxg_map WHERE state != 0"
            ).fetchone()[0]
            assert change_count == (1 + 4 + 5 + 1)

        if via == "reset":
            # using `snow reset`
            r = cli_runner.invoke(["reset"])
            assert r.exit_code == 0, r
        elif via == "checkout":
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

        h_after = H.db_table_hash(db, layer, pk_field)
        if h_before != h_after:
            r = db.execute(
                f"SELECT {pk_field} FROM {layer} WHERE {pk_field}=?;", [rec[pk_field]]
            )
            if r.fetchone():
                print(
                    "E: Newly inserted row is still there ({pk_field}={rec[pk_field]})"
                )
            r = db.execute(
                f"SELECT COUNT(*) FROM {layer} WHERE {pk_field} < ?;", [del_pk]
            )
            if r.fetchone()[0] != 4:
                print("E: Deleted rows {pk_field}<{del_pk} still missing")
            r = db.execute(
                f"SELECT COUNT(*) FROM {layer} WHERE {upd_field} = ?;",
                [upd_field_value],
            )
            if r.fetchone()[0] != 0:
                print("E: Updated rows not reset")
            r = db.execute(f"SELECT {pk_field} FROM {layer} WHERE {pk_field} = 9998;")
            if r.fetchone():
                print(
                    "E: Updated pk row is still there ({pk_field}={id_chg_pk} -> 9998)"
                )
            r = db.execute(
                f"SELECT {pk_field} FROM {layer} WHERE {pk_field} = ?;", [id_chg_pk]
            )
            if not r.fetchone():
                print("E: Updated pk row is missing ({pk_field}={id_chg_pk})")

        assert h_before == h_after


def test_geopackage_locking_edit(
    data_working_copy, geopackage, cli_runner, monkeypatch
):
    with data_working_copy("points.snow") as (repo, wc):
        db = geopackage(wc)

        is_checked = False
        orig_func = snowdrop.checkout.diff_feature_to_dict

        def _wrap(*args, **kwargs):
            nonlocal is_checked
            if not is_checked:
                with pytest.raises(
                    sqlite3.OperationalError, match=r"database is locked"
                ):
                    db.execute("UPDATE gpkg_context SET table_name=table_name;")
                is_checked = True

            return orig_func(*args, **kwargs)

        monkeypatch.setattr(snowdrop.checkout, "diff_feature_to_dict", _wrap)

        r = cli_runner.invoke(["checkout", "edd5a4b"])
        assert r.exit_code == 0, r
        assert is_checked

        assert H.last_change_time(db) == "2019-06-11T11:03:58.000000Z"


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

        assert repo.config["kx.workingcopy"] == f"GPKG:{new_path}:{H.POINTS_LAYER}"

        # relative path 2
        new_path = Path("other-thingz.gpkg")
        wc.rename(new_path)
        r = cli_runner.invoke(
            ["workingcopy-set-path", Path("../points.snow") / new_path]
        )
        assert r.exit_code == 0, r
        wc = new_path

        assert repo.config["kx.workingcopy"] == f"GPKG:{new_path}:{H.POINTS_LAYER}"

        # abs path
        new_path = tmp_path / "thingz.gpkg"
        wc.rename(new_path)
        r = cli_runner.invoke(["workingcopy-set-path", new_path])
        assert r.exit_code == 0, r

        assert repo.config["kx.workingcopy"] == f"GPKG:{new_path}:{H.POINTS_LAYER}"
