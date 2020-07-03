import subprocess
from pathlib import Path

import pytest

import apsw
import pygit2

import sno.checkout
from sno.working_copy import WorkingCopy
from sno.exceptions import INVALID_ARGUMENT, INVALID_OPERATION


H = pytest.helpers.helpers()


@pytest.mark.parametrize(
    "archive,table,commit_sha",
    [
        pytest.param("points", H.POINTS.LAYER, H.POINTS.HEAD_SHA, id="points"),
        pytest.param(
            "polygons", H.POLYGONS.LAYER, H.POLYGONS.HEAD_SHA, id="polygons-pk"
        ),
        pytest.param("table", H.TABLE.LAYER, H.TABLE.HEAD_SHA, id="table"),
    ],
)
@pytest.mark.parametrize("version", ["1.0", "2.0"])
def test_checkout_workingcopy(
    version, archive, table, commit_sha, data_archive, tmp_path, cli_runner, geopackage
):
    """ Checkout a working copy to edit """
    if version == "2.0":
        archive += "2"

    with data_archive(archive) as repo_path:
        H.clear_working_copy()

        wc = tmp_path / f"{table}.gpkg"
        r = cli_runner.invoke(["checkout", f"--path={wc}"])
        assert r.exit_code == 0, r
        lines = r.stdout.splitlines()
        # TODO: restore these
        # assert re.match(fr"Checkout HEAD to .+ as GPKG \.\.\.$", lines[0])
        # assert re.match(fr"Commit: {commit_sha}$", lines[1])

        assert wc.exists()
        db = geopackage(wc)
        assert H.row_count(db, table) > 0

        repo = pygit2.Repository(str(repo_path))
        assert repo.is_bare

        assert repo.head.name == "refs/heads/master"
        assert repo.head.shorthand == "master"

        head_tree = repo.head.peel(pygit2.Tree)
        meta_table = "gpkg.sno.meta" if version == "2.0" else ".sno-meta"

        wc_tree_id = (
            db.cursor()
            .execute(
                f"""SELECT value FROM "{meta_table}" WHERE table_name='*' AND key='tree';"""
            )
            .fetchone()[0]
        )
        assert wc_tree_id == head_tree.hex

        wc = WorkingCopy.open(repo)
        assert wc.assert_db_tree_match(head_tree)


def test_checkout_detached(data_working_copy, cli_runner, geopackage):
    """ Checkout a working copy to edit """
    with data_working_copy("points") as (repo_dir, wc):
        db = geopackage(wc)
        assert H.last_change_time(db) == "2019-06-20T14:28:33.000000Z"

        # checkout the previous commit
        r = cli_runner.invoke(["checkout", H.POINTS.HEAD1_SHA[:8]])
        assert r.exit_code == 0, r
        assert H.last_change_time(db) == "2019-06-11T11:03:58.000000Z"

        repo = pygit2.Repository(str(repo_dir))
        assert repo.head.target.hex == H.POINTS.HEAD1_SHA
        assert repo.head_is_detached
        assert repo.head.name == "HEAD"


def test_checkout_references(data_working_copy, cli_runner, geopackage, tmp_path):
    with data_working_copy("points") as (repo_dir, wc):
        db = geopackage(wc)
        repo = pygit2.Repository(str(repo_dir))

        # create a tag
        repo.create_reference("refs/tags/version1", repo.head.target)

        subprocess.run(["git", "init", "--bare", str(tmp_path)], check=True)

        r = cli_runner.invoke(["remote", "add", "myremote", tmp_path])
        assert r.exit_code == 0, r

        r = cli_runner.invoke(["push", "myremote", "master"])
        assert r.exit_code == 0, r

        def r_head():
            return (repo.head.name, repo.head.target.hex)

        # checkout the HEAD commit
        r = cli_runner.invoke(["checkout", "HEAD"])
        assert r.exit_code == 0, r
        assert r_head() == ("refs/heads/master", H.POINTS.HEAD_SHA)
        assert not repo.head_is_detached
        assert H.last_change_time(db) == "2019-06-20T14:28:33.000000Z"

        # checkout the HEAD-but-1 commit
        r = cli_runner.invoke(["checkout", "HEAD~1"])
        assert r.exit_code == 0, r
        assert r_head() == ("HEAD", H.POINTS.HEAD1_SHA)
        assert repo.head_is_detached
        assert H.last_change_time(db) == "2019-06-11T11:03:58.000000Z"

        # checkout the master HEAD via branch-name
        r = cli_runner.invoke(["checkout", "master"])
        assert r.exit_code == 0, r
        assert r_head() == ("refs/heads/master", H.POINTS.HEAD_SHA)
        assert not repo.head_is_detached
        assert H.last_change_time(db) == "2019-06-20T14:28:33.000000Z"

        # checkout a short-sha commit
        r = cli_runner.invoke(["checkout", H.POINTS.HEAD1_SHA[:8]])
        assert r.exit_code == 0, r
        assert r_head() == ("HEAD", H.POINTS.HEAD1_SHA)
        assert repo.head_is_detached
        assert H.last_change_time(db) == "2019-06-11T11:03:58.000000Z"

        # checkout the master HEAD via refspec
        r = cli_runner.invoke(["checkout", "refs/heads/master"])
        assert r.exit_code == 0, r
        assert r_head() == ("refs/heads/master", H.POINTS.HEAD_SHA)
        assert not repo.head_is_detached
        assert H.last_change_time(db) == "2019-06-20T14:28:33.000000Z"

        # checkout the tag
        r = cli_runner.invoke(["checkout", "version1"])
        assert r.exit_code == 0, r
        assert r_head() == ("HEAD", H.POINTS.HEAD_SHA)
        assert repo.head_is_detached
        assert H.last_change_time(db) == "2019-06-20T14:28:33.000000Z"

        # checkout the remote branch
        r = cli_runner.invoke(["checkout", "myremote/master"])
        assert r.exit_code == 0, r
        assert r_head() == ("HEAD", H.POINTS.HEAD_SHA)
        assert repo.head_is_detached
        assert H.last_change_time(db) == "2019-06-20T14:28:33.000000Z"


def test_checkout_branch(data_working_copy, geopackage, cli_runner, tmp_path):
    with data_working_copy("points") as (repo_path, wc):
        db = geopackage(wc)

        # creating a new branch with existing name errors
        r = cli_runner.invoke(["checkout", "-b", "master"])
        assert r.exit_code == INVALID_ARGUMENT, r
        assert r.stderr.splitlines()[-1].endswith(
            "A branch named 'master' already exists."
        )

        subprocess.run(["git", "init", "--bare", str(tmp_path)], check=True)
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
        assert repo.head.peel(pygit2.Commit).hex == H.POINTS.HEAD_SHA

        # make some changes
        db = geopackage(wc)
        with db:
            cur = db.cursor()
            cur.execute(H.POINTS.INSERT, H.POINTS.RECORD)
            assert db.changes() == 1

        r = cli_runner.invoke(["commit", "-m", "test1"])
        assert r.exit_code == 0, r

        assert repo.head.peel(pygit2.Commit).hex != H.POINTS.HEAD_SHA

        r = cli_runner.invoke(["checkout", "master"])
        assert r.exit_code == 0, r

        assert repo.head.name == "refs/heads/master"
        assert repo.head.peel(pygit2.Commit).hex == H.POINTS.HEAD_SHA

        # new branch from remote
        r = cli_runner.invoke(["checkout", "-b", "test99", "myremote/master"])
        assert r.exit_code == 0, r
        assert repo.head.name == "refs/heads/test99"
        assert "test99" in repo.branches
        assert repo.head.peel(pygit2.Commit).hex == H.POINTS.HEAD_SHA
        branch = repo.branches["test99"]
        assert branch.upstream_name == "refs/remotes/myremote/master"


def test_switch_branch(data_working_copy, geopackage, cli_runner, tmp_path):
    raise pytest.skip()  # apsw.SQLError: SQLError: Safety level may not be changed inside a transaction
    with data_working_copy("points") as (repo_path, wc):
        db = geopackage(wc)

        # creating a new branch with existing name errors
        r = cli_runner.invoke(["switch", "-c", "master"])
        assert r.exit_code == 2, r
        assert r.stdout.splitlines()[-1].endswith(
            "A branch named 'master' already exists."
        )

        subprocess.run(["git", "init", "--bare", str(tmp_path)], check=True)
        r = cli_runner.invoke(["remote", "add", "myremote", tmp_path])
        assert r.exit_code == 0, r

        r = cli_runner.invoke(["push", "--set-upstream", "myremote", "master"])
        assert r.exit_code == 0, r

        # new branch
        r = cli_runner.invoke(["switch", "-c", "foo"])
        assert r.exit_code == 0, r

        repo = pygit2.Repository(str(repo_path))
        assert repo.head.name == "refs/heads/foo"
        assert "foo" in repo.branches
        assert repo.head.peel(pygit2.Commit).hex == H.POINTS.HEAD_SHA

        # make some changes
        db = geopackage(wc)
        with db:
            cur = db.cursor()

            cur.execute(H.POINTS.INSERT, H.POINTS.RECORD)
            assert db.changes() == 1

            cur.execute(f"UPDATE {H.POINTS.LAYER} SET fid=30000 WHERE fid=3;")
            assert db.changes() == 1

        r = cli_runner.invoke(["commit", "-m", "test1"])
        assert r.exit_code == 0, r

        new_commit = repo.head.peel(pygit2.Commit).hex
        assert new_commit != H.POINTS.HEAD_SHA

        r = cli_runner.invoke(["switch", "master"])
        assert r.exit_code == 0, r

        assert H.row_count(db, H.POINTS.LAYER) == H.POINTS.ROWCOUNT

        assert repo.head.name == "refs/heads/master"
        assert repo.head.peel(pygit2.Commit).hex == H.POINTS.HEAD_SHA

        # make some changes
        with db:
            cur = db.cursor()

            cur.execute(H.POINTS.INSERT, H.POINTS.RECORD)
            assert db.changes() == 1

            cur.execute(f"UPDATE {H.POINTS.LAYER} SET fid=40000 WHERE fid=4;")
            assert db.changes() == 1

        r = cli_runner.invoke(["switch", "foo"])
        assert r.exit_code == INVALID_OPERATION, r
        assert "Error: You have uncommitted changes in your working copy." in r.stdout

        r = cli_runner.invoke(["switch", "foo", "--discard-changes"])
        assert r.exit_code == 0, r

        assert H.row_count(db, H.POINTS.LAYER) == H.POINTS.ROWCOUNT + 1

        assert repo.head.name == "refs/heads/foo"
        assert repo.head.peel(pygit2.Commit).hex == new_commit

        # new branch from remote
        r = cli_runner.invoke(["switch", "-c", "test99", "myremote/master"])
        assert r.exit_code == 0, r
        assert repo.head.name == "refs/heads/test99"
        assert "test99" in repo.branches
        assert repo.head.peel(pygit2.Commit).hex == H.POINTS.HEAD_SHA
        branch = repo.branches["test99"]
        assert branch.upstream_name == "refs/remotes/myremote/master"

        assert H.row_count(db, H.POINTS.LAYER) == H.POINTS.ROWCOUNT


@pytest.mark.parametrize(
    "archive,layer",
    [
        pytest.param("points", H.POINTS.LAYER, id="points"),
        pytest.param("polygons", H.POLYGONS.LAYER, id="polygons-pk"),
        pytest.param("table", H.TABLE.LAYER, id="table"),
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

    We can do this via `sno reset` or `sno checkout --force HEAD`
    """
    raise pytest.skip()  # apsw.SQLError: SQLError: Safety level may not be changed inside a transaction
    if layer == H.POINTS.LAYER:
        pk_field = H.POINTS.LAYER_PK
        rec = H.POINTS.RECORD
        sql = H.POINTS.INSERT
        del_pk = 5
        upd_field = "t50_fid"
        upd_field_value = 888_888
        upd_pk_range = (10, 15)
        id_chg_pk = 20
    elif layer == H.POLYGONS.LAYER:
        pk_field = H.POLYGONS.LAYER_PK
        rec = H.POLYGONS_RECORD
        sql = H.POLYGONS_INSERT
        del_pk = 1_456_912
        upd_field = "survey_reference"
        upd_field_value = "test"
        upd_pk_range = (1_459_750, 1_460_312)
        id_chg_pk = 1_460_583
    elif layer == H.TABLE.LAYER:
        pk_field = H.TABLE.LAYER_PK
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
            except apsw.Error:
                print(sql, rec)
                raise
            assert db.changes() == 1

            cur.execute(f"DELETE FROM {layer} WHERE {pk_field} < {del_pk};")
            assert db.changes() == 4
            cur.execute(
                f"UPDATE {layer} SET {upd_field} = ? WHERE {pk_field}>=? AND {pk_field}<?;",
                [upd_field_value, upd_pk_range[0], upd_pk_range[1]],
            )
            assert db.changes() == 5
            cur.execute(
                f"UPDATE {layer} SET {pk_field}=? WHERE {pk_field}=?;",
                [9998, id_chg_pk],
            )
            assert db.changes() == 1

            cur.execute("""SELECT COUNT(*) FROM ".sno-track";""")
            change_count = cur.fetchone()[0]
            assert change_count == (1 + 4 + 5 + 2)

        if via == "reset":
            # using `sno reset`
            r = cli_runner.invoke(["reset"])
            assert r.exit_code == 0, r
        elif via == "checkout":
            # using `sno checkout --force`

            # this should error
            r = cli_runner.invoke(["checkout", "HEAD"])
            assert r.exit_code == INVALID_OPERATION, r

            cur.execute("""SELECT COUNT(*) FROM ".sno-track";""")
            change_count = cur.fetchone()[0]
            assert change_count == (1 + 4 + 5 + 2)

            # do again with --force
            r = cli_runner.invoke(["checkout", "--force", "HEAD"])
            assert r.exit_code == 0, r
        else:
            raise NotImplementedError(f"via={via}")

        cur.execute("""SELECT COUNT(*) FROM ".sno-track";""")
        change_count = cur.fetchone()[0]
        assert change_count == 0

        h_after = H.db_table_hash(db, layer, pk_field)
        if h_before != h_after:
            cur.execute(
                f"SELECT {pk_field} FROM {layer} WHERE {pk_field}=?;", [rec[pk_field]]
            )
            if cur.fetchone():
                print(
                    "E: Newly inserted row is still there ({pk_field}={rec[pk_field]})"
                )
            cur.execute(f"SELECT COUNT(*) FROM {layer} WHERE {pk_field} < ?;", [del_pk])
            if cur.fetchone()[0] != 4:
                print("E: Deleted rows {pk_field}<{del_pk} still missing")
            cur.execute(
                f"SELECT COUNT(*) FROM {layer} WHERE {upd_field} = ?;",
                [upd_field_value],
            )
            if cur.fetchone()[0] != 0:
                print("E: Updated rows not reset")
            cur.execute(f"SELECT {pk_field} FROM {layer} WHERE {pk_field} = 9998;")
            if cur.fetchone():
                print(
                    "E: Updated pk row is still there ({pk_field}={id_chg_pk} -> 9998)"
                )
            cur.execute(
                f"SELECT {pk_field} FROM {layer} WHERE {pk_field} = ?;", [id_chg_pk]
            )
            if not cur.fetchone():
                print("E: Updated pk row is missing ({pk_field}={id_chg_pk})")

        assert h_before == h_after


def test_geopackage_locking_edit(
    data_working_copy, geopackage, cli_runner, monkeypatch
):
    with data_working_copy("points") as (repo, wc):
        db = geopackage(wc)

        is_checked = False
        orig_func = sno.working_copy.WorkingCopy_GPKG_1.write_features

        def _wrap(*args, **kwargs):
            nonlocal is_checked
            if not is_checked:
                with pytest.raises(apsw.BusyError, match=r"database is locked"):
                    db.cursor().execute(
                        "UPDATE gpkg_contents SET table_name=table_name;"
                    )
                is_checked = True

            return orig_func(*args, **kwargs)

        monkeypatch.setattr(
            sno.working_copy.WorkingCopy_GPKG_1, "write_features", _wrap
        )

        r = cli_runner.invoke(["checkout", H.POINTS.HEAD1_SHA])
        assert r.exit_code == 0, r
        assert is_checked

        assert H.last_change_time(db) == "2019-06-11T11:03:58.000000Z"


def test_workingcopy_set_path(data_working_copy, cli_runner, tmp_path):
    with data_working_copy("points") as (repo_path, wc):
        repo = pygit2.Repository(str(repo_path))

        r = cli_runner.invoke(["workingcopy-set-path", "/thingz.gpkg"])
        assert r.exit_code == INVALID_ARGUMENT, r

        # relative path 1
        new_path = Path("new-thingz.gpkg")
        wc.rename(new_path)
        assert new_path.exists()
        r = cli_runner.invoke(["workingcopy-set-path", new_path])
        assert r.exit_code == 0, r
        wc = new_path

        assert repo.config["sno.workingcopy.path"] == str(new_path)

        # relative path 2
        new_path = Path("other-thingz.gpkg")
        wc.rename(new_path)
        assert new_path.exists()
        r = cli_runner.invoke(["workingcopy-set-path", Path("../points") / new_path])
        assert r.exit_code == 0, r
        wc = new_path

        assert repo.config["sno.workingcopy.path"] == str(new_path)

        # abs path
        new_path = tmp_path / "thingz.gpkg"
        wc.rename(new_path)
        assert new_path.exists()
        r = cli_runner.invoke(["workingcopy-set-path", new_path])
        assert r.exit_code == 0, r

        assert repo.config["sno.workingcopy.path"] == str(new_path)


@pytest.mark.parametrize(
    "source",
    [pytest.param([], id="head"), pytest.param(["-s", H.POINTS.HEAD_SHA], id="prev"),],
)
@pytest.mark.parametrize(
    "pathspec", [pytest.param([], id="all"), pytest.param(["bob"], id="exclude"),],
)
def test_restore(source, pathspec, data_working_copy, cli_runner, geopackage):
    with data_working_copy("points", force_new=True) as (repo_dir, wc):
        layer = H.POINTS.LAYER
        pk_field = H.POINTS.LAYER_PK
        rec = H.POINTS.RECORD
        sql = H.POINTS.INSERT
        del_pk = 5
        upd_field = "t50_fid"
        upd_field_value = 888_888
        upd_pk_range = (10, 15)
        id_chg_pk = 20

        db = geopackage(wc)
        repo = pygit2.Repository(str(repo_dir))

        # make some changes
        with db:
            cur = db.cursor()
            cur.execute(f"UPDATE {H.POINTS.LAYER} SET fid=30000 WHERE fid=300;")
            assert db.changes() == 1

        r = cli_runner.invoke(["commit", "-m", "test1"])
        assert r.exit_code == 0, r

        new_commit = repo.head.peel(pygit2.Commit).hex
        assert new_commit != H.POINTS.HEAD_SHA
        print(f"Original commit={H.POINTS.HEAD_SHA} New commit={new_commit}")

        with db:
            cur = db.cursor()
            try:
                cur.execute(sql, rec)
            except apsw.Error:
                print(sql, rec)
                raise
            assert db.changes() == 1

            cur.execute(f"DELETE FROM {layer} WHERE {pk_field} < {del_pk};")
            assert db.changes() == 4
            cur.execute(
                f"UPDATE {layer} SET {upd_field} = ? WHERE {pk_field}>=? AND {pk_field}<?;",
                [upd_field_value, upd_pk_range[0], upd_pk_range[1]],
            )
            assert db.changes() == 5
            cur.execute(
                f"UPDATE {layer} SET {pk_field}=? WHERE {pk_field}=?;",
                [9998, id_chg_pk],
            )
            assert db.changes() == 1

            changes_pre = [
                r[0]
                for r in cur.execute(
                    'SELECT pk FROM ".sno-track" ORDER BY CAST(pk AS INTEGER);'
                )
            ]
            # .sno-track stores pk as strings
            assert changes_pre == [
                "1",
                "2",
                "3",
                "4",
                "10",
                "11",
                "12",
                "13",
                "14",
                "20",
                "9998",
                "9999",
            ]

        # using `sno restore
        r = cli_runner.invoke(["restore"] + source + pathspec)
        assert r.exit_code == 0, r

        changes_post = [
            r[0]
            for r in cur.execute(
                'SELECT pk FROM ".sno-track" ORDER BY CAST(pk AS INTEGER);'
            )
        ]

        cur.execute(
            f"""SELECT value FROM ".sno-meta" WHERE key = 'tree' AND table_name='*';"""
        )
        head_sha = cur.fetchone()[0]

        if pathspec:
            # we restore'd paths other than our test dataset, so all the changes should still be there
            assert changes_post == changes_pre

            if head_sha != new_commit:
                print(f"E: Bad Tree? {head_sha}")

            return

        if source:
            assert changes_post == ["300", "30000"]

            if head_sha != H.POINTS.HEAD_SHA:
                print(f"E: Bad Tree? {head_sha}")

            cur.execute(f"SELECT {pk_field} FROM {layer} WHERE {pk_field} = 300;")
            if not cur.fetchone():
                print("E: Previous PK bad? ({pk_field}=300)")
            return

        assert changes_post == []

        if head_sha != new_commit:
            print(f"E: Bad Tree? {head_sha}")

        cur.execute(
            f"""SELECT value FROM ".sno-meta" WHERE key = 'tree' AND table_name='*';"""
        )
        head_sha = cur.fetchone()[0]
        if head_sha != new_commit:
            print(f"E: Bad Tree? {head_sha}")

        cur.execute(
            f"SELECT {pk_field} FROM {layer} WHERE {pk_field}=?;", [rec[pk_field]]
        )
        if cur.fetchone():
            print("E: Newly inserted row is still there ({pk_field}={rec[pk_field]})")
        cur.execute(f"SELECT COUNT(*) FROM {layer} WHERE {pk_field} < ?;", [del_pk])
        if cur.fetchone()[0] != 4:
            print("E: Deleted rows {pk_field}<{del_pk} still missing")
        cur.execute(
            f"SELECT COUNT(*) FROM {layer} WHERE {upd_field} = ?;", [upd_field_value],
        )
        if cur.fetchone()[0] != 0:
            print("E: Updated rows not reset")
        cur.execute(f"SELECT {pk_field} FROM {layer} WHERE {pk_field} = 9998;")
        if cur.fetchone():
            print("E: Updated pk row is still there ({pk_field}={id_chg_pk} -> 9998)")
        cur.execute(
            f"SELECT {pk_field} FROM {layer} WHERE {pk_field} = ?;", [id_chg_pk]
        )
        if not cur.fetchone():
            print("E: Updated pk row is missing ({pk_field}={id_chg_pk})")

        cur.execute(f"SELECT {pk_field} FROM {layer} WHERE {pk_field} = 300;")
        if not cur.fetchone():
            print("E: Previous PK bad? ({pk_field}=300)")


def test_delete_branch(data_working_copy, cli_runner):
    with data_working_copy("points") as (repo_path, wc):
        # prevent deleting the current branch
        r = cli_runner.invoke(["branch", "-d", "master"])
        assert r.exit_code == INVALID_OPERATION, r
        assert "Cannot delete" in r.stderr

        r = cli_runner.invoke(["checkout", "-b", "test"])
        assert r.exit_code == 0, r

        r = cli_runner.invoke(["branch", "-d", "test"])
        assert r.exit_code == INVALID_OPERATION, r

        r = cli_runner.invoke(["checkout", "master"])
        assert r.exit_code == 0, r

        r = cli_runner.invoke(["branch", "-d", "test"])
        assert r.exit_code == 0, r
