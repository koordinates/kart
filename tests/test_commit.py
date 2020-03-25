import json
import os
import re
import shlex
import subprocess
import time

import pytest

import pygit2

from sno.commit import FALLBACK_EDITOR
from sno.structure import RepositoryStructure
from sno.working_copy import WorkingCopy


H = pytest.helpers.helpers()


def edit_points(dbcur):
    dbcur.execute(H.POINTS_INSERT, H.POINTS_RECORD)
    assert dbcur.getconnection().changes() == 1
    dbcur.execute(f"UPDATE {H.POINTS_LAYER} SET fid=9998 WHERE fid=1;")
    assert dbcur.getconnection().changes() == 1
    dbcur.execute(f"UPDATE {H.POINTS_LAYER} SET name='test' WHERE fid=2;")
    assert dbcur.getconnection().changes() == 1
    dbcur.execute(f"DELETE FROM {H.POINTS_LAYER} WHERE fid IN (3,30,31,32,33);")
    assert dbcur.getconnection().changes() == 5
    pk_del = 3
    return pk_del


def edit_polygons_pk(dbcur):
    dbcur.execute(H.POLYGONS_INSERT, H.POLYGONS_RECORD)
    assert dbcur.getconnection().changes() == 1
    dbcur.execute(f"UPDATE {H.POLYGONS_LAYER} SET id=9998 WHERE id=1424927;")
    assert dbcur.getconnection().changes() == 1
    dbcur.execute(
        f"UPDATE {H.POLYGONS_LAYER} SET survey_reference='test' WHERE id=1443053;"
    )
    assert dbcur.getconnection().changes() == 1
    dbcur.execute(
        f"DELETE FROM {H.POLYGONS_LAYER} WHERE id IN (1452332, 1456853, 1456912, 1457297, 1457355);"
    )
    assert dbcur.getconnection().changes() == 5
    pk_del = 1452332
    return pk_del


def edit_table(dbcur):
    dbcur.execute(H.TABLE_INSERT, H.TABLE_RECORD)
    assert dbcur.getconnection().changes() == 1
    dbcur.execute(f"UPDATE {H.TABLE_LAYER} SET OBJECTID=9998 WHERE OBJECTID=1;")
    assert dbcur.getconnection().changes() == 1
    dbcur.execute(f"UPDATE {H.TABLE_LAYER} SET name='test' WHERE OBJECTID=2;")
    assert dbcur.getconnection().changes() == 1
    dbcur.execute(f"DELETE FROM {H.TABLE_LAYER} WHERE OBJECTID IN (3,30,31,32,33);")
    assert dbcur.getconnection().changes() == 5
    pk_del = 3
    return pk_del


@pytest.mark.parametrize(
    "archive,layer",
    [
        pytest.param("points", H.POINTS_LAYER, id="points"),
        pytest.param("polygons", H.POLYGONS_LAYER, id="polygons_pk"),
        pytest.param("table", H.TABLE_LAYER, id="table"),
    ],
)
def test_commit(archive, layer, data_working_copy, geopackage, cli_runner, request):
    """ commit outstanding changes from the working copy """
    param_ids = H.parameter_ids(request)

    with data_working_copy(archive) as (repo_dir, wc_path):
        # empty
        r = cli_runner.invoke(["commit", "-m", "test-commit-empty"])
        assert r.exit_code == 1, r
        assert r.stdout.splitlines() == ["Error: No changes to commit"]

        # empty
        r = cli_runner.invoke(["commit", "-m", "test-commit-empty", "--allow-empty"])
        assert r.exit_code == 0, r

        # make some changes
        db = geopackage(wc_path)
        with db:
            cur = db.cursor()
            try:
                edit_func = globals()[f"edit_{param_ids[0]}"]
                pk_del = edit_func(cur)
            except KeyError:
                raise NotImplementedError(f"layer={layer}")

        print(f"deleted fid={pk_del}")

        r = cli_runner.invoke(["commit", "-m", "test-commit-1", "--json"])
        assert r.exit_code == 0, r
        commit_id = json.loads(r.stdout)["sno.commit/v1"]["commit"]
        print("commit:", commit_id)

        repo = pygit2.Repository(str(repo_dir))
        assert str(repo.head.target) == commit_id
        commit = repo.head.peel(pygit2.Commit)
        assert commit.message == "test-commit-1"
        assert time.time() - commit.commit_time < 3

        rs = RepositoryStructure(repo)
        wc = rs.working_copy
        dataset = rs[layer]

        tree = repo.head.peel(pygit2.Tree)
        assert dataset.get_feature_path(pk_del) not in tree

        cur.execute(
            f"SELECT COUNT(*) FROM {wc.TRACKING_TABLE} WHERE table_name=?;", [layer]
        )
        change_count = cur.fetchone()[0]
        assert change_count == 0, f"Changes still listed in {dataset.TRACKING_TABLE}"

        wc = WorkingCopy.open(repo)
        wc.assert_db_tree_match(tree)

        r = cli_runner.invoke(["diff"])
        assert r.exit_code == 0, r
        assert r.stdout == ""


def test_tag(data_working_copy, cli_runner):
    """ review commit history """
    with data_working_copy("points") as (repo_dir, wc):
        # create a tag
        r = cli_runner.invoke(["tag", "version1"])
        assert r.exit_code == 0, r

        repo = pygit2.Repository(str(repo_dir))
        assert "refs/tags/version1" in repo.references
        ref = repo.lookup_reference_dwim("version1")
        assert ref.target.hex == H.POINTS_HEAD_SHA


def test_commit_message(
    data_working_copy, cli_runner, monkeypatch, geopackage, tmp_path
):
    """ commit message handling """
    editor_in = None
    editor_out = None
    editor_cmd = None

    def monkey_editor(cmdline, **kwargs):
        nonlocal editor_cmd, editor_in
        editor_cmd = cmdline
        print("EDITOR", cmdline)
        editmsg_file = shlex.split(cmdline)[-1]
        with open(editmsg_file, "r+", encoding="utf-8") as ef:
            editor_in = ef.read()
            if editor_out:
                ef.seek(0)
                ef.truncate()
                ef.write(editor_out)
                return 0
            else:
                assert False, "Didn't expect editor to launch"

    monkeypatch.setattr(subprocess, "check_call", monkey_editor)
    monkeypatch.delenv("EDITOR", raising=False)
    monkeypatch.delenv("VISUAL", raising=False)
    monkeypatch.delenv("GIT_EDITOR", raising=False)

    with data_working_copy("points") as (repo_dir, wc_path):
        repo = pygit2.Repository(str(repo_dir))

        def last_message():
            return repo.head.peel(pygit2.Commit).message

        # normal
        r = cli_runner.invoke(
            ["commit", "--allow-empty", "-m", "the messagen\n\n\n\n\n"]
        )
        assert r.exit_code == 0, r
        assert last_message() == "the messagen"

        # E: empty
        r = cli_runner.invoke(["commit", "--allow-empty", "-m", ""])
        assert r.exit_code == 1, r

        # file
        f_commit_message = str(tmp_path / "commit-message.txt")
        with open(f_commit_message, mode="w", encoding="utf8") as f:
            f.write("\ni am a message\n\n\n")
            f.flush()

        r = cli_runner.invoke(["commit", "--allow-empty", "-F", f_commit_message])
        assert r.exit_code == 0, r
        assert last_message() == "i am a message"

        # E: conflict
        r = cli_runner.invoke(
            ["commit", "--allow-empty", "-F", f_commit_message, "-m", "foo"]
        )
        assert r.exit_code == 2, r
        assert "exclusive" in r.stdout

        # multiple
        r = cli_runner.invoke(
            [
                "commit",
                "--allow-empty",
                "-m",
                "one",
                "-m",
                "two\nthree\n",
                "-m",
                "four\n\n",
            ]
        )
        assert r.exit_code == 0, r
        assert last_message() == "one\n\ntwo\nthree\n\nfour"

        # default editor

        # make some changes
        db = geopackage(wc_path)
        with db:
            cur = db.cursor()
            edit_points(cur)

        editor_out = "I am a message\n#of hope, and\nof warning\n\t\n"
        r = cli_runner.invoke(["commit"])
        assert r.exit_code == 0, r
        editmsg_path = f"{repo_dir}{os.sep}COMMIT_EDITMSG"
        assert re.match(
            rf'{FALLBACK_EDITOR} "?{re.escape(editmsg_path)}"?$', editor_cmd
        )
        assert editor_in == (
            "\n"
            "# Please enter the commit message for your changes. Lines starting\n"
            "# with '#' will be ignored, and an empty message aborts the commit.\n"
            "#\n"
            "# On branch master\n"
            "#\n"
            "# Changes to be committed:\n"
            "#\n"
            "#   nz_pa_points_topo_150k/\n"
            "#     modified:  2 features\n"
            "#     new:       1 feature\n"
            "#     deleted:   5 features\n"
            "#\n"
        )
        print(last_message())
        assert last_message() == "I am a message\nof warning"

        monkeypatch.setenv("EDITOR", "/path/to/some/editor -abc")
        editor_out = "sqwark 🐧\n"
        r = cli_runner.invoke(["commit", "--allow-empty"])
        assert r.exit_code == 0, r
        editmsg_path = f"{repo_dir}{os.sep}COMMIT_EDITMSG"
        assert re.match(
            rf'/path/to/some/editor -abc "?{re.escape(editmsg_path)}"?$', editor_cmd
        )
        assert editor_in == (
            "\n"
            "# Please enter the commit message for your changes. Lines starting\n"
            "# with '#' will be ignored, and an empty message aborts the commit.\n"
            "#\n"
            "# On branch master\n"
            "#\n"
            "# Changes to be committed:\n"
            "#\n"
            "#   No changes (empty commit)\n"
            "#\n"
        )
        print(last_message())
        assert last_message() == "sqwark 🐧"


def test_empty(tmp_path, cli_runner, chdir):
    repo_path = tmp_path / "one.sno"

    # empty repo
    r = cli_runner.invoke(["init", repo_path])
    assert r.exit_code == 0, r
    with chdir(repo_path):
        r = cli_runner.invoke(["commit", "--allow-empty"])
        assert r.exit_code == 2, r
        assert "Empty repository" in r.stdout

    # empty dir
    empty_path = tmp_path / "two"
    empty_path.mkdir()
    with chdir(empty_path):
        r = cli_runner.invoke(["commit", "--allow-empty"])
        assert r.exit_code == 2, r
        assert "not an existing repository" in r.stdout
