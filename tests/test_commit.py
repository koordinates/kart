from ast import Dict
import json
import re
import shlex
import time

import pytest

import kart
from kart.exceptions import (
    INVALID_ARGUMENT,
    NO_CHANGES,
    NO_DATA,
    NO_REPOSITORY,
    SCHEMA_VIOLATION,
    NO_CHANGES,
    NotFound,
    InvalidOperation,
)
from kart.commit import fallback_editor
from kart.repo import KartRepo


H = pytest.helpers.helpers()


@pytest.mark.parametrize(
    "partial",
    [pytest.param(False, id=""), pytest.param(True, id="partial")],
)
@pytest.mark.parametrize(
    "archive,layer",
    [
        pytest.param("points", H.POINTS.LAYER, id="points"),
        pytest.param("polygons", H.POLYGONS.LAYER, id="polygons"),
        pytest.param("table", H.TABLE.LAYER, id="table"),
    ],
)
def test_commit(
    archive,
    layer,
    partial,
    data_working_copy,
    cli_runner,
    request,
    edit_points,
    edit_polygons,
    edit_table,
):
    """commit outstanding changes from the working copy"""

    with data_working_copy(archive) as (repo_dir, wc_path):
        # empty
        r = cli_runner.invoke(["commit", "-m", "test-commit-empty"])
        assert r.exit_code == NO_CHANGES, r
        assert r.stderr.splitlines() == ["Error: No changes to commit"]

        # empty
        r = cli_runner.invoke(["commit", "-m", "test-commit-empty", "--allow-empty"])
        assert r.exit_code == 0, r

        # make some changes
        repo = KartRepo(repo_dir)
        with repo.working_copy.tabular.session() as sess:
            try:
                edit_func = locals()[f"edit_{archive}"]
                pk_del = edit_func(sess)
            except KeyError:
                raise NotImplementedError(f"No edit_{archive}")

        print(f"deleted fid={pk_del}")

        repo = KartRepo(repo_dir)
        dataset = repo.datasets()[layer]

        table_wc = repo.working_copy.tabular
        original_change_count = table_wc.tracking_changes_count(dataset)

        if partial:
            r = cli_runner.invoke(
                ["commit", "-m", "test-commit-1", "-o", "json", f"{layer}:{pk_del}"]
            )
        else:
            r = cli_runner.invoke(["commit", "-m", "test-commit-1", "-o", "json"])

        assert r.exit_code == 0, r
        commit_id = json.loads(r.stdout)["kart.commit/v1"]["commit"]
        print("commit:", commit_id)

        assert str(repo.head.target) == commit_id
        commit = repo.head_commit
        assert commit.message == "test-commit-1"
        assert time.time() - commit.commit_time < 10

        tree = repo.head_tree
        assert dataset.encode_1pk_to_path(pk_del) not in tree

        table_wc.assert_matches_tree(tree)
        change_count = table_wc.tracking_changes_count(dataset)

        if partial:
            # All but one change should still be in the tracking table
            assert change_count == original_change_count - 1

            # Changes should still be visible in the working copy:
            r = cli_runner.invoke(["diff", "--exit-code"])
            assert r.exit_code == 1, r
            assert r.stdout != ""

        else:
            assert (
                change_count == 0
            ), f"Changes still listed in {table_wc.KART_TRACK_NAME} after full commit"

            r = cli_runner.invoke(["diff", "--exit-code"])
            assert r.exit_code == 0, r
            assert r.stdout == ""


def test_tag(data_working_copy, cli_runner):
    """review commit history"""
    with data_working_copy("points") as (repo_dir, wc):
        # create a tag
        r = cli_runner.invoke(["tag", "version1"])
        assert r.exit_code == 0, r

        repo = KartRepo(repo_dir)
        assert "refs/tags/version1" in repo.references
        ref = repo.lookup_reference_dwim("version1")
        assert ref.target.hex == H.POINTS.HEAD_SHA


def test_commit_message(
    data_working_copy, cli_runner, monkeypatch, tmp_path, edit_points
):
    """commit message handling"""
    editor_in = None
    editor_out = None
    editor_cmd = None

    def monkey_editor(cmdline):
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

    monkeypatch.setattr(kart.commit, "run_editor_cmd", monkey_editor)
    monkeypatch.delenv("EDITOR", raising=False)
    monkeypatch.delenv("VISUAL", raising=False)
    monkeypatch.delenv("GIT_EDITOR", raising=False)

    with data_working_copy("points") as (repo_dir, wc_path):
        repo = KartRepo(repo_dir)

        def last_message():
            return repo.head_commit.message

        # normal
        r = cli_runner.invoke(
            ["commit", "--allow-empty", "-m", "the messagen\n\n\n\n\n"]
        )
        assert r.exit_code == 0, r
        assert last_message() == "the messagen"

        # E: empty
        r = cli_runner.invoke(["commit", "--allow-empty", "-m", ""])
        assert r.exit_code == INVALID_ARGUMENT, r

        # file
        f_commit_message = str(tmp_path / "commit-message.txt")
        with open(f_commit_message, mode="w", encoding="utf8") as f:
            f.write("\ni am a message\n\n\n")
            f.flush()

        r = cli_runner.invoke(
            ["commit", "--allow-empty", f"--message=@{f_commit_message}"]
        )
        assert r.exit_code == 0, r
        assert last_message() == "i am a message"

        # E: conflict
        r = cli_runner.invoke(
            ["commit", "--allow-empty", f"--message=@{f_commit_message}", "-m", "foo"]
        )
        assert r.exit_code == 0, r
        assert last_message() == "i am a message\n\nfoo"

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
        repo = KartRepo(repo_dir)
        with repo.working_copy.tabular.session() as sess:
            edit_points(sess)

        editor_out = "I am a message\n#of hope, and\nof warning\n\t\n"
        r = cli_runner.invoke(["commit"])
        assert r.exit_code == 0, r
        editmsg_path = str(repo.gitdir_file("COMMIT_EDITMSG"))
        assert re.match(
            rf'{fallback_editor()} "?{re.escape(editmsg_path)}"?$', editor_cmd
        )
        assert editor_in.splitlines() == [
            "",
            "# Please enter the commit message for your changes. Lines starting",
            "# with '#' will be ignored, and an empty message aborts the commit.",
            "#",
            "# On branch main",
            "#",
            "# Changes to be committed:",
            "#",
            "#   nz_pa_points_topo_150k:",
            "#     feature:",
            "#       1 inserts",
            "#       2 updates",
            "#       5 deletes",
            "#",
        ]

        print(last_message())
        assert last_message() == "I am a message\nof warning"

        monkeypatch.setenv("EDITOR", "/path/to/some/editor -abc")
        editor_out = "sqwark 🐧\n"
        r = cli_runner.invoke(["commit", "--allow-empty"])
        assert r.exit_code == 0, r
        editmsg_path = str(repo.gitdir_file("COMMIT_EDITMSG"))
        assert re.match(
            rf'/path/to/some/editor -abc "?{re.escape(editmsg_path)}"?$', editor_cmd
        )
        assert editor_in == (
            "\n"
            "# Please enter the commit message for your changes. Lines starting\n"
            "# with '#' will be ignored, and an empty message aborts the commit.\n"
            "#\n"
            "# On branch main\n"
            "#\n"
            "# Changes to be committed:\n"
            "#\n"
            "#   No changes (empty commit)\n"
            "#\n"
        )
        print(last_message())
        assert last_message() == "sqwark 🐧"


def test_empty(tmp_path, cli_runner, chdir):
    repo_path = tmp_path / "one"

    # empty repo
    r = cli_runner.invoke(["init", str(repo_path)])
    assert r.exit_code == 0, r
    with chdir(repo_path):
        r = cli_runner.invoke(["commit", "--allow-empty"])
        assert r.exit_code == NO_DATA, r
        assert "Empty repository" in r.stderr

    # empty dir
    empty_path = tmp_path / "two"
    empty_path.mkdir()
    with chdir(empty_path):
        r = cli_runner.invoke(["commit", "--allow-empty"])
        assert r.exit_code == NO_REPOSITORY, r
        assert "not an existing Kart repository" in r.stderr


def test_commit_user_info(tmp_path, cli_runner, chdir, data_working_copy):
    with data_working_copy("points") as (repo_dir, wc_path):
        repo = KartRepo(repo_dir)

        # normal
        r = cli_runner.invoke(
            ["commit", "--allow-empty", "-m", "test"],
            env={
                "GIT_AUTHOR_DATE": "1000000000 +1230",
                "GIT_AUTHOR_NAME": "bob",
                "GIT_AUTHOR_EMAIL": "user@example.com",
            },
        )
        assert r.exit_code == 0, r

        author = repo.head_commit.author
        assert author.name == "bob"
        assert author.email == "user@example.com"
        assert author.time == 1000000000
        assert author.offset == 750


def test_commit_schema_violation(cli_runner, data_working_copy):
    with data_working_copy("points") as (repo_dir, wc_path):
        repo = KartRepo(repo_dir)
        with repo.working_copy.tabular.session() as sess:
            sess.execute(f"""UPDATE {H.POINTS.LAYER} SET geom="text" WHERE fid=1;""")
            sess.execute(
                f"UPDATE {H.POINTS.LAYER} SET t50_fid=123456789012 WHERE fid=2;"
            )
            sess.execute(
                f"""UPDATE {H.POINTS.LAYER} SET macronated="kinda" WHERE fid=3;"""
            )

        r = cli_runner.invoke(["commit", "-m", "test"])
        assert r.exit_code == SCHEMA_VIOLATION, r.stderr
        assert r.stderr.splitlines() == [
            "nz_pa_points_topo_150k: In column 'geom' value 'text' doesn't match schema type geometry",
            "nz_pa_points_topo_150k: In column 't50_fid' value 123456789012 does not fit into an int32: -2147483648 to 2147483647",
            "nz_pa_points_topo_150k: In column 'macronated' value 'kinda' exceeds limit of 1 characters",
            "Error: Schema violation - values do not match schema",
        ]


def test_commit_table_json_output(cli_runner, data_working_copy):
    new_table = "test_table"
    message = "test commit"
    with data_working_copy("points") as (path, wc):
        repo = KartRepo(path)
        with repo.working_copy.tabular.session() as sess:
            sess.execute(
                f"""CREATE TABLE IF NOT EXISTS {new_table} (test_id int primary key, field1 text, field2 text);"""
            )
            sess.execute(
                f"""INSERT INTO {new_table} (test_id, field1, field2)
                VALUES
                    (1, 'value1a', 'value1b'),
                    (2, 'value2a', 'value2b'),
                    (3, 'value3a', 'value3b'),
                    (4, 'value4a', 'value4b'),
                    (5, 'value5a', 'value5b');"""
            )

        r = cli_runner.invoke(
            ["add-dataset", new_table, "-m", message, "-o", "json"],
            env={
                "GIT_COMMITTER_DATE": "2010-1-1T00:00:00Z",
                "GIT_AUTHOR_EMAIL": "user@example.com",
                "GIT_COMMITTER_EMAIL": "committer@example.com",
            },
        )

        assert r.exit_code == 0, r

        expected_output: Dict[str, Dict[str, any]] = {
            "kart.commit/v1": {
                "commit": str(repo.head.target),
                "abbrevCommit": str(repo.head.target)[:7],
                "author": "user@example.com",
                "committer": "committer@example.com",
                "branch": "main",
                "message": message,
                "changes": {
                    new_table: {"meta": {"inserts": 1}, "feature": {"inserts": 5}}
                },
                "commitTime": "2010-01-01T00:00:00Z",
                "commitTimeOffset": "+00:00",
            }
        }

        assert json.loads(r.stdout) == expected_output


def test_commit_table_text_output(cli_runner, data_working_copy):
    new_table = "test_table"
    message = "test commit"
    with data_working_copy("points") as (path, wc):
        repo = KartRepo(path)
        with repo.working_copy.tabular.session() as sess:
            sess.execute(
                f"""CREATE TABLE IF NOT EXISTS {new_table} (test_id int primary key, field1 text, field2 text);"""
            )
            sess.execute(
                f"""INSERT INTO {new_table} (test_id, field1, field2)
                VALUES
                    (1, 'value1a', 'value1b'),
                    (2, 'value2a', 'value2b'),
                    (3, 'value3a', 'value3b'),
                    (4, 'value4a', 'value4b'),
                    (5, 'value5a', 'value5b');"""
            )

        r = cli_runner.invoke(
            ["add-dataset", new_table, "-m", message, "-o", "text"],
            env={
                "GIT_COMMITTER_DATE": "2010-1-1T00:00:00Z",
            },
        )

        assert r.exit_code == 0, r

        diff = {new_table: {"meta": {"inserts": 1}, "feature": {"inserts": 5}}}

        flat_diff = ""
        for table, table_diff in diff.items():
            flat_diff += f"  {table}:\n"
            for section, section_diff in table_diff.items():
                for op, count in section_diff.items():
                    flat_diff += f"    {section}:\n"
                    flat_diff += f"      {count} {op}\n"

        expected_output = f"[main {str(repo.head.target)[:7]}] {message}\n{flat_diff}  Date: Fri Jan  1 00:00:00 2010 +0000\n"

        assert r.stdout == expected_output


def test_commit_table_nonexistent(cli_runner, data_working_copy):
    new_table = "test_table"
    wrong_table = "wrong_test_table"
    message = "test commit"
    with data_working_copy("points") as (path, wc):
        repo = KartRepo(path)
        with repo.working_copy.tabular.session() as sess:
            sess.execute(
                f"""CREATE TABLE IF NOT EXISTS {new_table} (test_id int primary key, field1 text, field2 text);"""
            )

        try:
            cli_runner.invoke(["add-dataset", wrong_table, "-m", message, "-o", "text"])
        except NotFound as e:
            assert (
                str(e)
                == f"""Table '{wrong_table}' is not found\n\nTry running 'kart status --list-untracked-tables'\n"""
            )
            assert e.exit_code == NO_CHANGES


def test_commit_table_twice(cli_runner, data_working_copy):
    new_table = "test_table"
    message1 = "test commit1"
    message2 = "test commit2"

    with data_working_copy("points") as (path, wc):
        repo = KartRepo(path)
        with repo.working_copy.tabular.session() as sess:
            sess.execute(
                f"""CREATE TABLE IF NOT EXISTS {new_table} (test_id int primary key, field1 text, field2 text);"""
            )

        try:
            cli_runner.invoke(["add-dataset", new_table, "-m", message1, "-o", "text"])
            cli_runner.invoke(["add-dataset", new_table, "-m", message2, "-o", "text"])
        except InvalidOperation as e:
            assert (str(e) == f"Table '{new_table}' is already tracked\n",)
            assert e.exit_code == NO_CHANGES
