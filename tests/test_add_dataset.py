from ast import Dict
import json

from kart.exceptions import (
    NO_CHANGES,
    NotFound,
    InvalidOperation,
)
from kart.repo import KartRepo

import pytest

H = pytest.helpers.helpers()


def test_add_dataset_json_output__gpkg(cli_runner, data_working_copy):
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


def test_add_dataset_text_output__gpkg(cli_runner, data_working_copy):
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


def test_add_dataset_nonexistent__gpkg(cli_runner, data_working_copy):
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


def test_add_dataset_twice__gpkg(cli_runner, data_working_copy):
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
            assert str(e) == f"Table '{new_table}' is already tracked\n"
            assert e.exit_code == NO_CHANGES


def test_add_dataset_triggers__gpkg(cli_runner, data_working_copy):
    new_table = "test_table"
    message = "test commit"

    # Test how diff handles an existing table with triggers
    with data_working_copy("points") as (path, wc):
        repo = KartRepo(path)

        # Test how diff handles a new table after add-dataset
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

        r = cli_runner.invoke(["add-dataset", new_table, "-m", message, "-o", "text"])
        with repo.working_copy.tabular.session() as sess:
            sess.execute(
                f"""DELETE FROM {new_table}
                    WHERE test_id = 1;"""
            )
        r = cli_runner.invoke(["diff", "-o", "json"])

        output = json.loads(r.stdout)

        expected = {
            "test_table": {
                "feature": [
                    {
                        "-": {
                            "test_id": 1,
                            "field1": "value1a",
                            "field2": "value1b",
                        }
                    },
                ]
            }
        }

        assert output["kart.diff/v1+hexwkb"] == expected


def test_add_dataset__postgis(data_archive, cli_runner, new_postgis_db_schema):
    with data_archive("points") as repo_path:
        repo = KartRepo(repo_path)
        H.clear_working_copy()
        with new_postgis_db_schema() as (postgres_url, postgres_schema):
            r = cli_runner.invoke(["create-workingcopy", postgres_url])
            assert r.exit_code == 0, r.stderr

            with repo.working_copy.tabular.session() as sess:
                sess.execute(
                    f"""CREATE TABLE {postgres_schema}.dupe AS (SELECT * FROM {postgres_schema}.{H.POINTS.LAYER});"""
                )
                sess.execute(
                    f"""ALTER TABLE {postgres_schema}.dupe ADD PRIMARY KEY ({H.POINTS.LAYER_PK});"""
                )

            r = cli_runner.invoke(["status", "-ojson", "--list-untracked-tables"])
            assert r.exit_code == 0, r.stderr

            output = json.loads(r.stdout)
            assert output["kart.status/v2"]["workingCopy"]["untrackedTables"] == [
                "dupe"
            ]

            r = cli_runner.invoke(
                ["add-dataset", "dupe", "-ojson", "-m" "test commit"],
                env={
                    "GIT_AUTHOR_DATE": "2010-1-1T00:00:00Z",
                    "GIT_COMMITTER_DATE": "2010-1-1T00:00:00Z",
                    "GIT_AUTHOR_EMAIL": "user@example.com",
                    "GIT_COMMITTER_EMAIL": "committer@example.com",
                },
            )
            assert r.exit_code == 0, r.stderr

            output = json.loads(r.stdout)
            COMMIT_SHA = output["kart.commit/v1"]["commit"]
            ABBREV_COMMIT_SHA = output["kart.commit/v1"]["abbrevCommit"]
            assert output == {
                "kart.commit/v1": {
                    "commit": COMMIT_SHA,
                    "abbrevCommit": ABBREV_COMMIT_SHA,
                    "author": "user@example.com",
                    "committer": "committer@example.com",
                    "branch": "main",
                    "message": "test commit",
                    "changes": {
                        "dupe": {"meta": {"inserts": 2}, "feature": {"inserts": 2143}}
                    },
                    "commitTime": "2010-01-01T00:00:00Z",
                    "commitTimeOffset": "+00:00",
                }
            }
