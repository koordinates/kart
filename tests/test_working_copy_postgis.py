import pytest

import pygit2

from sno.sno_repo import SnoRepo
from sno.working_copy import WorkingCopy


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
@pytest.mark.parametrize("version", ["1", "2"])
def test_checkout_workingcopy(
    version, archive, table, commit_sha, data_archive, cli_runner, new_postgis_db_schema
):
    """ Checkout a working copy """
    if version == "2":
        archive += "2"
    with data_archive(archive) as repo_path:
        repo = SnoRepo(repo_path)
        H.clear_working_copy()

        with new_postgis_db_schema() as (postgres_url, postgres_schema):
            repo.config["sno.workingcopy.path"] = postgres_url
            r = cli_runner.invoke(["checkout"])
            assert r.exit_code == 0, r.stderr
            assert r.stdout.splitlines() == [
                f"Creating working copy at {postgres_url} ..."
            ]

            r = cli_runner.invoke(["status"])
            assert r.exit_code == 0, r.stderr
            assert r.stdout.splitlines() == [
                "On branch master",
                "",
                "Nothing to commit, working copy clean",
            ]

            wc = WorkingCopy.get(repo)
            assert wc.is_created()

            head_tree_id = repo.head.peel(pygit2.Tree).id.hex
            assert wc.assert_db_tree_match(head_tree_id)


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
def test_commit_edits(
    archive,
    table,
    commit_sha,
    data_archive,
    cli_runner,
    new_postgis_db_schema,
    edit_points,
    edit_polygons,
    edit_table,
):
    """ Checkout a working copy and make some edits """
    with data_archive(f"{archive}2") as repo_path:
        repo = SnoRepo(repo_path)
        H.clear_working_copy()

        with new_postgis_db_schema() as (postgres_url, postgres_schema):
            repo.config["sno.workingcopy.path"] = postgres_url
            r = cli_runner.invoke(["checkout"])
            assert r.exit_code == 0, r.stderr
            assert r.stdout.splitlines() == [
                f"Creating working copy at {postgres_url} ..."
            ]

            r = cli_runner.invoke(["status"])
            assert r.exit_code == 0, r.stderr
            assert r.stdout.splitlines() == [
                "On branch master",
                "",
                "Nothing to commit, working copy clean",
            ]

            wc = WorkingCopy.get(repo)
            assert wc.is_created()

            table_prefix = postgres_schema + "."
            with wc.session() as db:
                dbcur = db.cursor()
                if archive == "points":
                    edit_points(dbcur, table_prefix)
                elif archive == "polygons":
                    edit_polygons(dbcur, table_prefix)
                elif archive == "table":
                    edit_table(dbcur, table_prefix)

            r = cli_runner.invoke(["status"])
            assert r.exit_code == 0, r.stderr
            assert r.stdout.splitlines() == [
                "On branch master",
                "",
                "Changes in working copy:",
                '  (use "sno commit" to commit)',
                '  (use "sno reset" to discard changes)',
                "",
                f"  {table}:",
                "    feature:",
                "      1 inserts",
                "      2 updates",
                "      5 deletes",
            ]
            orig_head = repo.head.peel(pygit2.Commit).hex

            r = cli_runner.invoke(["commit", "-m", "test_commit"])
            assert r.exit_code == 0, r.stderr

            r = cli_runner.invoke(["status"])
            assert r.exit_code == 0, r.stderr
            assert r.stdout.splitlines() == [
                "On branch master",
                "",
                "Nothing to commit, working copy clean",
            ]

            new_head = repo.head.peel(pygit2.Commit).hex
            assert new_head != orig_head

            r = cli_runner.invoke(["checkout", "HEAD^"])

            assert repo.head.peel(pygit2.Commit).hex == orig_head
