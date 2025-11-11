import json

from kart.exceptions import NO_CHANGES, NotFound, InvalidOperation
from kart.repo import KartRepo

import pytest

H = pytest.helpers.helpers()


def test_mv_basic__gpkg(cli_runner, data_working_copy):
    """Test basic dataset rename"""
    with data_working_copy("points") as (path, wc):
        repo = KartRepo(path)

        # Rename the dataset
        r = cli_runner.invoke(
            ["mv", H.POINTS.LAYER, "renamed_points", "-m", "Rename points"]
        )
        assert r.exit_code == 0, r.stderr

        assert H.POINTS.LAYER not in repo.datasets()
        assert "renamed_points" in repo.datasets()

        # Verify the working copy is clean
        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r.stderr
        assert "Nothing to commit" in r.stdout


def test_mv_json_output__gpkg(cli_runner, data_working_copy):
    """Test JSON output for mv command"""
    with data_working_copy("points") as (path, wc):
        repo = KartRepo(path)
        message = "Rename dataset"

        r = cli_runner.invoke(
            ["mv", H.POINTS.LAYER, "renamed_points", "-m", message, "-o", "json"],
            env={
                "GIT_COMMITTER_DATE": "2010-1-1T00:00:00Z",
                "GIT_AUTHOR_EMAIL": "user@example.com",
                "GIT_COMMITTER_EMAIL": "committer@example.com",
            },
        )

        assert r.exit_code == 0, r.stderr

        output = json.loads(r.stdout)
        assert "kart.commit/v1" in output
        assert output["kart.commit/v1"]["message"] == message
        assert output["kart.commit/v1"]["branch"] == "main"


def test_mv_nonexistent_dataset__gpkg(cli_runner, data_working_copy):
    """Test trying to rename a dataset that doesn't exist"""
    with data_working_copy("points") as (path, wc):
        try:
            cli_runner.invoke(["mv", "nonexistent", "new_name", "-m", "test"])
        except NotFound as e:
            assert "nonexistent" in str(e)
            assert e.exit_code == NO_CHANGES


def test_mv_to_existing_name__gpkg(cli_runner, data_archive):
    """Test trying to rename a dataset to a name that already exists"""
    with data_archive("polygons") as repo_path:
        # Import points as well so we have two datasets
        cli_runner.invoke(["import", "data/points.gpkg"])

        try:
            cli_runner.invoke(["mv", H.POLYGONS.LAYER, H.POINTS.LAYER, "-m", "test"])
        except InvalidOperation as e:
            assert "already exists" in str(e)
            assert e.exit_code == NO_CHANGES


def test_mv_working_copy_updated__gpkg(cli_runner, data_working_copy):
    """Test that the working copy is properly updated after rename"""
    with data_working_copy("points") as (path, wc):
        repo = KartRepo(path)

        # Rename the dataset
        r = cli_runner.invoke(["mv", H.POINTS.LAYER, "renamed_points", "-m", "Rename"])
        assert r.exit_code == 0, r.stderr

        # Check that the renamed table exists in the working copy
        with repo.working_copy.tabular.session() as sess:
            result = sess.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='renamed_points'"
            )
            tables = [row[0] for row in result]
            assert "renamed_points" in tables

            # Check that the old table doesn't exist
            result = sess.execute(
                f"SELECT name FROM sqlite_master WHERE type='table' AND name='{H.POINTS.LAYER}'"
            )
            tables = [row[0] for row in result]
            assert H.POINTS.LAYER not in tables


def test_mv_dirty_working_copy__gpkg(cli_runner, data_working_copy):
    """Test that mv fails when the working copy is dirty"""
    with data_working_copy("points") as (path, wc):
        repo = KartRepo(path)
        table_wc = repo.working_copy.tabular

        # Make the working copy dirty by deleting a feature
        with table_wc.session() as sess:
            r = sess.execute(
                f"DELETE FROM {H.POINTS.LAYER} WHERE {H.POINTS.LAYER_PK} = 1;"
            )
            assert r.rowcount == 1

        # Verify the working copy is dirty
        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r.stderr
        assert "Changes" in r.stdout or "modified" in r.stdout.lower()

        # Try to rename - should fail
        r = cli_runner.invoke(["mv", H.POINTS.LAYER, "renamed_points", "-m", "Rename"])
        assert r.exit_code != 0, r.stderr
        assert "dirty" in r.stderr.lower() or "uncommitted" in r.stderr.lower()


def test_mv_hierarchical_dataset__gpkg(cli_runner, data_archive, tmp_path):
    """Test renaming a dataset with a slash in its name (hierarchical dataset)"""
    with data_archive("points") as repo_path:
        repo = KartRepo(repo_path)

        # Import with a hierarchical path
        source_gpkg = tmp_path / "source.gpkg"
        cli_runner.invoke(["export", H.POINTS.LAYER, str(source_gpkg)])

        r = cli_runner.invoke(
            ["import", str(source_gpkg), f"{H.POINTS.LAYER}:path/to/dataset"]
        )
        assert r.exit_code == 0, r.stderr
        assert "path/to/dataset" in repo.datasets()

        # rename it
        r = cli_runner.invoke(
            ["mv", "path/to/dataset", "new/path/dataset", "-m", "Rename hierarchical"]
        )
        assert r.exit_code == 0, r.stderr

        assert "path/to/dataset" not in repo.datasets()
        assert "new/path/dataset" in repo.datasets()
