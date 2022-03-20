from glob import glob
import json
import re
import subprocess
import pytest

from kart.exceptions import INVALID_FILE_FORMAT
from kart.repo import KartRepo

DUMMY_REPO = "git@example.com/example.git"

# using a fixture instead of a skipif decorator means we get one aggregated skip
# message rather than one per test
@pytest.fixture(scope="session")
def requires_pdal():
    has_pdal = False
    try:
        import pdal

        assert pdal.Pipeline
        has_pdal = True
    except ModuleNotFoundError:
        pass

    pytest.helpers.feature_assert_or_skip(
        "pdal package installed", "KART_EXPECT_PDAL", has_pdal, ci_require=False
    )


@pytest.fixture(scope="session")
def requires_git_lfs():
    r = subprocess.run(["git", "lfs", "--version"])
    has_git_lfs = r.returncode == 0

    pytest.helpers.feature_assert_or_skip(
        "Git LFS installed", "KART_EXPECT_GIT_LFS", has_git_lfs, ci_require=False
    )


def test_import_single_las(
    tmp_path, chdir, cli_runner, data_archive_readonly, requires_pdal, requires_git_lfs
):
    # Using postgres here because it has the best type preservation
    with data_archive_readonly("point-cloud/autzen.tgz") as autzen:
        repo_path = tmp_path / "point-cloud-repo"
        r = cli_runner.invoke(["init", repo_path])
        assert r.exit_code == 0, r.stderr

        repo = KartRepo(repo_path)
        with chdir(repo_path):
            r = cli_runner.invoke(
                ["point-cloud-import", f"{autzen}/autzen.las", "--dataset-path=autzen"]
            )
            assert r.exit_code == 0, r.stderr

            r = cli_runner.invoke(["data", "ls"])
            assert r.exit_code == 0, r.stderr
            assert r.stdout.splitlines() == ["autzen"]

            schema_json = (
                repo.head_tree / "autzen/.point-cloud-dataset.v1/meta/schema.json"
            )
            assert json.loads(schema_json.data) == {
                "dimensions": [
                    {"name": "X", "size": 8, "type": "floating"},
                    {"name": "Y", "size": 8, "type": "floating"},
                    {"name": "Z", "size": 8, "type": "floating"},
                    {"name": "Intensity", "size": 2, "type": "unsigned"},
                    {"name": "ReturnNumber", "size": 1, "type": "unsigned"},
                    {"name": "NumberOfReturns", "size": 1, "type": "unsigned"},
                    {"name": "ScanDirectionFlag", "size": 1, "type": "unsigned"},
                    {"name": "EdgeOfFlightLine", "size": 1, "type": "unsigned"},
                    {"name": "Classification", "size": 1, "type": "unsigned"},
                    {"name": "ScanAngleRank", "size": 4, "type": "floating"},
                    {"name": "UserData", "size": 1, "type": "unsigned"},
                    {"name": "PointSourceId", "size": 2, "type": "unsigned"},
                    {"name": "GpsTime", "size": 8, "type": "floating"},
                ],
                "CRS": "EPSG:2994",
            }

            r = cli_runner.invoke(["remote", "add", "origin", DUMMY_REPO])
            assert r.exit_code == 0, r.stderr
            repo.config[f"lfs.{DUMMY_REPO}/info/lfs.locksverify"] = False

            stdout = subprocess.check_output(
                ["kart", "lfs", "push", "origin", "--all", "--dry-run"], encoding="utf8"
            )
            assert re.match(
                r"push [0-9a-f]{64} => autzen/.point-cloud-dataset.v1/tiles/e8/autzen.copc.laz",
                stdout.splitlines()[0],
            )


def test_import_several_las(
    tmp_path, chdir, cli_runner, data_archive_readonly, requires_pdal, requires_git_lfs
):
    # Using postgres here because it has the best type preservation
    with data_archive_readonly("point-cloud/auckland.tgz") as auckland:
        repo_path = tmp_path / "point-cloud-repo"
        r = cli_runner.invoke(["init", repo_path])
        assert r.exit_code == 0

        repo = KartRepo(repo_path)
        with chdir(repo_path):
            r = cli_runner.invoke(
                [
                    "point-cloud-import",
                    *glob(f"{auckland}/auckland_*.laz"),
                    "--dataset-path=auckland",
                ]
            )
            assert r.exit_code == 0, r.stderr

            r = cli_runner.invoke(["data", "ls"])
            assert r.exit_code == 0, r.stderr
            assert r.stdout.splitlines() == ["auckland"]

            schema_json = (
                repo.head_tree / "auckland/.point-cloud-dataset.v1/meta/schema.json"
            )
            assert json.loads(schema_json.data) == {
                "dimensions": [
                    {"name": "X", "size": 8, "type": "floating"},
                    {"name": "Y", "size": 8, "type": "floating"},
                    {"name": "Z", "size": 8, "type": "floating"},
                    {"name": "Intensity", "size": 2, "type": "unsigned"},
                    {"name": "ReturnNumber", "size": 1, "type": "unsigned"},
                    {"name": "NumberOfReturns", "size": 1, "type": "unsigned"},
                    {"name": "ScanDirectionFlag", "size": 1, "type": "unsigned"},
                    {"name": "EdgeOfFlightLine", "size": 1, "type": "unsigned"},
                    {"name": "Classification", "size": 1, "type": "unsigned"},
                    {"name": "ScanAngleRank", "size": 4, "type": "floating"},
                    {"name": "UserData", "size": 1, "type": "unsigned"},
                    {"name": "PointSourceId", "size": 2, "type": "unsigned"},
                    {"name": "GpsTime", "size": 8, "type": "floating"},
                    {"name": "Red", "size": 2, "type": "unsigned"},
                    {"name": "Green", "size": 2, "type": "unsigned"},
                    {"name": "Blue", "size": 2, "type": "unsigned"},
                ],
                "CRS": "EPSG:2193",
            }

            r = cli_runner.invoke(["remote", "add", "origin", DUMMY_REPO])
            assert r.exit_code == 0, r.stderr
            repo.config[f"lfs.{DUMMY_REPO}/info/lfs.locksverify"] = False

            stdout = subprocess.check_output(
                ["kart", "lfs", "push", "origin", "--all", "--dry-run"], encoding="utf8"
            )
            lines = stdout.splitlines()
            for i in range(16):
                assert re.match(
                    r"push [0-9a-f]{64} => auckland/.point-cloud-dataset.v1/tiles/[0-9a-f]{2}/auckland_\d_\d.copc.laz",
                    lines[i],
                )


def test_import_mismatched_las(
    tmp_path, chdir, cli_runner, data_archive_readonly, requires_pdal, requires_git_lfs
):
    # Using postgres here because it has the best type preservation
    with data_archive_readonly("point-cloud/auckland.tgz") as auckland:
        with data_archive_readonly("point-cloud/autzen.tgz") as autzen:
            repo_path = tmp_path / "point-cloud-repo"
            r = cli_runner.invoke(["init", repo_path])
            assert r.exit_code == 0
            with chdir(repo_path):
                r = cli_runner.invoke(
                    [
                        "point-cloud-import",
                        *glob(f"{auckland}/auckland_*.laz"),
                        f"{autzen}/autzen.las",
                        "--dataset-path=mixed",
                    ]
                )
                assert r.exit_code == INVALID_FILE_FORMAT
                assert "Non-homogenous" in r.stderr
