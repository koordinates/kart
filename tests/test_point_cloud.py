from glob import glob
import json
import re
import shutil
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
    with data_archive_readonly("point-cloud/las-autzen.tgz") as autzen:
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

            r = cli_runner.invoke(["meta", "get", "autzen", "schema.json", "-ojson"])
            assert r.exit_code == 0, r.stderr
            assert json.loads(r.stdout) == {
                "autzen": {
                    "schema.json": {
                        "dimensions": [
                            {"name": "X", "size": 8, "type": "floating"},
                            {"name": "Y", "size": 8, "type": "floating"},
                            {"name": "Z", "size": 8, "type": "floating"},
                            {"name": "Intensity", "size": 2, "type": "unsigned"},
                            {"name": "ReturnNumber", "size": 1, "type": "unsigned"},
                            {"name": "NumberOfReturns", "size": 1, "type": "unsigned"},
                            {
                                "name": "ScanDirectionFlag",
                                "size": 1,
                                "type": "unsigned",
                            },
                            {"name": "EdgeOfFlightLine", "size": 1, "type": "unsigned"},
                            {"name": "Classification", "size": 1, "type": "unsigned"},
                            {"name": "ScanAngleRank", "size": 4, "type": "floating"},
                            {"name": "UserData", "size": 1, "type": "unsigned"},
                            {"name": "PointSourceId", "size": 2, "type": "unsigned"},
                            {"name": "GpsTime", "size": 8, "type": "floating"},
                        ],
                        "CRS": "EPSG:2994",
                    }
                }
            }

            r = cli_runner.invoke(["show", "HEAD", "autzen:tile:autzen.copc.laz"])
            assert r.exit_code == 0, r.stderr
            assert r.stdout.splitlines()[4:] == [
                "    Importing 1 LAZ tiles as autzen",
                "",
                "+++ autzen:tile:autzen.copc.laz",
                "+                                     name = autzen.copc.laz",
                "+                                      oid = sha256:4fc66b29491b8b22fc5deb69da86e588e93e276aa0511460fba6521048081701",
                "+                                     size = 2839",
            ]

            r = cli_runner.invoke(["remote", "add", "origin", DUMMY_REPO])
            assert r.exit_code == 0, r.stderr
            repo.config[f"lfs.{DUMMY_REPO}/info/lfs.locksverify"] = False

            stdout = subprocess.check_output(
                ["kart", "lfs", "push", "origin", "--all", "--dry-run"], encoding="utf8"
            )
            assert re.match(
                r"push [0-9a-f]{64} => autzen/.point-cloud-dataset.v1/tile/e8/autzen.copc.laz",
                stdout.splitlines()[0],
            )

            assert (repo_path / "autzen" / "tiles" / "autzen.copc.laz").is_file()


@pytest.mark.slow
def test_import_several_las(
    tmp_path, chdir, cli_runner, data_archive_readonly, requires_pdal, requires_git_lfs
):
    # Using postgres here because it has the best type preservation
    with data_archive_readonly("point-cloud/laz-auckland.tgz") as auckland:
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

            r = cli_runner.invoke(["meta", "get", "auckland", "schema.json", "-ojson"])
            assert r.exit_code == 0, r.stderr
            assert json.loads(r.stdout) == {
                "auckland": {
                    "schema.json": {
                        "dimensions": [
                            {"name": "X", "size": 8, "type": "floating"},
                            {"name": "Y", "size": 8, "type": "floating"},
                            {"name": "Z", "size": 8, "type": "floating"},
                            {"name": "Intensity", "size": 2, "type": "unsigned"},
                            {"name": "ReturnNumber", "size": 1, "type": "unsigned"},
                            {"name": "NumberOfReturns", "size": 1, "type": "unsigned"},
                            {
                                "name": "ScanDirectionFlag",
                                "size": 1,
                                "type": "unsigned",
                            },
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
                }
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
                    r"push [0-9a-f]{64} => auckland/.point-cloud-dataset.v1/tile/[0-9a-f]{2}/auckland_\d_\d.copc.laz",
                    lines[i],
                )

            for x in range(4):
                for y in range(4):
                    assert (
                        repo_path / "auckland" / "tiles" / f"auckland_{x}_{y}.copc.laz"
                    ).is_file()


def test_import_mismatched_las(
    tmp_path, chdir, cli_runner, data_archive_readonly, requires_pdal, requires_git_lfs
):
    # Using postgres here because it has the best type preservation
    with data_archive_readonly("point-cloud/laz-auckland.tgz") as auckland:
        with data_archive_readonly("point-cloud/las-autzen.tgz") as autzen:
            repo_path = tmp_path / "point-cloud-repo"
            r = cli_runner.invoke(["init", repo_path])
            assert r.exit_code == 0, r.stderr
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


def test_working_copy_edit(cli_runner, data_working_copy, monkeypatch):
    monkeypatch.setenv("X_KART_POINT_CLOUDS", "1")

    # TODO - remove Kart's requirement for a GPKG working copy
    with data_working_copy("point-cloud/auckland.tgz") as (repo_path, wc_path):
        r = cli_runner.invoke(["diff"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == []

        tiles_path = repo_path / "auckland" / "tiles"
        assert tiles_path.is_dir()

        shutil.copy(
            tiles_path / "auckland_0_0.copc.laz", tiles_path / "auckland_1_1.copc.laz"
        )
        # TODO - add rename detection.
        (tiles_path / "auckland_3_3.copc.laz").rename(
            tiles_path / "auckland_4_4.copc.laz"
        )

        r = cli_runner.invoke(["diff"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            "--- auckland:tile:auckland_1_1.copc.laz",
            "+++ auckland:tile:auckland_1_1.copc.laz",
            "-                                      oid = sha256:1d9934b50ebd057d893281813fda8deffb0ad03b3c354ec1c5d7557134c40be1",
            "+                                      oid = sha256:dafd2ed5671190433ca1e7cea364a94d9e00c11f0a7b3927ce93554df5b1cd5c",
            "-                                     size = 23570",
            "+                                     size = 68665",
            "--- auckland:tile:auckland_3_3.copc.laz",
            "-                                     name = auckland_3_3.copc.laz",
            "-                                      oid = sha256:522ef2ff7f66b51516021cde1fa7b9f301acde6713772958d6f1303fdac40c25",
            "-                                     size = 1334",
            "+++ auckland:tile:auckland_4_4.copc.laz",
            "+                                     name = auckland_4_4.copc.laz",
            "+                                      oid = sha256:522ef2ff7f66b51516021cde1fa7b9f301acde6713772958d6f1303fdac40c25",
            "+                                     size = 1334",
        ]

        r = cli_runner.invoke(["commit", "-m", "Edit point cloud tiles"])
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["show"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines()[4:] == [
            "    Edit point cloud tiles",
            "",
            "--- auckland:tile:auckland_1_1.copc.laz",
            "+++ auckland:tile:auckland_1_1.copc.laz",
            "-                                      oid = sha256:1d9934b50ebd057d893281813fda8deffb0ad03b3c354ec1c5d7557134c40be1",
            "+                                      oid = sha256:dafd2ed5671190433ca1e7cea364a94d9e00c11f0a7b3927ce93554df5b1cd5c",
            "-                                     size = 23570",
            "+                                     size = 68665",
            "--- auckland:tile:auckland_3_3.copc.laz",
            "-                                     name = auckland_3_3.copc.laz",
            "-                                      oid = sha256:522ef2ff7f66b51516021cde1fa7b9f301acde6713772958d6f1303fdac40c25",
            "-                                     size = 1334",
            "+++ auckland:tile:auckland_4_4.copc.laz",
            "+                                     name = auckland_4_4.copc.laz",
            "+                                      oid = sha256:522ef2ff7f66b51516021cde1fa7b9f301acde6713772958d6f1303fdac40c25",
            "+                                     size = 1334",
        ]

        r = cli_runner.invoke(["diff"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == []
