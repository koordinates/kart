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

            r = cli_runner.invoke(["meta", "get", "autzen", "fileInfo.json", "-ojson"])
            assert r.exit_code == 0, r.stderr
            assert json.loads(r.stdout) == {
                "autzen": {
                    "fileInfo.json": {
                        "compression": "laz",
                        "lasVersion": "1.4",
                        "optimization": "copc",
                        "optimizationVersion": "1.0",
                        "pointDataRecordFormat": 6,
                        "pointDataRecordLength": 30,
                    }
                }
            }

            r = cli_runner.invoke(["meta", "get", "autzen", "schema.json", "-ojson"])
            assert r.exit_code == 0, r.stderr
            assert json.loads(r.stdout) == {
                "autzen": {
                    "schema.json": [
                        {"name": "X", "dataType": "float", "size": 64},
                        {"name": "Y", "dataType": "float", "size": 64},
                        {"name": "Z", "dataType": "float", "size": 64},
                        {"name": "Intensity", "dataType": "integer", "size": 16},
                        {"name": "ReturnNumber", "dataType": "integer", "size": 8},
                        {"name": "NumberOfReturns", "dataType": "integer", "size": 8},
                        {"name": "ScanDirectionFlag", "dataType": "integer", "size": 8},
                        {"name": "EdgeOfFlightLine", "dataType": "integer", "size": 8},
                        {"name": "Classification", "dataType": "integer", "size": 8},
                        {"name": "ScanAngleRank", "dataType": "float", "size": 32},
                        {"name": "UserData", "dataType": "integer", "size": 8},
                        {"name": "PointSourceId", "dataType": "integer", "size": 16},
                        {"name": "GpsTime", "dataType": "float", "size": 64},
                        {"name": "ScanChannel", "dataType": "integer", "size": 8},
                        {"name": "ClassFlags", "dataType": "integer", "size": 8},
                    ]
                }
            }

            r = cli_runner.invoke(["show", "HEAD", "autzen:tile:autzen.copc.laz"])
            assert r.exit_code == 0, r.stderr
            assert r.stdout.splitlines()[4:] == [
                "    Importing 1 LAZ tiles as autzen",
                "",
                "+++ autzen:tile:autzen.copc.laz",
                "+                                     name = autzen.copc.laz",
                "+                              crs84Extent = -123.075389,-123.0625145,44.04998981,44.06229306,407.35,536.84",
                "+                             nativeExtent = 635616.31,638864.6,848977.79,853362.37,407.35,536.84",
                "+                               pointCount = 106",
                "+                                      oid = sha256:213ef4211ba375e2eec60aa61b6c230d1a3d1498b8fcc39150fd3040ee8f0512",
                "+                                     size = 3607",
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

            assert (repo_path / "autzen" / "autzen.copc.laz").is_file()


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
                    "schema.json": [
                        {"name": "X", "dataType": "float", "size": 64},
                        {"name": "Y", "dataType": "float", "size": 64},
                        {"name": "Z", "dataType": "float", "size": 64},
                        {"name": "Intensity", "dataType": "integer", "size": 16},
                        {"name": "ReturnNumber", "dataType": "integer", "size": 8},
                        {"name": "NumberOfReturns", "dataType": "integer", "size": 8},
                        {"name": "ScanDirectionFlag", "dataType": "integer", "size": 8},
                        {"name": "EdgeOfFlightLine", "dataType": "integer", "size": 8},
                        {"name": "Classification", "dataType": "integer", "size": 8},
                        {"name": "ScanAngleRank", "dataType": "float", "size": 32},
                        {"name": "UserData", "dataType": "integer", "size": 8},
                        {"name": "PointSourceId", "dataType": "integer", "size": 16},
                        {"name": "GpsTime", "dataType": "float", "size": 64},
                        {"name": "ScanChannel", "dataType": "integer", "size": 8},
                        {"name": "ClassFlags", "dataType": "integer", "size": 8},
                        {"name": "Red", "dataType": "integer", "size": 16},
                        {"name": "Green", "dataType": "integer", "size": 16},
                        {"name": "Blue", "dataType": "integer", "size": 16},
                    ]
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
                        repo_path / "auckland" / f"auckland_{x}_{y}.copc.laz"
                    ).is_file()


def test_import_no_convert(
    tmp_path, chdir, cli_runner, data_archive_readonly, requires_pdal, requires_git_lfs
):
    with data_archive_readonly("point-cloud/laz-auckland.tgz") as auckland:
        repo_path = tmp_path / "point-cloud-repo"
        r = cli_runner.invoke(["init", repo_path])
        assert r.exit_code == 0

        with chdir(repo_path):
            r = cli_runner.invoke(
                [
                    "point-cloud-import",
                    *glob(f"{auckland}/auckland_0_0.laz"),
                    "--dataset-path=auckland",
                    "--no-convert-to-copc",
                ]
            )
            assert r.exit_code == 0, r.stderr

            r = cli_runner.invoke(["show", "HEAD", "auckland:tile:auckland_0_0.laz"])
            assert r.exit_code == 0, r.stderr
            assert r.stdout.splitlines()[4:] == [
                "    Importing 1 LAZ tiles as auckland",
                "",
                "+++ auckland:tile:auckland_0_0.laz",
                "+                                     name = auckland_0_0.laz",
                "+                              crs84Extent = 174.7382443,174.7496594,-36.85123712,-36.84206322,-1.66,99.83",
                "+                             nativeExtent = 1754987.85,1755987.77,5920219.76,5921219.64,-1.66,99.83",
                "+                               pointCount = 4231",
                "+                                      oid = sha256:6b980ce4d7f4978afd3b01e39670e2071a792fba441aca45be69be81cb48b08c",
                "+                                     size = 51489",
            ]


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


def test_working_copy_edit(cli_runner, data_archive, monkeypatch, requires_pdal):
    monkeypatch.setenv("X_KART_POINT_CLOUDS", "1")

    with data_archive("point-cloud/auckland.tgz") as repo_path:
        r = cli_runner.invoke(["diff"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == []

        tiles_path = repo_path / "auckland"
        assert tiles_path.is_dir()

        shutil.copy(
            tiles_path / "auckland_0_0.copc.laz", tiles_path / "auckland_1_1.copc.laz"
        )
        # TODO - add rename detection.
        (tiles_path / "auckland_3_3.copc.laz").rename(
            tiles_path / "auckland_4_4.copc.laz"
        )

        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            "On branch main",
            "",
            "Changes in working copy:",
            '  (use "kart commit" to commit)',
            '  (use "kart restore" to discard changes)',
            "",
            "  auckland:",
            "    tile:",
            "      1 inserts",
            "      1 updates",
            "      1 deletes",
        ]

        r = cli_runner.invoke(["diff"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            "--- auckland:tile:auckland_1_1.copc.laz",
            "+++ auckland:tile:auckland_1_1.copc.laz",
            "-                              crs84Extent = 174.7492629,174.7606572,-36.84205419,-36.83288872,-1.48,35.15",
            "+                              crs84Extent = 174.7382443,174.7496594,-36.85123712,-36.84206322,-1.66,99.83",
            "-                             nativeExtent = 1755989.03,1756987.13,5921220.62,5922219.49,-1.48,35.15",
            "+                             nativeExtent = 1754987.85,1755987.77,5920219.76,5921219.64,-1.66,99.83",
            "-                               pointCount = 1558",
            "+                               pointCount = 4231",
            "-                                      oid = sha256:a6ca999e8e2c317b0ef3d217205b631b6f77d5eaa53c00c738ffc39fbbbf1748",
            "+                                      oid = sha256:a036c877bbdcaaa9d14e19f0e23154cf786b3b353a51565fedef52e3ab25cf80",
            "-                                     size = 24517",
            "+                                     size = 69593",
            "--- auckland:tile:auckland_3_3.copc.laz",
            "-                                     name = auckland_3_3.copc.laz",
            "-                              crs84Extent = 174.7726418,174.7819673,-36.82369125,-36.82346553,-1.28,9.8",
            "-                             nativeExtent = 1758093.46,1758925.34,5923219.8,5923229.38,-1.28,9.8",
            "-                               pointCount = 29",
            "-                                      oid = sha256:64895828ea03ce9cafaef4f387338aab8d498c8eccaef1503b8b3bd97e57c5a3",
            "-                                     size = 2319",
            "+++ auckland:tile:auckland_4_4.copc.laz",
            "+                                     name = auckland_4_4.copc.laz",
            "+                              crs84Extent = 174.7726418,174.7819673,-36.82369125,-36.82346553,-1.28,9.8",
            "+                             nativeExtent = 1758093.46,1758925.34,5923219.8,5923229.38,-1.28,9.8",
            "+                               pointCount = 29",
            "+                                      oid = sha256:64895828ea03ce9cafaef4f387338aab8d498c8eccaef1503b8b3bd97e57c5a3",
            "+                                     size = 2319",
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
            "-                              crs84Extent = 174.7492629,174.7606572,-36.84205419,-36.83288872,-1.48,35.15",
            "+                              crs84Extent = 174.7382443,174.7496594,-36.85123712,-36.84206322,-1.66,99.83",
            "-                             nativeExtent = 1755989.03,1756987.13,5921220.62,5922219.49,-1.48,35.15",
            "+                             nativeExtent = 1754987.85,1755987.77,5920219.76,5921219.64,-1.66,99.83",
            "-                               pointCount = 1558",
            "+                               pointCount = 4231",
            "-                                      oid = sha256:a6ca999e8e2c317b0ef3d217205b631b6f77d5eaa53c00c738ffc39fbbbf1748",
            "+                                      oid = sha256:a036c877bbdcaaa9d14e19f0e23154cf786b3b353a51565fedef52e3ab25cf80",
            "-                                     size = 24517",
            "+                                     size = 69593",
            "--- auckland:tile:auckland_3_3.copc.laz",
            "-                                     name = auckland_3_3.copc.laz",
            "-                              crs84Extent = 174.7726418,174.7819673,-36.82369125,-36.82346553,-1.28,9.8",
            "-                             nativeExtent = 1758093.46,1758925.34,5923219.8,5923229.38,-1.28,9.8",
            "-                               pointCount = 29",
            "-                                      oid = sha256:64895828ea03ce9cafaef4f387338aab8d498c8eccaef1503b8b3bd97e57c5a3",
            "-                                     size = 2319",
            "+++ auckland:tile:auckland_4_4.copc.laz",
            "+                                     name = auckland_4_4.copc.laz",
            "+                              crs84Extent = 174.7726418,174.7819673,-36.82369125,-36.82346553,-1.28,9.8",
            "+                             nativeExtent = 1758093.46,1758925.34,5923219.8,5923229.38,-1.28,9.8",
            "+                               pointCount = 29",
            "+                                      oid = sha256:64895828ea03ce9cafaef4f387338aab8d498c8eccaef1503b8b3bd97e57c5a3",
            "+                                     size = 2319",
        ]

        r = cli_runner.invoke(["show", "-ojson"])
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["show", "-ojson-lines"])
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["diff"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == []


def test_working_copy_restore_reset(
    cli_runner, data_archive, monkeypatch, requires_pdal
):
    monkeypatch.setenv("X_KART_POINT_CLOUDS", "1")

    def file_count(path):
        return len(list(path.iterdir()))

    with data_archive("point-cloud/auckland.tgz") as repo_path:
        repo = KartRepo(repo_path)

        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines()[-1] == "Nothing to commit, working copy clean"

        tiles_path = repo_path / "auckland"
        assert tiles_path.is_dir()
        assert file_count(tiles_path) == 16

        for tile in tiles_path.glob("auckland_0_*.copc.laz"):
            tile.unlink()

        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r.stderr
        assert "4 deletes" in r.stdout.splitlines()[-1]

        r = cli_runner.invoke(["commit", "-m", "4 deletes"])
        assert r.exit_code == 0, r.stderr
        assert file_count(tiles_path) == 12
        edit_commit = repo.head_commit.hex

        r = cli_runner.invoke(
            ["restore", "-s", "HEAD^", "auckland:tile:auckland_0_0.copc.laz"]
        )
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r.stderr
        assert "1 insert" in r.stdout.splitlines()[-1]
        assert file_count(tiles_path) == 13

        r = cli_runner.invoke(["reset", "HEAD^", "--discard-changes"])
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines()[-1] == "Nothing to commit, working copy clean"
        assert file_count(tiles_path) == 16

        r = cli_runner.invoke(
            [
                "restore",
                "-s",
                edit_commit,
                "auckland:tile:auckland_0_1.copc.laz",
                "auckland:tile:auckland_0_2.copc.laz",
            ]
        )
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r.stderr
        assert "2 deletes" in r.stdout.splitlines()[-1]
        assert file_count(tiles_path) == 14

        r = cli_runner.invoke(["reset", edit_commit, "--discard-changes"])
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines()[-1] == "Nothing to commit, working copy clean"
        assert file_count(tiles_path) == 12
