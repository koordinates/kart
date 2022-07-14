from glob import glob
import json
import re
import subprocess
import pytest

from kart.exceptions import (
    INVALID_FILE_FORMAT,
    WORKING_COPY_OR_IMPORT_CONFLICT,
)
from kart.repo import KartRepo
from .fixtures import requires_pdal, requires_git_lfs  # noqa

DUMMY_REPO = "git@example.com/example.git"


def count_head_tile_changes(cli_runner, dataset_path):
    r = cli_runner.invoke(["show", "HEAD", "-ojson"])
    assert r.exit_code == 0, r.stderr
    output = json.loads(r.stdout)
    tile_changes = output["kart.diff/v1+hexwkb"][dataset_path]["tile"]
    inserts = len([t for t in tile_changes if "+" in t and "-" not in t])
    updates = len([t for t in tile_changes if "+" in t and "-" in t])
    deletes = len([t for t in tile_changes if "-" in t and "+" not in t])
    return inserts, updates, deletes


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

            r = cli_runner.invoke(["meta", "get", "autzen", "format.json", "-ojson"])
            assert r.exit_code == 0, r.stderr
            assert json.loads(r.stdout) == {
                "autzen": {
                    "format.json": {
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

            r = cli_runner.invoke(["show", "HEAD", "autzen:tile:autzen"])
            assert r.exit_code == 0, r.stderr
            assert r.stdout.splitlines()[4:] == [
                "    Importing 1 LAZ tiles as autzen",
                "",
                "+++ autzen:tile:autzen",
                "+                                     name = autzen.copc.laz",
                "+                              crs84Extent = -123.075389,-123.0625145,44.04998981,44.06229306,407.35,536.84",
                "+                                   format = laz-1.4/copc-1.0",
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
                r"push [0-9a-f]{64} => autzen/.point-cloud-dataset.v1/tile/60/autzen",
                stdout.splitlines()[0],
            )

            assert (repo_path / "autzen" / "autzen.copc.laz").is_file()


@pytest.mark.slow
def test_import_several_laz(
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
                    r"push [0-9a-f]{64} => auckland/.point-cloud-dataset.v1/tile/[0-9a-f]{2}/auckland_\d_\d",
                    lines[i],
                )

            for x in range(4):
                for y in range(4):
                    assert (
                        repo_path / "auckland" / f"auckland_{x}_{y}.copc.laz"
                    ).is_file()


def test_import_single_laz_no_convert(
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
                    f"{auckland}/auckland_0_0.laz",
                    "--message=test_import_single_laz_no_convert",
                    "--dataset-path=auckland",
                    "--preserve-format",
                ]
            )
            assert r.exit_code == 0, r.stderr

            r = cli_runner.invoke(["meta", "get", "auckland", "format.json", "-ojson"])
            assert r.exit_code == 0, r.stderr
            assert json.loads(r.stdout) == {
                "auckland": {
                    "format.json": {
                        "compression": "laz",
                        "lasVersion": "1.2",
                        "pointDataRecordFormat": 3,
                        "pointDataRecordLength": 34,
                    }
                }
            }

            r = cli_runner.invoke(["show", "HEAD", "auckland:tile:auckland_0_0"])
            assert r.exit_code == 0, r.stderr
            assert r.stdout.splitlines()[4:] == [
                "    test_import_single_laz_no_convert",
                "",
                "+++ auckland:tile:auckland_0_0",
                "+                                     name = auckland_0_0.laz",
                "+                              crs84Extent = 174.7382443,174.7496594,-36.85123712,-36.84206322,-1.66,99.83",
                "+                                   format = laz-1.2",
                "+                             nativeExtent = 1754987.85,1755987.77,5920219.76,5921219.64,-1.66,99.83",
                "+                               pointCount = 4231",
                "+                                      oid = sha256:6b980ce4d7f4978afd3b01e39670e2071a792fba441aca45be69be81cb48b08c",
                "+                                     size = 51489",
            ]


def test_import_replace_existing(
    tmp_path,
    chdir,
    cli_runner,
    data_archive,
    data_archive_readonly,
    requires_pdal,
):
    with data_archive_readonly("point-cloud/laz-auckland.tgz") as src:
        with data_archive("point-cloud/auckland.tgz"):
            r = cli_runner.invoke(
                [
                    "point-cloud-import",
                    f"{src}/auckland_0_0.laz",
                    "--message=Import again but don't convert to COPC this time",
                    "--dataset-path=auckland",
                    "--preserve-format",
                    "--replace-existing",
                ]
            )
            assert r.exit_code == 0, r.stderr

            # Originally this dataset was COPC, but now it's changed to LAZ 1.2
            # (because we used --preserve-format)
            r = cli_runner.invoke(["meta", "get", "auckland", "format.json", "-ojson"])
            assert r.exit_code == 0, r.stderr
            assert json.loads(r.stdout) == {
                "auckland": {
                    "format.json": {
                        "compression": "laz",
                        "lasVersion": "1.2",
                        "pointDataRecordFormat": 3,
                        "pointDataRecordLength": 34,
                    }
                }
            }

            # All tiles were replaced with the single tile we imported.
            inserts, updates, deletes = count_head_tile_changes(cli_runner, "auckland")
            assert deletes == 15
            assert inserts == 0
            assert updates == 1


def test_import_delete_tiles_only(
    tmp_path, chdir, cli_runner, data_archive, data_archive_readonly
):
    with data_archive("point-cloud/auckland.tgz"):
        r = cli_runner.invoke(
            [
                "point-cloud-import",
                "--dataset-path=auckland",
                "--delete=auckland_0_0.laz",
            ]
        )
        assert r.exit_code == 0, r.stderr

        # One tile was deleted, no other changes were made
        inserts, updates, deletes = count_head_tile_changes(cli_runner, "auckland")
        assert deletes == 1
        assert inserts == 0
        assert updates == 0


def test_import_delete_tiles_and_import_sources_error(
    tmp_path, chdir, cli_runner, data_archive, data_archive_readonly
):
    with data_archive_readonly("point-cloud/laz-auckland.tgz") as src:
        with data_archive("point-cloud/auckland.tgz"):
            r = cli_runner.invoke(
                [
                    "point-cloud-import",
                    "--dataset-path=auckland",
                    # This doesn't do what you'd think; the glob expands to give the
                    # first argument to the --delete option, and the other args would get imported.
                    # So we don't allow this without an explicit --update-existing flag.
                    "--delete",
                    *glob(f"{src}/auckland_3_*.laz"),
                ]
            )
            assert r.exit_code == WORKING_COPY_OR_IMPORT_CONFLICT, r.stderr
            assert (
                "Dataset path 'auckland' conflicts with existing path 'auckland'"
                in r.stderr
            )


def test_import_conflicting_dataset(
    tmp_path, chdir, cli_runner, data_archive, data_archive_readonly, requires_pdal
):
    with data_archive_readonly("point-cloud/laz-auckland.tgz") as src:
        with data_archive("point-cloud/auckland.tgz"):
            r = cli_runner.invoke(
                [
                    "point-cloud-import",
                    "--dataset-path=auckland",
                    f"{src}/autzen.laz",
                ]
            )
            assert r.exit_code == WORKING_COPY_OR_IMPORT_CONFLICT
            assert (
                r.stderr.strip()
                == "Error: Dataset path 'auckland' conflicts with existing path 'auckland'"
            )


def test_import_update_existing_non_homogenous(
    tmp_path, chdir, cli_runner, data_archive, data_archive_readonly, requires_pdal
):
    with data_archive_readonly("point-cloud/laz-autzen.tgz") as src:
        with data_archive("point-cloud/auckland.tgz"):
            r = cli_runner.invoke(
                [
                    "point-cloud-import",
                    "--dataset-path=auckland",
                    "--update-existing",
                    f"{src}/autzen.laz",
                ]
            )
            assert r.exit_code == WORKING_COPY_OR_IMPORT_CONFLICT, r.stderr
            assert "The input files have more than one file format" in r.stderr


def test_import_update_existing(
    tmp_path, chdir, cli_runner, data_archive, data_archive_readonly, requires_pdal
):
    with data_archive("point-cloud/laz-auckland.tgz") as src:
        (src / "new_tile.laz").symlink_to(src / "auckland_0_0.laz")
        with data_archive("point-cloud/auckland.tgz"):
            r = cli_runner.invoke(
                [
                    "point-cloud-import",
                    "--dataset-path=auckland",
                    "--update-existing",
                    "--convert-to-copc",
                    f"{src}/new_tile.laz",
                    f"{src}/auckland_0_0.laz",
                ]
            )
            assert r.exit_code == 0, r.stderr

            # One tile was added, one was updated, no other changes were made
            inserts, updates, deletes = count_head_tile_changes(cli_runner, "auckland")
            assert deletes == 0
            assert inserts == 1
            assert updates == 1


def test_import_single_las_no_convert(
    tmp_path, chdir, cli_runner, data_archive_readonly, requires_pdal, requires_git_lfs
):
    with data_archive_readonly("point-cloud/las-autzen.tgz") as autzen:
        repo_path = tmp_path / "point-cloud-repo"
        r = cli_runner.invoke(["init", repo_path])
        assert r.exit_code == 0

        with chdir(repo_path):
            r = cli_runner.invoke(
                [
                    "point-cloud-import",
                    f"{autzen}/autzen.las",
                    "--dataset-path=autzen",
                    "--preserve-format",
                ]
            )
            assert r.exit_code == INVALID_FILE_FORMAT
            assert "LAS datasets are not supported" in r.stderr


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
                assert r.exit_code == WORKING_COPY_OR_IMPORT_CONFLICT
                assert "Non-homogenous" in r.stderr
                # This is disallowed even though we are converting to COPC, since these tiles would have different
                # schemas even once converted to COPC.
                assert "schema" in r.stderr
