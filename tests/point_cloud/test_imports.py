from glob import glob
import json
import shutil
import subprocess
import textwrap

import pytest


from kart.exceptions import (
    INVALID_FILE_FORMAT,
    WORKING_COPY_OR_IMPORT_CONFLICT,
    UNCOMMITTED_CHANGES,
    NO_CHANGES,
)
from kart.repo import KartRepo

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


def test_import_single_las__convert(
    tmp_path,
    chdir,
    cli_runner,
    data_archive_readonly,
    check_lfs_hashes,
    requires_pdal,
    requires_git_lfs,
):
    with data_archive_readonly("point-cloud/las-autzen.tgz") as autzen:
        repo_path = tmp_path / "point-cloud-repo"
        r = cli_runner.invoke(["init", repo_path])
        assert r.exit_code == 0, r.stderr

        repo = KartRepo(repo_path)
        with chdir(repo_path):
            r = cli_runner.invoke(
                [
                    "point-cloud-import",
                    f"{autzen}/autzen.las",
                    "--dataset-path=autzen",
                    "--convert-to-copc",
                ]
            )
            assert r.exit_code == 0, r.stderr

            check_lfs_hashes(repo, 1)

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
                        {"name": "X", "dataType": "integer", "size": 32},
                        {"name": "Y", "dataType": "integer", "size": 32},
                        {"name": "Z", "dataType": "integer", "size": 32},
                        {
                            "name": "Intensity",
                            "dataType": "integer",
                            "size": 16,
                            "unsigned": True,
                        },
                        {
                            "name": "Return Number",
                            "dataType": "integer",
                            "size": 4,
                            "unsigned": True,
                        },
                        {
                            "name": "Number of Returns",
                            "dataType": "integer",
                            "size": 4,
                            "unsigned": True,
                        },
                        {"name": "Synthetic", "dataType": "integer", "size": 1},
                        {"name": "Key-Point", "dataType": "integer", "size": 1},
                        {"name": "Withheld", "dataType": "integer", "size": 1},
                        {"name": "Overlap", "dataType": "integer", "size": 1},
                        {
                            "name": "Scanner Channel",
                            "dataType": "integer",
                            "size": 2,
                            "unsigned": True,
                        },
                        {
                            "name": "Scan Direction Flag",
                            "dataType": "integer",
                            "size": 1,
                        },
                        {
                            "name": "Edge of Flight Line",
                            "dataType": "integer",
                            "size": 1,
                        },
                        {
                            "name": "Classification",
                            "dataType": "integer",
                            "size": 8,
                            "unsigned": True,
                        },
                        {
                            "name": "User Data",
                            "dataType": "integer",
                            "size": 8,
                            "unsigned": True,
                        },
                        {"name": "Scan Angle", "dataType": "integer", "size": 16},
                        {
                            "name": "Point Source ID",
                            "dataType": "integer",
                            "size": 16,
                            "unsigned": True,
                        },
                        {"name": "GPS Time", "dataType": "float", "size": 64},
                    ]
                }
            }

            r = cli_runner.invoke(["show", "HEAD", "autzen:tile:autzen"])
            assert r.exit_code == 0, r.stderr
            # The [4:-2] slice chops off:
            # * the commit hash and date, they change every time
            # * the oid and size; they're nondeterministic after conversion.
            assert r.stdout.splitlines()[4:-2] == [
                "    Importing 1 LAZ tiles as autzen",
                "",
                "+++ autzen:tile:autzen",
                "+                                     name = autzen.copc.laz",
                "+                              crs84Extent = POLYGON((-123.0748659 44.0499898,-123.0753890 44.0620142,-123.0630351 44.0622931,-123.0625145 44.0502686,-123.0748659 44.0499898))",
                "+                                   format = laz-1.4/copc-1.0",
                "+                             nativeExtent = 635616.31,638864.6,848977.79,853362.37,407.35,536.84",
                "+                               pointCount = 106",
                "+                                sourceOid = sha256:068a349959a45957184606a0442f8dd69aef24543e11963bc63835301df532f5",
            ]

            r = cli_runner.invoke(["remote", "add", "origin", DUMMY_REPO])
            assert r.exit_code == 0, r.stderr
            repo.config[f"lfs.{DUMMY_REPO}/info/lfs.locksverify"] = False

            head_sha = repo.head_commit.hex
            stdout = subprocess.check_output(
                ["kart", "lfs+", "pre-push", "origin", "DUMMY_REPO", "--dry-run"],
                input=f"main {head_sha} main 0000000000000000000000000000000000000000\n",
                encoding="utf8",
            )
            assert (
                stdout.splitlines()[0]
                == "Running pre-push with --dry-run: found 1 LFS blob (3.5KiB) to push"
            )

            assert (repo_path / "autzen" / "autzen.copc.laz").is_file()


@pytest.mark.slow
@pytest.mark.parametrize("command", ["point-cloud-import", "import"])
def test_import_several_laz__convert(
    command,
    tmp_path,
    chdir,
    cli_runner,
    data_archive_readonly,
    check_lfs_hashes,
    requires_pdal,
    requires_git_lfs,
    check_tile_is_reflinked,
):
    with data_archive_readonly("point-cloud/laz-auckland.tgz") as auckland:
        repo_path = tmp_path / "point-cloud-repo"
        r = cli_runner.invoke(["init", repo_path])
        assert r.exit_code == 0

        repo = KartRepo(repo_path)
        with chdir(repo_path):
            r = cli_runner.invoke(
                [
                    command,
                    *glob(f"{auckland}/auckland_*.laz"),
                    "--dataset-path=auckland",
                    "--convert-to-copc",
                ]
            )
            assert r.exit_code == 0, r.stderr

            check_lfs_hashes(repo, 16)

            r = cli_runner.invoke(["data", "ls"])
            assert r.exit_code == 0, r.stderr
            assert r.stdout.splitlines() == ["auckland"]

            r = cli_runner.invoke(["meta", "get", "auckland", "schema.json", "-ojson"])
            assert r.exit_code == 0, r.stderr
            assert json.loads(r.stdout) == {
                "auckland": {
                    "schema.json": [
                        {"name": "X", "dataType": "integer", "size": 32},
                        {"name": "Y", "dataType": "integer", "size": 32},
                        {"name": "Z", "dataType": "integer", "size": 32},
                        {
                            "name": "Intensity",
                            "dataType": "integer",
                            "size": 16,
                            "unsigned": True,
                        },
                        {
                            "name": "Return Number",
                            "dataType": "integer",
                            "size": 4,
                            "unsigned": True,
                        },
                        {
                            "name": "Number of Returns",
                            "dataType": "integer",
                            "size": 4,
                            "unsigned": True,
                        },
                        {"name": "Synthetic", "dataType": "integer", "size": 1},
                        {"name": "Key-Point", "dataType": "integer", "size": 1},
                        {"name": "Withheld", "dataType": "integer", "size": 1},
                        {"name": "Overlap", "dataType": "integer", "size": 1},
                        {
                            "name": "Scanner Channel",
                            "dataType": "integer",
                            "size": 2,
                            "unsigned": True,
                        },
                        {
                            "name": "Scan Direction Flag",
                            "dataType": "integer",
                            "size": 1,
                        },
                        {
                            "name": "Edge of Flight Line",
                            "dataType": "integer",
                            "size": 1,
                        },
                        {
                            "name": "Classification",
                            "dataType": "integer",
                            "size": 8,
                            "unsigned": True,
                        },
                        {
                            "name": "User Data",
                            "dataType": "integer",
                            "size": 8,
                            "unsigned": True,
                        },
                        {"name": "Scan Angle", "dataType": "integer", "size": 16},
                        {
                            "name": "Point Source ID",
                            "dataType": "integer",
                            "size": 16,
                            "unsigned": True,
                        },
                        {"name": "GPS Time", "dataType": "float", "size": 64},
                        {
                            "name": "Red",
                            "dataType": "integer",
                            "size": 16,
                            "unsigned": True,
                        },
                        {
                            "name": "Green",
                            "dataType": "integer",
                            "size": 16,
                            "unsigned": True,
                        },
                        {
                            "name": "Blue",
                            "dataType": "integer",
                            "size": 16,
                            "unsigned": True,
                        },
                    ]
                }
            }

            r = cli_runner.invoke(["remote", "add", "origin", DUMMY_REPO])
            assert r.exit_code == 0, r.stderr
            repo.config[f"lfs.{DUMMY_REPO}/info/lfs.locksverify"] = False

            head_sha = repo.head_commit.hex
            stdout = subprocess.check_output(
                ["kart", "lfs+", "pre-push", "origin", "DUMMY_REPO", "--dry-run"],
                input=f"main {head_sha} main 0000000000000000000000000000000000000000\n",
                encoding="utf8",
            )
            assert stdout.splitlines()[0].startswith(
                "Running pre-push with --dry-run: found 16 LFS blobs"
            )

            for x in range(4):
                for y in range(4):
                    assert (
                        repo_path / "auckland" / f"auckland_{x}_{y}.copc.laz"
                    ).is_file()
                    check_tile_is_reflinked(
                        repo_path / "auckland" / f"auckland_{x}_{y}.copc.laz", repo
                    )


@pytest.mark.parametrize("command", ["point-cloud-import", "import"])
def test_import_single_laz__no_convert(
    command,
    tmp_path,
    chdir,
    cli_runner,
    data_archive_readonly,
    check_lfs_hashes,
    requires_pdal,
    requires_git_lfs,
):
    with data_archive_readonly("point-cloud/laz-auckland.tgz") as auckland:
        repo_path = tmp_path / "point-cloud-repo"
        r = cli_runner.invoke(["init", repo_path])
        assert r.exit_code == 0

        with chdir(repo_path):
            r = cli_runner.invoke(
                [
                    command,
                    f"{auckland}/auckland_0_0.laz",
                    "--message=test_import_single_laz_no_convert",
                    "--dataset-path=auckland",
                    "--preserve-format",
                ]
            )
            assert r.exit_code == 0, r.stderr

            check_lfs_hashes(KartRepo(repo_path), 1)

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
                "+                              crs84Extent = POLYGON((174.7384483 -36.8512371,174.7382443 -36.8422277,174.7494540 -36.8420632,174.7496594 -36.8510726,174.7384483 -36.8512371))",
                "+                                   format = laz-1.2",
                "+                             nativeExtent = 1754987.85,1755987.77,5920219.76,5921219.64,-1.66,99.83",
                "+                               pointCount = 4231",
                "+                                      oid = sha256:6b980ce4d7f4978afd3b01e39670e2071a792fba441aca45be69be81cb48b08c",
                "+                                     size = 51489",
            ]


@pytest.mark.parametrize("command", ["point-cloud-import", "import"])
def test_import_replace_existing(
    command,
    cli_runner,
    data_archive,
    data_archive_readonly,
    requires_pdal,
):
    with data_archive_readonly("point-cloud/laz-auckland.tgz") as src:
        with data_archive("point-cloud/auckland.tgz"):
            r = cli_runner.invoke(
                [
                    command,
                    f"{src}/auckland_0_0.laz",
                    "--message=Import again but don't convert to COPC this time",
                    "--dataset-path=auckland",
                    "--preserve-format",
                    "--replace-existing",
                ]
            )
            assert r.exit_code == 0, r.stderr

            # Originally this dataset was COPC, but now it"s changed to LAZ 1.2
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


def test_import_delete_tiles_only(cli_runner, data_archive):
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
    cli_runner, data_archive, data_archive_readonly
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
    cli_runner, data_archive, data_archive_readonly, requires_pdal
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
    cli_runner, data_archive, data_archive_readonly, requires_pdal
):
    with data_archive_readonly("point-cloud/laz-autzen.tgz") as src:
        with data_archive("point-cloud/auckland.tgz"):
            r = cli_runner.invoke(
                [
                    "point-cloud-import",
                    "--dataset-path=auckland",
                    "--update-existing",
                    "--convert-to-copc",
                    f"{src}/autzen.laz",
                ]
            )
            assert r.exit_code == WORKING_COPY_OR_IMPORT_CONFLICT, r.stderr
            assert "The imported files would have more than one schema" in r.stderr


def test_import_update_existing_with_dirty_workingcopy(cli_runner, data_archive):
    with data_archive("point-cloud/auckland.tgz") as repo_path:
        # make any workingcopy change
        (repo_path / "auckland" / "auckland_1_1.copc.laz").unlink()
        # then try any import which changes an existing dataset
        r = cli_runner.invoke(
            [
                "point-cloud-import",
                "--dataset-path=auckland",
                "--delete=auckland_0_0.laz",
            ]
        )
        assert r.exit_code == UNCOMMITTED_CHANGES, r.stderr
        assert "You have uncommitted changes in your working copy." in r.stderr


def test_import_amend(cli_runner, data_archive):
    with data_archive("point-cloud/auckland.tgz"):
        # Originally, 16 tiles were imported
        inserts, updates, deletes = count_head_tile_changes(cli_runner, "auckland")
        assert inserts == 16
        assert updates == 0
        assert deletes == 0

        r = cli_runner.invoke(
            [
                "point-cloud-import",
                "--dataset-path=auckland",
                "--amend",
                "--delete=auckland_0_0.laz",
            ]
        )
        assert r.exit_code == 0, r.stderr

        # Since we deleted one in the same commit, now only 15 tiles were imported
        inserts, updates, deletes = count_head_tile_changes(cli_runner, "auckland")
        assert inserts == 15
        assert updates == 0
        assert deletes == 0


def test_import_update_existing(cli_runner, data_archive, requires_pdal):
    with data_archive("point-cloud/laz-auckland.tgz") as src:
        (src / "auckland_0_1.laz").unlink()
        shutil.copy(src / "auckland_0_0.laz", src / "auckland_0_1.laz")
        shutil.copy(src / "auckland_0_0.laz", src / "new_tile.laz")
        with data_archive("point-cloud/auckland.tgz"):
            r = cli_runner.invoke(
                [
                    "point-cloud-import",
                    "--dataset-path=auckland",
                    "--update-existing",
                    "--convert-to-copc",
                    f"{src}/auckland_0_0.laz",
                    f"{src}/auckland_0_1.laz",
                    f"{src}/new_tile.laz",
                    "--delete",
                    f"{src}/auckland_0_2.laz",
                ]
            )
            assert r.exit_code == 0, r.stderr

            # One tile was added, one was updated, and one was deleted.
            # (One tile was updated but the new version was the same as the old, so it's not counted.)
            inserts, updates, deletes = count_head_tile_changes(cli_runner, "auckland")
            assert inserts == 1
            assert updates == 1
            assert deletes == 1


def test_import_update_existing_homogenous_crs(
    cli_runner, data_archive_readonly, requires_pdal, chdir, tmp_path
):
    """
    Regression test for https://github.com/koordinates/kart/issues/973
    """
    EXPECTED_CRS_WKT = textwrap.dedent(
        """\
        COMPD_CS["NAD83(2011) / UTM zone 12N + NAVD88 height",
            PROJCS["NAD83(2011) / UTM zone 12N",
                GEOGCS["NAD83(2011)",
                    DATUM["NAD83_National_Spatial_Reference_System_2011",
                        SPHEROID["GRS 1980", 6378137, 298.257222101,
                            AUTHORITY["EPSG", "7019"]],
                        AUTHORITY["EPSG", "1116"]],
                    PRIMEM["Greenwich", 0,
                        AUTHORITY["EPSG", "8901"]],
                    UNIT["degree", 0.0174532925199433,
                        AUTHORITY["EPSG", "9122"]],
                    AUTHORITY["EPSG", "6318"]],
                PROJECTION["Transverse_Mercator"],
                PARAMETER["latitude_of_origin", 0],
                PARAMETER["central_meridian", -111],
                PARAMETER["scale_factor", 0.9996],
                PARAMETER["false_easting", 500000],
                PARAMETER["false_northing", 0],
                UNIT["meter", 1,
                    AUTHORITY["EPSG", "9001"]],
                AXIS["X", EAST],
                AXIS["Y", NORTH],
                AUTHORITY["EPSG", "6341"]],
            VERT_CS["NAVD88 height",
                VERT_DATUM["North American Vertical Datum 1988", 2005,
                    AUTHORITY["EPSG", "5103"]],
                UNIT["meter", 1,
                    AUTHORITY["EPSG", "9001"]],
                AXIS["Up", UP],
                AUTHORITY["EPSG", "5703"]]]
        """
    )

    with data_archive_readonly("point-cloud/laz-utah.tgz") as src:
        repo_path = tmp_path / "point-cloud-repo"
        r = cli_runner.invoke(["init", repo_path])
        with chdir(repo_path):
            # Import a single tile
            r = cli_runner.invoke(
                [
                    "point-cloud-import",
                    "--dataset-path=utah",
                    "--convert-to-copc",
                    str(src / "utah_1.laz"),
                ]
            )
            assert r.exit_code == 0, r.stderr
            r = cli_runner.invoke(["meta", "get", "utah", "crs.wkt", "-ojson"])
            crs_wkt = json.loads(r.stdout)["utah"]["crs.wkt"]
            assert crs_wkt == EXPECTED_CRS_WKT

            # Import a second tile. It should work since they have the same CRS
            r = cli_runner.invoke(
                [
                    "point-cloud-import",
                    "--dataset-path=utah",
                    "--convert-to-copc",
                    "--update-existing",
                    str(src / "utah_2.laz"),
                ]
            )
            assert r.exit_code == 0, r.stderr


def test_import_replace_existing_with_no_changes(
    cli_runner, data_archive, requires_pdal
):
    with data_archive("point-cloud/laz-auckland.tgz") as src:
        with data_archive("point-cloud/auckland.tgz"):
            r = cli_runner.invoke(
                [
                    "point-cloud-import",
                    "--dataset-path=auckland",
                    "--replace-existing",
                    "--convert-to-copc",
                    *glob(f"{src}/*.laz"),
                ]
            )
            assert r.exit_code == NO_CHANGES, r.stderr


def test_import_empty_commit_error(cli_runner, data_archive, requires_pdal):
    with data_archive("point-cloud/laz-auckland.tgz") as src:
        with data_archive("point-cloud/auckland.tgz"):
            # Update an existing tile from the same source (ie no changes)
            r = cli_runner.invoke(
                [
                    "point-cloud-import",
                    "--dataset-path=auckland",
                    "--update-existing",
                    "--convert-to-copc",
                    f"{src}/auckland_0_0.laz",
                ]
            )
            assert r.exit_code == NO_CHANGES, r.stderr
            assert "No changes to commit" in r.stderr


def test_import_single_las__no_convert(
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


def test_import_convert_to_copc_mismatched_CRS(
    tmp_path, chdir, cli_runner, data_archive_readonly, requires_pdal, requires_git_lfs
):
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
                        "--convert-to-copc",
                    ]
                )
                assert r.exit_code == WORKING_COPY_OR_IMPORT_CONFLICT
                assert "Non-homogenous" in r.stderr
                # This is disallowed even though we are converting to COPC, since these tiles have and will
                # continue to have different CRSs when converted to COPC
                assert "The input files have more than one CRS:" in r.stderr


def test_import_convert_to_copc_mismatched_schema(
    tmp_path, chdir, cli_runner, data_archive_readonly, requires_pdal, requires_git_lfs
):
    from kart.point_cloud import pdal_execute_pipeline

    with data_archive_readonly("point-cloud/las-autzen.tgz") as autzen:
        pipeline = [
            {"type": "readers.las", "filename": f"{autzen}/autzen.las"},
            {
                "type": "writers.las",
                "filename": f"{tmp_path}/converted.laz",
                "forward": "all",
                "compression": True,
                "major_version": 1,
                "minor_version": 4,
                "dataformat_id": 7,
            },
        ]
        pdal_execute_pipeline(pipeline)
        assert (tmp_path / "converted.laz").is_file()

        repo_path = tmp_path / "point-cloud-repo"
        r = cli_runner.invoke(["init", repo_path])
        assert r.exit_code == 0, r.stderr
        with chdir(repo_path):
            r = cli_runner.invoke(
                [
                    "point-cloud-import",
                    f"{autzen}/autzen.las",
                    f"{tmp_path}/converted.laz",
                    "--dataset-path=mixed",
                    "--convert-to-copc",
                ]
            )
            assert r.exit_code == WORKING_COPY_OR_IMPORT_CONFLICT
            assert "Non-homogenous" in r.stderr
            # This is disallowed even though we are converting to COPC, since these tiles would have different
            # schemas even once converted to COPC.
            assert "The imported files would have more than one schema:" in r.stderr


def test_import_extra_bytes_vlr__no_convert(
    data_archive_readonly, tmp_path, cli_runner, chdir
):
    with data_archive_readonly("point-cloud/laz-extrabytesvlr.tgz") as extrabytes:
        repo_path = tmp_path / "point-cloud-repo"
        r = cli_runner.invoke(["init", repo_path])
        assert r.exit_code == 0

        with chdir(repo_path):
            r = cli_runner.invoke(
                [
                    "point-cloud-import",
                    f"{extrabytes}/extrabytesvlr.laz",
                    "--dataset-path=extrabytes",
                    "--preserve-format",
                ]
            )
            assert r.exit_code == 0, r.stderr

            r = cli_runner.invoke(
                ["meta", "get", "extrabytes", "format.json", "-ojson"]
            )
            assert r.exit_code == 0, r.stderr
            assert json.loads(r.stdout) == {
                "extrabytes": {
                    "format.json": {
                        "compression": "laz",
                        "lasVersion": "1.4",
                        "pointDataRecordFormat": 3,
                        "pointDataRecordLength": 61,
                        "extraBytesVlr": True,
                    }
                }
            }
            r = cli_runner.invoke(
                ["meta", "get", "extrabytes", "schema.json", "-ojson"]
            )
            assert r.exit_code == 0, r.stderr
            assert json.loads(r.stdout) == {
                "extrabytes": {
                    "schema.json": [
                        {"name": "X", "dataType": "integer", "size": 32},
                        {"name": "Y", "dataType": "integer", "size": 32},
                        {"name": "Z", "dataType": "integer", "size": 32},
                        {
                            "name": "Intensity",
                            "dataType": "integer",
                            "size": 16,
                            "unsigned": True,
                        },
                        {
                            "name": "Return Number",
                            "dataType": "integer",
                            "size": 3,
                            "unsigned": True,
                        },
                        {
                            "name": "Number of Returns",
                            "dataType": "integer",
                            "size": 3,
                            "unsigned": True,
                        },
                        {
                            "name": "Scan Direction Flag",
                            "dataType": "integer",
                            "size": 1,
                        },
                        {
                            "name": "Edge of Flight Line",
                            "dataType": "integer",
                            "size": 1,
                        },
                        {
                            "name": "Classification",
                            "dataType": "integer",
                            "size": 5,
                            "unsigned": True,
                        },
                        {"name": "Synthetic", "dataType": "integer", "size": 1},
                        {"name": "Key-Point", "dataType": "integer", "size": 1},
                        {"name": "Withheld", "dataType": "integer", "size": 1},
                        {"name": "Scan Angle Rank", "dataType": "integer", "size": 8},
                        {
                            "name": "User Data",
                            "dataType": "integer",
                            "size": 8,
                            "unsigned": True,
                        },
                        {
                            "name": "Point Source ID",
                            "dataType": "integer",
                            "size": 16,
                            "unsigned": True,
                        },
                        {"name": "GPS Time", "dataType": "float", "size": 64},
                        {
                            "name": "Red",
                            "dataType": "integer",
                            "size": 16,
                            "unsigned": True,
                        },
                        {
                            "name": "Green",
                            "dataType": "integer",
                            "size": 16,
                            "unsigned": True,
                        },
                        {
                            "name": "Blue",
                            "dataType": "integer",
                            "size": 16,
                            "unsigned": True,
                        },
                        {
                            "name": "Extra Flags",
                            "dataType": "integer",
                            "size": 8,
                            "unsigned": True,
                        },
                        {"name": "Temperature", "dataType": "integer", "size": 16},
                        {"name": "Uptime", "dataType": "integer", "size": 32},
                        {
                            "name": "Nanotime",
                            "dataType": "integer",
                            "size": 64,
                            "unsigned": True,
                        },
                        {"name": "Gravity", "dataType": "float", "size": 32},
                        {"name": "Radiosity", "dataType": "float", "size": 64},
                    ]
                }
            }


def test_import_extra_bytes_vlr__convert_to_copc(
    data_archive_readonly, tmp_path, cli_runner, chdir
):
    with data_archive_readonly("point-cloud/laz-extrabytesvlr.tgz") as extrabytes:
        repo_path = tmp_path / "point-cloud-repo"
        r = cli_runner.invoke(["init", repo_path])
        assert r.exit_code == 0

        with chdir(repo_path):
            r = cli_runner.invoke(
                [
                    "point-cloud-import",
                    f"{extrabytes}/extrabytesvlr.laz",
                    "--dataset-path=extrabytes",
                    "--convert-to-copc",
                ]
            )
            assert r.exit_code == 0, r.stderr

            r = cli_runner.invoke(
                ["meta", "get", "extrabytes", "format.json", "-ojson"]
            )
            assert r.exit_code == 0, r.stderr
            assert json.loads(r.stdout) == {
                "extrabytes": {
                    "format.json": {
                        "compression": "laz",
                        "lasVersion": "1.4",
                        "optimization": "copc",
                        "optimizationVersion": "1.0",
                        "pointDataRecordFormat": 7,
                        "pointDataRecordLength": 63,
                        "extraBytesVlr": True,
                    }
                }
            }
            r = cli_runner.invoke(
                ["meta", "get", "extrabytes", "schema.json", "-ojson"]
            )
            assert r.exit_code == 0, r.stderr
            assert json.loads(r.stdout) == {
                "extrabytes": {
                    "schema.json": [
                        {"name": "X", "dataType": "integer", "size": 32},
                        {"name": "Y", "dataType": "integer", "size": 32},
                        {"name": "Z", "dataType": "integer", "size": 32},
                        {
                            "name": "Intensity",
                            "dataType": "integer",
                            "size": 16,
                            "unsigned": True,
                        },
                        {
                            "name": "Return Number",
                            "dataType": "integer",
                            "size": 4,
                            "unsigned": True,
                        },
                        {
                            "name": "Number of Returns",
                            "dataType": "integer",
                            "size": 4,
                            "unsigned": True,
                        },
                        {"name": "Synthetic", "dataType": "integer", "size": 1},
                        {"name": "Key-Point", "dataType": "integer", "size": 1},
                        {"name": "Withheld", "dataType": "integer", "size": 1},
                        {"name": "Overlap", "dataType": "integer", "size": 1},
                        {
                            "name": "Scanner Channel",
                            "dataType": "integer",
                            "size": 2,
                            "unsigned": True,
                        },
                        {
                            "name": "Scan Direction Flag",
                            "dataType": "integer",
                            "size": 1,
                        },
                        {
                            "name": "Edge of Flight Line",
                            "dataType": "integer",
                            "size": 1,
                        },
                        {
                            "name": "Classification",
                            "dataType": "integer",
                            "size": 8,
                            "unsigned": True,
                        },
                        {
                            "name": "User Data",
                            "dataType": "integer",
                            "size": 8,
                            "unsigned": True,
                        },
                        {"name": "Scan Angle", "dataType": "integer", "size": 16},
                        {
                            "name": "Point Source ID",
                            "dataType": "integer",
                            "size": 16,
                            "unsigned": True,
                        },
                        {"name": "GPS Time", "dataType": "float", "size": 64},
                        {
                            "name": "Red",
                            "dataType": "integer",
                            "size": 16,
                            "unsigned": True,
                        },
                        {
                            "name": "Green",
                            "dataType": "integer",
                            "size": 16,
                            "unsigned": True,
                        },
                        {
                            "name": "Blue",
                            "dataType": "integer",
                            "size": 16,
                            "unsigned": True,
                        },
                        {
                            "name": "Extra Flags",
                            "dataType": "integer",
                            "size": 8,
                            "unsigned": True,
                        },
                        {"name": "Temperature", "dataType": "integer", "size": 16},
                        {"name": "Uptime", "dataType": "integer", "size": 32},
                        {
                            "name": "Nanotime",
                            "dataType": "integer",
                            "size": 64,
                            "unsigned": True,
                        },
                        {"name": "Gravity", "dataType": "float", "size": 32},
                        {"name": "Radiosity", "dataType": "float", "size": 64},
                    ]
                }
            }


@pytest.mark.slow
def test_point_cloud_import_from_s3__no_convert(
    tmp_path,
    chdir,
    cli_runner,
    s3_test_data_point_cloud,
    check_lfs_hashes,
    check_tile_is_reflinked,
):
    repo_path = tmp_path / "point-cloud-repo"
    r = cli_runner.invoke(["init", repo_path])
    assert r.exit_code == 0

    repo = KartRepo(repo_path)
    with chdir(repo_path):
        r = cli_runner.invoke(
            [
                "point-cloud-import",
                s3_test_data_point_cloud,
                "--message=test_import_from_s3_no_convert",
                "--dataset-path=auckland",
                "--preserve-format",
            ]
        )
        assert r.exit_code == 0, r.stderr

        check_lfs_hashes(repo, 16)

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
            "    test_import_from_s3_no_convert",
            "",
            "+++ auckland:tile:auckland_0_0",
            "+                                     name = auckland_0_0.laz",
            "+                              crs84Extent = POLYGON((174.7384483 -36.8512371,174.7382443 -36.8422277,174.7494540 -36.8420632,174.7496594 -36.8510726,174.7384483 -36.8512371))",
            "+                                   format = laz-1.2",
            "+                             nativeExtent = 1754987.85,1755987.77,5920219.76,5921219.64,-1.66,99.83",
            "+                               pointCount = 4231",
            "+                                      oid = sha256:6b980ce4d7f4978afd3b01e39670e2071a792fba441aca45be69be81cb48b08c",
            "+                                     size = 51489",
        ]

        for x in range(4):
            for y in range(4):
                assert (repo_path / "auckland" / f"auckland_{x}_{y}.laz").is_file()
                check_tile_is_reflinked(
                    repo_path / "auckland" / f"auckland_{x}_{y}.laz", repo
                )


@pytest.mark.slow
def test_point_cloud_import_from_s3__convert(
    tmp_path,
    chdir,
    cli_runner,
    s3_test_data_point_cloud,
    check_lfs_hashes,
    check_tile_is_reflinked,
):
    repo_path = tmp_path / "point-cloud-repo"
    r = cli_runner.invoke(["init", repo_path])
    assert r.exit_code == 0

    repo = KartRepo(repo_path)
    with chdir(repo_path):
        r = cli_runner.invoke(
            [
                "point-cloud-import",
                s3_test_data_point_cloud,
                "--message=test_import_from_s3_convert",
                "--dataset-path=auckland",
                "--convert-to-copc",
            ]
        )
        assert r.exit_code == 0, r.stderr

        check_lfs_hashes(repo, 16)

        r = cli_runner.invoke(["meta", "get", "auckland", "format.json", "-ojson"])
        assert r.exit_code == 0, r.stderr
        assert json.loads(r.stdout) == {
            "auckland": {
                "format.json": {
                    "compression": "laz",
                    "lasVersion": "1.4",
                    "optimization": "copc",
                    "optimizationVersion": "1.0",
                    "pointDataRecordFormat": 7,
                    "pointDataRecordLength": 36,
                }
            }
        }

        r = cli_runner.invoke(["show", "HEAD", "auckland:tile:auckland_0_0"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines()[4:-2] == [
            "    test_import_from_s3_convert",
            "",
            "+++ auckland:tile:auckland_0_0",
            "+                                     name = auckland_0_0.copc.laz",
            "+                              crs84Extent = POLYGON((174.7384483 -36.8512371,174.7382443 -36.8422277,174.7494540 -36.8420632,174.7496594 -36.8510726,174.7384483 -36.8512371))",
            "+                                   format = laz-1.4/copc-1.0",
            "+                             nativeExtent = 1754987.85,1755987.77,5920219.76,5921219.64,-1.66,99.83",
            "+                               pointCount = 4231",
            "+                                sourceOid = sha256:6b980ce4d7f4978afd3b01e39670e2071a792fba441aca45be69be81cb48b08c",
        ]

        for x in range(4):
            for y in range(4):
                assert (repo_path / "auckland" / f"auckland_{x}_{y}.copc.laz").is_file()
                check_tile_is_reflinked(
                    repo_path / "auckland" / f"auckland_{x}_{y}.copc.laz", repo
                )


def test_import_with_crs_override(
    tmp_path,
    chdir,
    cli_runner,
    data_archive_readonly,
    check_lfs_hashes,
    requires_pdal,
    requires_git_lfs,
):
    """Test that --override-crs option overrides the CRS of all imported tiles."""
    with data_archive_readonly("point-cloud/laz-auckland.tgz") as auckland:
        repo_path = tmp_path / "point-cloud-repo"
        r = cli_runner.invoke(["init", repo_path])
        assert r.exit_code == 0, r.stderr

        repo = KartRepo(repo_path)
        with chdir(repo_path):
            # First, import a single tile to establish the dataset CRS
            # This tile is actually in EPSG:2193 but we override it to EPSG:2994
            r = cli_runner.invoke(
                [
                    "point-cloud-import",
                    f"{auckland}/auckland_0_0.laz",
                    "--dataset-path=auckland",
                    "--override-crs=EPSG:2994",
                    "--convert-to-copc",
                ]
            )
            assert r.exit_code == 0, r.stderr

            # Get the original CRS from the dataset
            r = cli_runner.invoke(["meta", "get", "auckland", "crs.wkt"])
            assert r.exit_code == 0, r.stderr
            original_crs = r.stdout.strip()
            assert "2994" in original_crs

            # Import another tile with --override-crs=EPSG:2994 to override its CRS
            r = cli_runner.invoke(
                [
                    "point-cloud-import",
                    f"{auckland}/auckland_0_1.laz",
                    "--dataset-path=auckland",
                    "--update-existing",
                    "--override-crs=EPSG:2994",
                    "--convert-to-copc",
                ]
            )
            assert r.exit_code == 0, r.stderr

            # Verify that both tiles were imported successfully
            r = cli_runner.invoke(["data", "ls"])
            assert r.exit_code == 0, r.stderr
            assert "auckland" in r.stdout

            # Check that the CRS is still consistent in the dataset
            r = cli_runner.invoke(["meta", "get", "auckland", "crs.wkt"])
            assert r.exit_code == 0, r.stderr
            final_crs = r.stdout.strip()

            assert "2994" in final_crs

            check_lfs_hashes(repo, 2)


def test_override_crs_rewrites_files(
    tmp_path,
    chdir,
    cli_runner,
    data_archive_readonly,
    requires_pdal,
    requires_git_lfs,
):
    """Test that --override-crs actually rewrites the CRS in the imported LAZ files."""
    import subprocess
    import json

    with data_archive_readonly("point-cloud/laz-auckland.tgz") as auckland:
        repo_path = tmp_path / "point-cloud-repo"
        r = cli_runner.invoke(["init", repo_path])
        assert r.exit_code == 0, r.stderr

        with chdir(repo_path):
            # Import a tile with CRS override
            r = cli_runner.invoke(
                [
                    "point-cloud-import",
                    f"{auckland}/auckland_0_0.laz",
                    "--dataset-path=auckland",
                    "--override-crs=EPSG:4326",
                    "--convert-to-copc",
                ]
            )
            assert r.exit_code == 0, r.stderr

            # Check out the working copy to get the actual file
            r = cli_runner.invoke(["checkout"])
            assert r.exit_code == 0, r.stderr

            # Use pdal info to check the CRS in the actual LAZ file
            laz_files = list((repo_path / "auckland").glob("*.laz"))
            assert len(laz_files) == 1

            # Run pdal info to get the CRS from the file
            result = subprocess.run(
                ["pdal", "info", str(laz_files[0]), "--metadata"],
                capture_output=True,
                text=True,
            )
            assert result.returncode == 0

            metadata = json.loads(result.stdout)
            # Check that the CRS in the file is EPSG:4326
            srs = metadata.get("metadata", {}).get("srs", {})
            wkt = srs.get("wkt", "")

            # EPSG:4326 should appear in the WKT
            assert (
                "4326" in wkt or "WGS 84" in wkt or "WGS_1984" in wkt
            ), f"CRS not overridden in file. WKT: {wkt}"
