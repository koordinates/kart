import re
import shutil

import pygit2
import pytest

from kart import is_windows
from kart.exceptions import (
    WORKING_COPY_OR_IMPORT_CONFLICT,
    NO_CHANGES,
    INVALID_OPERATION,
)
from kart.lfs_util import get_hash_and_size_of_file
from kart.point_cloud.metadata_util import extract_pc_tile_metadata
from kart.repo import KartRepo
from kart import subprocess_util as subprocess
from kart.workdir import FileSystemWorkingCopy
from .fixtures import requires_pdal  # noqa


def test_working_copy_edit(cli_runner, data_archive, requires_pdal):
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

        EXPECTED_TILE_DIFF = [
            "--- auckland:tile:auckland_1_1",
            "+++ auckland:tile:auckland_1_1",
            "-                              crs84Extent = POLYGON((174.7494680 -36.8420542,174.7492629 -36.8330539,174.7604509 -36.8328887,174.7606572 -36.8418890,174.7494680 -36.8420542))",
            "+                              crs84Extent = POLYGON((174.7384483 -36.8512371,174.7382443 -36.8422277,174.7494540 -36.8420632,174.7496594 -36.8510726,174.7384483 -36.8512371))",
            "-                             nativeExtent = 1755989.03,1756987.13,5921220.62,5922219.49,-1.48,35.15",
            "+                             nativeExtent = 1754987.85,1755987.77,5920219.76,5921219.64,-1.66,99.83",
            "-                               pointCount = 1558",
            "+                               pointCount = 4231",
            "-                                sourceOid = sha256:d89966fb10b30d6987955ae1b97c752ba875de89da1881e2b05820878d17eab9",
            "-                                      oid = sha256:8624133a3fa257e528fe1e0a01e1f2a7fa9f453cbe4fe283a31eabaf77c68794",
            "+                                      oid = sha256:adbc1dc7fc99c88fcb627b9c40cdb56c211b791fe9cf83fe066b1a9932c12569",
            "-                                     size = 19975",
            "+                                     size = 54396",
            "--- auckland:tile:auckland_3_3",
            "-                                     name = auckland_3_3.copc.laz",
            "-                              crs84Extent = POLYGON((174.7726438 -36.8236912,174.7726418 -36.8236049,174.7819653 -36.8234655,174.7819673 -36.8235518,174.7726438 -36.8236912))",
            "-                                   format = laz-1.4/copc-1.0",
            "-                             nativeExtent = 1758093.46,1758925.34,5923219.8,5923229.38,-1.28,9.8",
            "-                               pointCount = 29",
            "-                                sourceOid = sha256:4190c9056b732fadd6e86500e93047a787d88812f7a4af21c7759d92d1d48954",
            "-                                      oid = sha256:0fd4dc03d2e9963658cf70e9d52fa1eaa7292da71d89d0188cfa88d5afb75ab6",
            "-                                     size = 2314",
            "+++ auckland:tile:auckland_4_4",
            "+                                     name = auckland_4_4.copc.laz",
            "+                              crs84Extent = POLYGON((174.7726438 -36.8236912,174.7726418 -36.8236049,174.7819653 -36.8234655,174.7819673 -36.8235518,174.7726438 -36.8236912))",
            "+                                   format = laz-1.4/copc-1.0",
            "+                             nativeExtent = 1758093.46,1758925.34,5923219.8,5923229.38,-1.28,9.8",
            "+                               pointCount = 29",
            "+                                      oid = sha256:0fd4dc03d2e9963658cf70e9d52fa1eaa7292da71d89d0188cfa88d5afb75ab6",
            "+                                     size = 2314",
        ]

        r = cli_runner.invoke(["diff"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == EXPECTED_TILE_DIFF

        r = cli_runner.invoke(["commit", "-m", "Edit point cloud tiles"])
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["show"])
        assert r.exit_code == 0, r.stderr
        assert (
            r.stdout.splitlines()[4:]
            == ["    Edit point cloud tiles", ""] + EXPECTED_TILE_DIFF
        )

        r = cli_runner.invoke(["show", "-ojson"])
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["show", "-ojson-lines"])
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["diff"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == []


def test_working_copy_restore_reset(cli_runner, data_archive, requires_pdal):
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

        r = cli_runner.invoke(["restore", "-s", "HEAD^", "auckland:tile:auckland_0_0"])
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
                "auckland:tile:auckland_0_1",
                "auckland:tile:auckland_0_2",
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


def test_working_copy_meta_edit(
    cli_runner, data_archive, data_archive_readonly, requires_pdal
):
    with data_archive_readonly("point-cloud/laz-autzen.tgz") as autzen:
        with data_archive("point-cloud/auckland.tgz") as repo_path:
            r = cli_runner.invoke(["diff"])
            assert r.exit_code == 0, r.stderr
            assert r.stdout.splitlines() == []

            tiles_path = repo_path / "auckland"
            assert tiles_path.is_dir()

            shutil.copy(autzen / "autzen.laz", tiles_path / "autzen.laz")

            r = cli_runner.invoke(["status"])
            assert r.exit_code == 0, r.stderr
            # TODO: kart status doesn't report on these type of conflicts.
            assert r.stdout.splitlines() == [
                "On branch main",
                "",
                "Changes in working copy:",
                '  (use "kart commit" to commit)',
                '  (use "kart restore" to discard changes)',
                "",
                "  auckland:",
                "    meta:",
                "      3 updates",
                "    tile:",
                "      1 inserts",
            ]

            # The diff has conflicts as the user is adding a tile with a new CRS without deleting the tiles with the old CRS:
            # Same for the format and the schema.
            r = cli_runner.invoke(["diff"])
            assert r.exit_code == 0, r.stderr
            assert r.stdout.splitlines() == [
                "--- auckland:meta:crs.wkt",
                "+++ auckland:meta:crs.wkt",
                '- COMPD_CS["NZGD2000 / New Zealand Transverse Mercator 2000 + VERT_CS",',
                '-     PROJCS["NZGD2000 / New Zealand Transverse Mercator 2000",',
                '-         GEOGCS["NZGD2000",',
                '-             DATUM["New_Zealand_Geodetic_Datum_2000",',
                '-                 SPHEROID["GRS 1980", 6378137, 298.257222101,',
                '-                     AUTHORITY["EPSG", "7019"]],',
                '-                 AUTHORITY["EPSG", "6167"]],',
                '-             PRIMEM["Greenwich", 0,',
                '-                 AUTHORITY["EPSG", "8901"]],',
                '-             UNIT["degree", 0.0174532925199433,',
                '-                 AUTHORITY["EPSG", "9122"]],',
                '-             AUTHORITY["EPSG", "4167"]],',
                '-         PROJECTION["Transverse_Mercator"],',
                '-         PARAMETER["latitude_of_origin", 0],',
                '-         PARAMETER["central_meridian", 173],',
                '-         PARAMETER["scale_factor", 0.9996],',
                '-         PARAMETER["false_easting", 1600000],',
                '-         PARAMETER["false_northing", 10000000],',
                '-         UNIT["metre", 1,',
                '-             AUTHORITY["EPSG", "9001"]],',
                '-         AXIS["Northing", NORTH],',
                '-         AXIS["Easting", EAST],',
                '-         AUTHORITY["EPSG", "2193"]],',
                '-     VERT_CS["NZVD2009 height",',
                '-         VERT_DATUM["New Zealand Vertical Datum 2009", 2005,',
                '-             AUTHORITY["EPSG", "1039"]],',
                '-         UNIT["metre", 1,',
                '-             AUTHORITY["EPSG", "9001"]],',
                '-         AXIS["Gravity-related height", UP],',
                '-         AUTHORITY["EPSG", "4440"]]]',
                "- ",
                "+ <<<<<<< ",
                '+ COMPD_CS["NZGD2000 / New Zealand Transverse Mercator 2000 + VERT_CS",',
                '+     PROJCS["NZGD2000 / New Zealand Transverse Mercator 2000",',
                '+         GEOGCS["NZGD2000",',
                '+             DATUM["New_Zealand_Geodetic_Datum_2000",',
                '+                 SPHEROID["GRS 1980", 6378137, 298.257222101,',
                '+                     AUTHORITY["EPSG", "7019"]],',
                '+                 AUTHORITY["EPSG", "6167"]],',
                '+             PRIMEM["Greenwich", 0,',
                '+                 AUTHORITY["EPSG", "8901"]],',
                '+             UNIT["degree", 0.0174532925199433,',
                '+                 AUTHORITY["EPSG", "9122"]],',
                '+             AUTHORITY["EPSG", "4167"]],',
                '+         PROJECTION["Transverse_Mercator"],',
                '+         PARAMETER["latitude_of_origin", 0],',
                '+         PARAMETER["central_meridian", 173],',
                '+         PARAMETER["scale_factor", 0.9996],',
                '+         PARAMETER["false_easting", 1600000],',
                '+         PARAMETER["false_northing", 10000000],',
                '+         UNIT["metre", 1,',
                '+             AUTHORITY["EPSG", "9001"]],',
                '+         AXIS["Northing", NORTH],',
                '+         AXIS["Easting", EAST],',
                '+         AUTHORITY["EPSG", "2193"]],',
                '+     VERT_CS["NZVD2009 height",',
                '+         VERT_DATUM["New Zealand Vertical Datum 2009", 2005,',
                '+             AUTHORITY["EPSG", "1039"]],',
                '+         UNIT["metre", 1,',
                '+             AUTHORITY["EPSG", "9001"]],',
                '+         AXIS["Gravity-related height", UP],',
                '+         AUTHORITY["EPSG", "4440"]]]',
                "+ ",
                "+ ======== ",
                '+ PROJCS["NAD83(HARN) / Oregon GIC Lambert (ft)",',
                '+     GEOGCS["NAD83(HARN)",',
                '+         DATUM["NAD83_High_Accuracy_Reference_Network",',
                '+             SPHEROID["GRS 1980", 6378137, 298.257222101,',
                '+                 AUTHORITY["EPSG", "7019"]],',
                '+             AUTHORITY["EPSG", "6152"]],',
                '+         PRIMEM["Greenwich", 0,',
                '+             AUTHORITY["EPSG", "8901"]],',
                '+         UNIT["degree", 0.0174532925199433,',
                '+             AUTHORITY["EPSG", "9122"]],',
                '+         AUTHORITY["EPSG", "4152"]],',
                '+     PROJECTION["Lambert_Conformal_Conic_2SP"],',
                '+     PARAMETER["latitude_of_origin", 41.75],',
                '+     PARAMETER["central_meridian", -120.5],',
                '+     PARAMETER["standard_parallel_1", 43],',
                '+     PARAMETER["standard_parallel_2", 45.5],',
                '+     PARAMETER["false_easting", 1312335.958],',
                '+     PARAMETER["false_northing", 0],',
                '+     UNIT["foot", 0.3048,',
                '+         AUTHORITY["EPSG", "9002"]],',
                '+     AXIS["Easting", EAST],',
                '+     AXIS["Northing", NORTH],',
                '+     AUTHORITY["EPSG", "2994"]]',
                "+ ",
                "+ >>>>>>> ",
                "--- auckland:meta:format.json",
                "+++ auckland:meta:format.json",
                "- {",
                '-   "compression": "laz",',
                '-   "lasVersion": "1.4",',
                '-   "optimization": "copc",',
                '-   "optimizationVersion": "1.0",',
                '-   "pointDataRecordFormat": 7,',
                '-   "pointDataRecordLength": 36',
                "- }",
                "+ <<<<<<< ",
                "+ {",
                '+   "compression": "laz",',
                '+   "lasVersion": "1.4",',
                '+   "optimization": "copc",',
                '+   "optimizationVersion": "1.0",',
                '+   "pointDataRecordFormat": 7,',
                '+   "pointDataRecordLength": 36',
                "+ }",
                "+ ======== ",
                "+ {",
                '+   "compression": "laz",',
                '+   "lasVersion": "1.2",',
                '+   "optimization": null,',
                '+   "optimizationVersion": null,',
                '+   "pointDataRecordFormat": 1,',
                '+   "pointDataRecordLength": 28',
                "+ }",
                "+ >>>>>>> ",
                "--- auckland:meta:schema.json",
                "+++ auckland:meta:schema.json",
                "- [",
                "-   {",
                '-     "name": "X",',
                '-     "dataType": "integer",',
                '-     "size": 32',
                "-   },",
                "-   {",
                '-     "name": "Y",',
                '-     "dataType": "integer",',
                '-     "size": 32',
                "-   },",
                "-   {",
                '-     "name": "Z",',
                '-     "dataType": "integer",',
                '-     "size": 32',
                "-   },",
                "-   {",
                '-     "name": "Intensity",',
                '-     "dataType": "integer",',
                '-     "size": 16,',
                '-     "unsigned": true',
                "-   },",
                "-   {",
                '-     "name": "Return Number",',
                '-     "dataType": "integer",',
                '-     "size": 4,',
                '-     "unsigned": true',
                "-   },",
                "-   {",
                '-     "name": "Number of Returns",',
                '-     "dataType": "integer",',
                '-     "size": 4,',
                '-     "unsigned": true',
                "-   },",
                "-   {",
                '-     "name": "Synthetic",',
                '-     "dataType": "integer",',
                '-     "size": 1',
                "-   },",
                "-   {",
                '-     "name": "Key-Point",',
                '-     "dataType": "integer",',
                '-     "size": 1',
                "-   },",
                "-   {",
                '-     "name": "Withheld",',
                '-     "dataType": "integer",',
                '-     "size": 1',
                "-   },",
                "-   {",
                '-     "name": "Overlap",',
                '-     "dataType": "integer",',
                '-     "size": 1',
                "-   },",
                "-   {",
                '-     "name": "Scanner Channel",',
                '-     "dataType": "integer",',
                '-     "size": 2,',
                '-     "unsigned": true',
                "-   },",
                "-   {",
                '-     "name": "Scan Direction Flag",',
                '-     "dataType": "integer",',
                '-     "size": 1',
                "-   },",
                "-   {",
                '-     "name": "Edge of Flight Line",',
                '-     "dataType": "integer",',
                '-     "size": 1',
                "-   },",
                "-   {",
                '-     "name": "Classification",',
                '-     "dataType": "integer",',
                '-     "size": 8,',
                '-     "unsigned": true',
                "-   },",
                "-   {",
                '-     "name": "User Data",',
                '-     "dataType": "integer",',
                '-     "size": 8,',
                '-     "unsigned": true',
                "-   },",
                "-   {",
                '-     "name": "Scan Angle",',
                '-     "dataType": "integer",',
                '-     "size": 16',
                "-   },",
                "-   {",
                '-     "name": "Point Source ID",',
                '-     "dataType": "integer",',
                '-     "size": 16,',
                '-     "unsigned": true',
                "-   },",
                "-   {",
                '-     "name": "GPS Time",',
                '-     "dataType": "float",',
                '-     "size": 64',
                "-   },",
                "-   {",
                '-     "name": "Red",',
                '-     "dataType": "integer",',
                '-     "size": 16,',
                '-     "unsigned": true',
                "-   },",
                "-   {",
                '-     "name": "Green",',
                '-     "dataType": "integer",',
                '-     "size": 16,',
                '-     "unsigned": true',
                "-   },",
                "-   {",
                '-     "name": "Blue",',
                '-     "dataType": "integer",',
                '-     "size": 16,',
                '-     "unsigned": true',
                "-   }",
                "- ]",
                "+ <<<<<<< ",
                "+ [",
                "+   {",
                '+     "name": "X",',
                '+     "dataType": "integer",',
                '+     "size": 32',
                "+   },",
                "+   {",
                '+     "name": "Y",',
                '+     "dataType": "integer",',
                '+     "size": 32',
                "+   },",
                "+   {",
                '+     "name": "Z",',
                '+     "dataType": "integer",',
                '+     "size": 32',
                "+   },",
                "+   {",
                '+     "name": "Intensity",',
                '+     "dataType": "integer",',
                '+     "size": 16,',
                '+     "unsigned": true',
                "+   },",
                "+   {",
                '+     "name": "Return Number",',
                '+     "dataType": "integer",',
                '+     "size": 4,',
                '+     "unsigned": true',
                "+   },",
                "+   {",
                '+     "name": "Number of Returns",',
                '+     "dataType": "integer",',
                '+     "size": 4,',
                '+     "unsigned": true',
                "+   },",
                "+   {",
                '+     "name": "Synthetic",',
                '+     "dataType": "integer",',
                '+     "size": 1',
                "+   },",
                "+   {",
                '+     "name": "Key-Point",',
                '+     "dataType": "integer",',
                '+     "size": 1',
                "+   },",
                "+   {",
                '+     "name": "Withheld",',
                '+     "dataType": "integer",',
                '+     "size": 1',
                "+   },",
                "+   {",
                '+     "name": "Overlap",',
                '+     "dataType": "integer",',
                '+     "size": 1',
                "+   },",
                "+   {",
                '+     "name": "Scanner Channel",',
                '+     "dataType": "integer",',
                '+     "size": 2,',
                '+     "unsigned": true',
                "+   },",
                "+   {",
                '+     "name": "Scan Direction Flag",',
                '+     "dataType": "integer",',
                '+     "size": 1',
                "+   },",
                "+   {",
                '+     "name": "Edge of Flight Line",',
                '+     "dataType": "integer",',
                '+     "size": 1',
                "+   },",
                "+   {",
                '+     "name": "Classification",',
                '+     "dataType": "integer",',
                '+     "size": 8,',
                '+     "unsigned": true',
                "+   },",
                "+   {",
                '+     "name": "User Data",',
                '+     "dataType": "integer",',
                '+     "size": 8,',
                '+     "unsigned": true',
                "+   },",
                "+   {",
                '+     "name": "Scan Angle",',
                '+     "dataType": "integer",',
                '+     "size": 16',
                "+   },",
                "+   {",
                '+     "name": "Point Source ID",',
                '+     "dataType": "integer",',
                '+     "size": 16,',
                '+     "unsigned": true',
                "+   },",
                "+   {",
                '+     "name": "GPS Time",',
                '+     "dataType": "float",',
                '+     "size": 64',
                "+   },",
                "+   {",
                '+     "name": "Red",',
                '+     "dataType": "integer",',
                '+     "size": 16,',
                '+     "unsigned": true',
                "+   },",
                "+   {",
                '+     "name": "Green",',
                '+     "dataType": "integer",',
                '+     "size": 16,',
                '+     "unsigned": true',
                "+   },",
                "+   {",
                '+     "name": "Blue",',
                '+     "dataType": "integer",',
                '+     "size": 16,',
                '+     "unsigned": true',
                "+   }",
                "+ ]",
                "+ ======== ",
                "+ [",
                "+   {",
                '+     "name": "X",',
                '+     "dataType": "integer",',
                '+     "size": 32',
                "+   },",
                "+   {",
                '+     "name": "Y",',
                '+     "dataType": "integer",',
                '+     "size": 32',
                "+   },",
                "+   {",
                '+     "name": "Z",',
                '+     "dataType": "integer",',
                '+     "size": 32',
                "+   },",
                "+   {",
                '+     "name": "Intensity",',
                '+     "dataType": "integer",',
                '+     "size": 16,',
                '+     "unsigned": true',
                "+   },",
                "+   {",
                '+     "name": "Return Number",',
                '+     "dataType": "integer",',
                '+     "size": 3,',
                '+     "unsigned": true',
                "+   },",
                "+   {",
                '+     "name": "Number of Returns",',
                '+     "dataType": "integer",',
                '+     "size": 3,',
                '+     "unsigned": true',
                "+   },",
                "+   {",
                '+     "name": "Scan Direction Flag",',
                '+     "dataType": "integer",',
                '+     "size": 1',
                "+   },",
                "+   {",
                '+     "name": "Edge of Flight Line",',
                '+     "dataType": "integer",',
                '+     "size": 1',
                "+   },",
                "+   {",
                '+     "name": "Classification",',
                '+     "dataType": "integer",',
                '+     "size": 5,',
                '+     "unsigned": true',
                "+   },",
                "+   {",
                '+     "name": "Synthetic",',
                '+     "dataType": "integer",',
                '+     "size": 1',
                "+   },",
                "+   {",
                '+     "name": "Key-Point",',
                '+     "dataType": "integer",',
                '+     "size": 1',
                "+   },",
                "+   {",
                '+     "name": "Withheld",',
                '+     "dataType": "integer",',
                '+     "size": 1',
                "+   },",
                "+   {",
                '+     "name": "Scan Angle Rank",',
                '+     "dataType": "integer",',
                '+     "size": 8',
                "+   },",
                "+   {",
                '+     "name": "User Data",',
                '+     "dataType": "integer",',
                '+     "size": 8,',
                '+     "unsigned": true',
                "+   },",
                "+   {",
                '+     "name": "Point Source ID",',
                '+     "dataType": "integer",',
                '+     "size": 16,',
                '+     "unsigned": true',
                "+   },",
                "+   {",
                '+     "name": "GPS Time",',
                '+     "dataType": "float",',
                '+     "size": 64',
                "+   }",
                "+ ]",
                "+ >>>>>>> ",
                "+++ auckland:tile:autzen",
                "+                                     name = autzen.laz",
                "+                              crs84Extent = POLYGON((-123.0748659 44.0499898,-123.0753890 44.0620142,-123.0630351 44.0622931,-123.0625145 44.0502686,-123.0748659 44.0499898))",
                "+                                   format = laz-1.2",
                "+                             nativeExtent = 635616.31,638864.6,848977.79,853362.37,407.35,536.84",
                "+                               pointCount = 106",
                "+                                      oid = sha256:751ec764325610dae8f37d7f4273e3b404e5acb64421676fd72e7e31468c6720",
                "+                                     size = 2359",
            ]
            r = cli_runner.invoke(["commit", "-m", "conflicts"])
            assert r.exit_code == WORKING_COPY_OR_IMPORT_CONFLICT
            assert (
                "Committing more than one 'format.json' for 'auckland' is not supported"
                in r.stderr
            )

            # If all the old tiles are deleted, there will no longer be a conflict.
            for tile in tiles_path.glob("auckland_*.copc.laz"):
                tile.unlink()

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
                "    meta:",
                "      3 updates",
                "    tile:",
                "      1 inserts",
                "      16 deletes",
            ]

            EXPECTED_META_DIFF = [
                "--- auckland:meta:crs.wkt",
                "+++ auckland:meta:crs.wkt",
                '- COMPD_CS["NZGD2000 / New Zealand Transverse Mercator 2000 + VERT_CS",',
                '-     PROJCS["NZGD2000 / New Zealand Transverse Mercator 2000",',
                '-         GEOGCS["NZGD2000",',
                '-             DATUM["New_Zealand_Geodetic_Datum_2000",',
                '-                 SPHEROID["GRS 1980", 6378137, 298.257222101,',
                '-                     AUTHORITY["EPSG", "7019"]],',
                '-                 AUTHORITY["EPSG", "6167"]],',
                '-             PRIMEM["Greenwich", 0,',
                '-                 AUTHORITY["EPSG", "8901"]],',
                '-             UNIT["degree", 0.0174532925199433,',
                '-                 AUTHORITY["EPSG", "9122"]],',
                '-             AUTHORITY["EPSG", "4167"]],',
                '-         PROJECTION["Transverse_Mercator"],',
                '-         PARAMETER["latitude_of_origin", 0],',
                '-         PARAMETER["central_meridian", 173],',
                '-         PARAMETER["scale_factor", 0.9996],',
                '-         PARAMETER["false_easting", 1600000],',
                '-         PARAMETER["false_northing", 10000000],',
                '-         UNIT["metre", 1,',
                '-             AUTHORITY["EPSG", "9001"]],',
                '-         AXIS["Northing", NORTH],',
                '-         AXIS["Easting", EAST],',
                '-         AUTHORITY["EPSG", "2193"]],',
                '-     VERT_CS["NZVD2009 height",',
                '-         VERT_DATUM["New Zealand Vertical Datum 2009", 2005,',
                '-             AUTHORITY["EPSG", "1039"]],',
                '-         UNIT["metre", 1,',
                '-             AUTHORITY["EPSG", "9001"]],',
                '-         AXIS["Gravity-related height", UP],',
                '-         AUTHORITY["EPSG", "4440"]]]',
                "- ",
                '+ PROJCS["NAD83(HARN) / Oregon GIC Lambert (ft)",',
                '+     GEOGCS["NAD83(HARN)",',
                '+         DATUM["NAD83_High_Accuracy_Reference_Network",',
                '+             SPHEROID["GRS 1980", 6378137, 298.257222101,',
                '+                 AUTHORITY["EPSG", "7019"]],',
                '+             AUTHORITY["EPSG", "6152"]],',
                '+         PRIMEM["Greenwich", 0,',
                '+             AUTHORITY["EPSG", "8901"]],',
                '+         UNIT["degree", 0.0174532925199433,',
                '+             AUTHORITY["EPSG", "9122"]],',
                '+         AUTHORITY["EPSG", "4152"]],',
                '+     PROJECTION["Lambert_Conformal_Conic_2SP"],',
                '+     PARAMETER["latitude_of_origin", 41.75],',
                '+     PARAMETER["central_meridian", -120.5],',
                '+     PARAMETER["standard_parallel_1", 43],',
                '+     PARAMETER["standard_parallel_2", 45.5],',
                '+     PARAMETER["false_easting", 1312335.958],',
                '+     PARAMETER["false_northing", 0],',
                '+     UNIT["foot", 0.3048,',
                '+         AUTHORITY["EPSG", "9002"]],',
                '+     AXIS["Easting", EAST],',
                '+     AXIS["Northing", NORTH],',
                '+     AUTHORITY["EPSG", "2994"]]',
                "+ ",
                "--- auckland:meta:format.json",
                "+++ auckland:meta:format.json",
                "- {",
                '-   "compression": "laz",',
                '-   "lasVersion": "1.4",',
                '-   "optimization": "copc",',
                '-   "optimizationVersion": "1.0",',
                '-   "pointDataRecordFormat": 7,',
                '-   "pointDataRecordLength": 36',
                "- }",
                "+ <<<<<<< ",
                "+ {",
                '+   "compression": "laz",',
                '+   "lasVersion": "1.2",',
                '+   "optimization": null,',
                '+   "optimizationVersion": null,',
                '+   "pointDataRecordFormat": 1,',
                '+   "pointDataRecordLength": 28',
                "+ }",
                "+ >>>>>>> ",
                "--- auckland:meta:schema.json",
                "+++ auckland:meta:schema.json",
                "  [",
                "    {",
                '      "name": "X",',
                '      "dataType": "integer",',
                '      "size": 32',
                "    },",
                "    {",
                '      "name": "Y",',
                '      "dataType": "integer",',
                '      "size": 32',
                "    },",
                "    {",
                '      "name": "Z",',
                '      "dataType": "integer",',
                '      "size": 32',
                "    },",
                "    {",
                '      "name": "Intensity",',
                '      "dataType": "integer",',
                '      "size": 16,',
                '      "unsigned": true',
                "    },",
                "    {",
                '      "name": "Return Number",',
                '      "dataType": "integer",',
                '-     "size": 4,',
                '+     "size": 3,',
                '      "unsigned": true,',
                "    },",
                "    {",
                '      "name": "Number of Returns",',
                '      "dataType": "integer",',
                '-     "size": 4,',
                '+     "size": 3,',
                '      "unsigned": true,',
                "    },",
                "+   {",
                '+     "name": "Scan Direction Flag",',
                '+     "dataType": "integer",',
                '+     "size": 1',
                "+   },",
                "+   {",
                '+     "name": "Edge of Flight Line",',
                '+     "dataType": "integer",',
                '+     "size": 1',
                "+   },",
                "+   {",
                '+     "name": "Classification",',
                '+     "dataType": "integer",',
                '+     "size": 5,',
                '+     "unsigned": true',
                "+   },",
                "    {",
                '      "name": "Synthetic",',
                '      "dataType": "integer",',
                '      "size": 1',
                "    },",
                "    {",
                '      "name": "Key-Point",',
                '      "dataType": "integer",',
                '      "size": 1',
                "    },",
                "    {",
                '      "name": "Withheld",',
                '      "dataType": "integer",',
                '      "size": 1',
                "    },",
                "-   {",
                '-     "name": "Overlap",',
                '-     "dataType": "integer",',
                '-     "size": 1',
                "-   },",
                "-   {",
                '-     "name": "Scanner Channel",',
                '-     "dataType": "integer",',
                '-     "size": 2,',
                '-     "unsigned": true',
                "-   },",
                "-   {",
                '-     "name": "Scan Direction Flag",',
                '-     "dataType": "integer",',
                '-     "size": 1',
                "-   },",
                "-   {",
                '-     "name": "Edge of Flight Line",',
                '-     "dataType": "integer",',
                '-     "size": 1',
                "-   },",
                "-   {",
                '-     "name": "Classification",',
                '-     "dataType": "integer",',
                '-     "size": 8,',
            ]

            # We can't downgrade the dataset format from COPC to non-COPC without adding extra flags.
            r = cli_runner.invoke(["diff"])
            assert r.exit_code == 0, r.stderr
            assert r.stdout.splitlines()[:171] == EXPECTED_META_DIFF
            assert (
                "Committing these tiles as-is would change the format of dataset 'auckland' from cloud-optimized to non-cloud-optimized."
                in r.stderr
            )

            r = cli_runner.invoke(["commit", "-m", "Edit meta items"])
            assert r.exit_code == WORKING_COPY_OR_IMPORT_CONFLICT
            assert (
                "Committing these tiles as-is would change the format of dataset 'auckland' from cloud-optimized to non-cloud-optimized."
                in r.stderr
            )

            success_meta_diff = [
                line
                for line in EXPECTED_META_DIFF
                if line not in ("+ <<<<<<< ", "+ >>>>>>> ")
            ]

            # We can downgrade the dataset format from COPC to non-COPC once we specify --no-convert-to-dataset-format.
            r = cli_runner.invoke(["diff", "--no-convert-to-dataset-format"])
            assert r.exit_code == 0, r.stderr
            assert r.stdout.splitlines()[:169] == success_meta_diff
            r = cli_runner.invoke(
                ["commit", "-m", "Edit meta items", "--no-convert-to-dataset-format"]
            )
            assert r.exit_code == 0, r.stderr

            r = cli_runner.invoke(["show"])
            assert (
                r.stdout.splitlines()[4:175]
                == ["    Edit meta items", ""] + success_meta_diff
            )


def test_working_copy_commit_las(
    cli_runner, data_archive, data_archive_readonly, requires_pdal
):
    with data_archive_readonly("point-cloud/las-autzen.tgz") as autzen:
        with data_archive("point-cloud/auckland.tgz") as repo_path:
            tiles_path = repo_path / "auckland"
            assert tiles_path.is_dir()

            shutil.copy(autzen / "autzen.las", tiles_path / "autzen.las")

            r = cli_runner.invoke(["commit", "-m", "Add single LAS file"])
            assert r.exit_code == WORKING_COPY_OR_IMPORT_CONFLICT
            assert "Committing LAS tiles is not supported" in r.stderr

            # If all the old tiles are deleted, there will no longer be a conflict, but we still can't commit LAS files.
            for tile in tiles_path.glob("auckland_*.copc.laz"):
                tile.unlink()

            r = cli_runner.invoke(
                ["commit", "-m", "Replace entire dataset with single LAS file"]
            )
            assert r.exit_code == WORKING_COPY_OR_IMPORT_CONFLICT
            assert "Committing LAS tiles is not supported" in r.stderr


def test_working_copy_commit_and_convert_to_copc(
    cli_runner, data_archive, data_archive_readonly, requires_pdal
):
    with data_archive_readonly("point-cloud/laz-auckland.tgz") as data_dir:
        with data_archive("point-cloud/auckland.tgz") as repo_path:
            tiles_path = repo_path / "auckland"
            assert tiles_path.is_dir()

            shutil.copy(data_dir / "auckland_0_0.laz", tiles_path / "new.laz")

            # The non-COPC LAZ file conflicts with the COPC dataset.
            r = cli_runner.invoke(["diff"])
            assert r.exit_code == 0, r.stderr
            assert r.stdout.splitlines()[:29] == [
                "--- auckland:meta:format.json",
                "+++ auckland:meta:format.json",
                "- {",
                '-   "compression": "laz",',
                '-   "lasVersion": "1.4",',
                '-   "optimization": "copc",',
                '-   "optimizationVersion": "1.0",',
                '-   "pointDataRecordFormat": 7,',
                '-   "pointDataRecordLength": 36',
                "- }",
                "+ <<<<<<< ",
                "+ {",
                '+   "compression": "laz",',
                '+   "lasVersion": "1.4",',
                '+   "optimization": "copc",',
                '+   "optimizationVersion": "1.0",',
                '+   "pointDataRecordFormat": 7,',
                '+   "pointDataRecordLength": 36',
                "+ }",
                "+ ======== ",
                "+ {",
                '+   "compression": "laz",',
                '+   "lasVersion": "1.2",',
                '+   "optimization": null,',
                '+   "optimizationVersion": null,',
                '+   "pointDataRecordFormat": 3,',
                '+   "pointDataRecordLength": 34',
                "+ }",
                "+ >>>>>>> ",
            ]

            r = cli_runner.invoke(["diff", "--convert-to-dataset-format"])
            assert r.exit_code == 0, r.stderr
            assert "auckland:meta:format.json" not in r.stdout
            assert r.stdout.splitlines() == [
                "+++ auckland:tile:new",
                "+                                     name = new.copc.laz",
                "+                               sourceName = new.laz",
                "+                              crs84Extent = POLYGON((174.7384483 -36.8512371,174.7382443 -36.8422277,174.7494540 -36.8420632,174.7496594 -36.8510726,174.7384483 -36.8512371))",
                "+                                   format = laz-1.4/copc-1.0",
                "+                             sourceFormat = laz-1.2",
                "+                             nativeExtent = 1754987.85,1755987.77,5920219.76,5921219.64,-1.66,99.83",
                "+                               pointCount = 4231",
                "+                                sourceOid = sha256:6b980ce4d7f4978afd3b01e39670e2071a792fba441aca45be69be81cb48b08c",
                "+                               sourceSize = 51489",
            ]

            r = cli_runner.invoke(["commit", "-m", "Commit new LAZ tile"])
            assert r.exit_code == WORKING_COPY_OR_IMPORT_CONFLICT

            r = cli_runner.invoke(
                ["commit", "--convert-to-dataset-format", "-m", "Commit new LAZ tile"]
            )
            assert r.exit_code == 0, r.stderr

            r = cli_runner.invoke(["show"])
            assert r.exit_code == 0, r.stderr
            output = r.stdout.splitlines()
            assert [
                "    Commit new LAZ tile",
                "",
                "+++ auckland:tile:new",
                "+                                     name = new.copc.laz",
                "+                              crs84Extent = POLYGON((174.7384483 -36.8512371,174.7382443 -36.8422277,174.7494540 -36.8420632,174.7496594 -36.8510726,174.7384483 -36.8512371))",
                "+                                   format = laz-1.4/copc-1.0",
                "+                             nativeExtent = 1754987.85,1755987.77,5920219.76,5921219.64,-1.66,99.83",
                "+                               pointCount = 4231",
                "+                                sourceOid = sha256:6b980ce4d7f4978afd3b01e39670e2071a792fba441aca45be69be81cb48b08c",
            ]

            assert re.match(r"\+\s+oid = sha256:[0-9a-f]{64}", output[-2])
            assert re.match(r"\+\s+size = [0-9]{5}", output[-1])

            r = cli_runner.invoke(["status"])
            assert r.exit_code == 0, r.stderr
            assert r.stdout.splitlines()[-1] == "Nothing to commit, working copy clean"

            assert tiles_path.is_dir()
            assert not (tiles_path / "new.laz").exists()
            assert (tiles_path / "new.copc.laz").is_file()

            converted_tile_metadata = extract_pc_tile_metadata(
                tiles_path / "new.copc.laz"
            )
            assert converted_tile_metadata["tile"]["format"] == "laz-1.4/copc-1.0"
            assert converted_tile_metadata["tile"]["pointCount"] == 4231


def test_working_copy_mtime_updated(cli_runner, data_archive, requires_pdal):
    # Tests the following:
    # 1. Diffs work properly when files have mtimes (modified-timestamps)
    # that make it look like the file has been modified, but in fact it has not.
    # 2. Running a diff causes the mtimes to be updated for unmodified files,
    # where the mtimes in the index no longer match the actual file. (This
    # means the next diff can run quicker since we can use the mtime check instead
    # of the comparing hashes, which involves hashing the file and takes longer).

    with data_archive("point-cloud/auckland.tgz") as repo_path:
        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines()[-1] == "Nothing to commit, working copy clean"

        repo = KartRepo(repo_path)
        env_overrides = {"GIT_INDEX_FILE": str(repo.working_copy.workdir.index_path)}

        def get_touched_files():
            # git diff-files never compares OIDs - it just lists files which appear
            # to be dirty based on a different mtime to the mtime in the index.
            cmd = ["git", "diff-files"]
            return (
                subprocess.check_output(
                    cmd,
                    env_overrides=env_overrides,
                    encoding="utf-8",
                    cwd=repo.workdir_path,
                )
                .strip()
                .splitlines()
            )

        # At this point in our test, the index has all the correct mtimes.
        # Nothing is touched or modified..
        assert len(get_touched_files()) == 0

        # So, we touch all the tiles.
        for laz_file in repo.workdir_path.glob("auckland/*.laz"):
            laz_file.touch()
        # Now all 16 tiles are touched according to Git.
        assert len(get_touched_files()) == 16

        # Then we re-run kart status. In spite of all the files being touched,
        # Kart correctly reports that no tiles have been modified.
        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines()[-1] == "Nothing to commit, working copy clean"

        # As a side effect of generating the last diff, the new mtimes of the unmodified
        # files were written back to the index, so that the next diff can run quicker.
        assert len(get_touched_files()) == 0

        # Finally, check that the point cloud tiles themselves are not found in the ODB:
        for laz_file in repo.workdir_path.glob("auckland/*.laz"):
            assert pygit2.hashfile(laz_file) not in repo.odb


def test_lfs_fetch(cli_runner, data_archive):
    with data_archive("point-cloud/auckland.tgz") as repo_path:
        # Delete everything in the local LFS cache.
        shutil.rmtree(repo_path / ".kart" / "lfs")

        r = cli_runner.invoke(["lfs+", "fetch", "HEAD", "--dry-run"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            "Running fetch with --dry-run:",
            "  Found 16 blobs to fetch from the remote",
            "",
            "LFS blob OID:                                                    (Pointer file OID):",
            "0a696f35ab1404bbe9663e52774aaa800b0cf308ad2e5e5a9735d1c8e8b0a8c4 (7e8e0ec75c9c6b6654ebfc3b73f525a53f9db1de)",
            "0fd4dc03d2e9963658cf70e9d52fa1eaa7292da71d89d0188cfa88d5afb75ab6 (c395c80aed369e277b14a59f1b2381ed2e144da4)",
            "27411724d0de7c09913eb3d22b4f1d352afb8fa5b786403f59474e47c7492d9d (46b2fc52bd59713ad5e94ac218f77fee01052d25)",
            "32b5fe23040b236dfe469456dd8f7ebbb4dcb3326305ba3e183714a32e4dd1ac (8d89fa5a1c1dd4ee484db48e5b9298276ead942b)",
            "41e855de94194ea12da99ed84612050eccd9f79523b275337012e43e69e425e9 (b60c750892f6627f31396ee08d5c6867b5d2c855)",
            "4269cf4db9798d077786bb2f842aa28608fd3a52dd7cdaa0fa66bc1cb47cc483 (8475219b0020c0dc1c2d062ea6de6deca03a2fe5)",
            "4a406d29abc0b57325449d9ebe69400441742ac3d8116133f25a1d160c2a2cc7 (91d370b8fc3a1ffa0fe665cc6dc775028f98ce58)",
            "583789bcea43177dbba446574f00f817b2f89782fcf71709d911b2ad10872d0e (78a08de18526de1be7a54a234d4752f81f9e43fe)",
            "644fa013aca1e97827be4e8cc36a5a6f347cc7fba3764c560386ed59a1b571e9 (eb16310c8482e019c6990f509e68baacf497d990)",
            "842b5d3fac074264a128559c0fc0ff462c4d15349fa1e88ebfa8a866df024394 (4bd35d35b7a5365dc6d367fc012d305da59c41f5)",
            "8624133a3fa257e528fe1e0a01e1f2a7fa9f453cbe4fe283a31eabaf77c68794 (16f7d4ea72e9a4efab2ffb095138949b6de202ba)",
            "ab12b4d27ce40f976af3584f44ab04bdeba0de32304ed7f6baf7ed264dba6ca0 (892df97ea61468e0040b3154ab7e105817a49c20)",
            "adbc1dc7fc99c88fcb627b9c40cdb56c211b791fe9cf83fe066b1a9932c12569 (1616bc399e7e1774066df88434db9f6ea5d7ec91)",
            "c9de49a81e30153254fc65c8eb291545cbb30b520aff7d4ec0cff0fab086c60b (73afa5fbb9d3f7e81ddfd9eb58d6d50806118daf)",
            "cfa530937fdbfde520ad2f2c56e64062b19063abee659d1ec8c02544b28b1b88 (fe2b0b1bade56db7a93b355e7e4cc5401ce78b72)",
            "f28e69d8a1c9ce6b6494cd17645c0316af9c28641ccd9058f32dc1f60da50a13 (58a1bfb65cc13f6ac593b4cb3edd866056a2461f)",
        ]


def test_lfs_gc(cli_runner, data_archive, monkeypatch):
    with data_archive("point-cloud/auckland.tgz") as repo_path:
        # Delete everything in the local LFS cache.
        for file in (repo_path / "auckland").glob("auckland_3_*.copc.laz"):
            file.unlink()

        r = cli_runner.invoke(["lfs+", "gc", "--dry-run"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            "Running gc with --dry-run: found 0 LFS blobs (0B) to delete from the cache"
        ]

        r = cli_runner.invoke(["commit", "-m", "Delete auckland_3_*"])
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["lfs+", "gc", "--dry-run"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            "Can't delete 4 LFS blobs (82KiB) from the cache since they have not been pushed to the remote",
            "Running gc with --dry-run: found 0 LFS blobs (0B) to delete from the cache",
        ]

        # Simulate pushing the latest commit to the remote (we don't actually have a remote set up):
        (repo_path / ".kart" / "refs" / "remotes" / "origin").mkdir(
            parents=True, exist_ok=True
        )
        shutil.copy(
            repo_path / ".kart" / "refs" / "heads" / "main",
            repo_path / ".kart" / "refs" / "remotes" / "origin" / "main",
        )

        r = cli_runner.invoke(["lfs+", "gc", "--dry-run"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            "Running gc with --dry-run: found 4 LFS blobs (82KiB) to delete from the cache",
            "0a696f35ab1404bbe9663e52774aaa800b0cf308ad2e5e5a9735d1c8e8b0a8c4",
            "0fd4dc03d2e9963658cf70e9d52fa1eaa7292da71d89d0188cfa88d5afb75ab6",
            "27411724d0de7c09913eb3d22b4f1d352afb8fa5b786403f59474e47c7492d9d",
            "c9de49a81e30153254fc65c8eb291545cbb30b520aff7d4ec0cff0fab086c60b",
        ]

        r = cli_runner.invoke(["lfs+", "gc"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            "Deleting 4 LFS blobs (82KiB) from the cache..."
        ]

        r = cli_runner.invoke(["lfs+", "gc"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == ["Deleting 0 LFS blobs (0B) from the cache..."]

        r = cli_runner.invoke(["lfs+", "fetch", "HEAD^", "--dry-run"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            "Running fetch with --dry-run:",
            "  Found 4 blobs to fetch from the remote",
            "",
            "LFS blob OID:                                                    (Pointer file OID):",
            "0a696f35ab1404bbe9663e52774aaa800b0cf308ad2e5e5a9735d1c8e8b0a8c4 (7e8e0ec75c9c6b6654ebfc3b73f525a53f9db1de)",
            "0fd4dc03d2e9963658cf70e9d52fa1eaa7292da71d89d0188cfa88d5afb75ab6 (c395c80aed369e277b14a59f1b2381ed2e144da4)",
            "27411724d0de7c09913eb3d22b4f1d352afb8fa5b786403f59474e47c7492d9d (46b2fc52bd59713ad5e94ac218f77fee01052d25)",
            "c9de49a81e30153254fc65c8eb291545cbb30b520aff7d4ec0cff0fab086c60b (73afa5fbb9d3f7e81ddfd9eb58d6d50806118daf)",
        ]


def _remove_copy_on_write_warning(stderr_output):
    warning = FileSystemWorkingCopy.COPY_ON_WRITE_WARNING
    if (
        len(stderr_output) >= len(warning)
        and stderr_output[0 : len(warning)] == warning
    ):
        return stderr_output[len(warning) :]
    return stderr_output


def test_working_copy_progress_bar(
    cli_runner, data_archive, monkeypatch, requires_pdal
):
    with data_archive("point-cloud/auckland.tgz") as repo_path:
        r = cli_runner.invoke(["create-workingcopy", "--delete-existing"])
        assert r.exit_code == 0, r.stderr
        progress_output = r.stderr.splitlines()
        progress_output = _remove_copy_on_write_warning(progress_output)
        assert progress_output == ["Writing tiles for dataset 1 of 1: auckland"]

        # Since stderr is not atty during testing, we have to force progress to show using KART_SHOW_PROGRESS.
        monkeypatch.setenv("KART_SHOW_PROGRESS", "1")
        r = cli_runner.invoke(["create-workingcopy", "--delete-existing"])
        assert r.exit_code == 0, r.stderr
        progress_output = r.stderr.splitlines()
        progress_output = _remove_copy_on_write_warning(progress_output)
        assert progress_output[0] == "Writing tiles for dataset 1 of 1: auckland"
        assert re.fullmatch(
            r"auckland: 100%\|█+\| 16/16 \[[0-9:<]+, [0-9\.]+tile/s\]",
            progress_output[-1],
        )


@pytest.mark.parametrize(
    "tile_filename",
    [
        "new.COPC.LAZ",
        "new.laz",
        "new.LAZ",
    ],
)
def test_working_copy_add_with_non_standard_extension(
    tile_filename, cli_runner, data_archive, requires_pdal
):
    with data_archive("point-cloud/auckland.tgz") as repo_path:
        tile_path = repo_path / "auckland" / "auckland_0_0.copc.laz"
        orig_hash_and_size = get_hash_and_size_of_file(tile_path)

        new_tile_path = repo_path / "auckland" / tile_filename
        shutil.copy(tile_path, new_tile_path)
        assert get_hash_and_size_of_file(new_tile_path) == orig_hash_and_size

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
        ]

        r = cli_runner.invoke(["diff", "--exit-code"])
        assert r.exit_code == 1

        r = cli_runner.invoke(["commit", "-m", "insert new tile"])
        assert r.exit_code == 0, r.stderr

        names = {f.name for f in (repo_path / "auckland").glob("auckland_0_0.*")}
        assert names == {"auckland_0_0.copc.laz"}
        assert (
            get_hash_and_size_of_file(repo_path / "auckland" / "auckland_0_0.copc.laz")
            == orig_hash_and_size
        )


@pytest.mark.parametrize(
    "tile_filename",
    [
        "auckland_0_0.COPC.LAZ",
        "auckland_0_0.laz",
        "auckland_0_0.LAZ",
    ],
)
def test_working_copy_rename_extension(
    tile_filename, cli_runner, data_archive, requires_pdal
):
    with data_archive("point-cloud/auckland.tgz") as repo_path:
        tile_path = repo_path / "auckland" / "auckland_0_0.copc.laz"
        orig_hash_and_size = get_hash_and_size_of_file(tile_path)

        new_tile_path = repo_path / "auckland" / tile_filename
        tile_path.rename(new_tile_path)
        assert get_hash_and_size_of_file(new_tile_path) == orig_hash_and_size

        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            "On branch main",
            "",
            "Nothing to commit, working copy clean",
        ]

        r = cli_runner.invoke(["diff", "--exit-code"])
        assert r.exit_code == 0

        r = cli_runner.invoke(["commit", "-m", "rename extension"])
        assert r.exit_code == NO_CHANGES

        r = cli_runner.invoke(["reset", "--discard-changes"])

        names = {f.name for f in (repo_path / "auckland").glob("auckland_0_0.*")}
        assert names == {"auckland_0_0.copc.laz"}

        assert get_hash_and_size_of_file(tile_path) == orig_hash_and_size


def test_working_copy_conflicting_extension(cli_runner, data_archive, requires_pdal):
    with data_archive("point-cloud/auckland.tgz") as repo_path:
        tile_path = repo_path / "auckland" / "auckland_0_0.copc.laz"

        new_tile_path = repo_path / "auckland" / "auckland_0_0.laz"
        shutil.copy(tile_path, new_tile_path)

        r = cli_runner.invoke(["status"])
        assert r.exit_code == INVALID_OPERATION
        assert "More than one tile found in working copy with the same name" in r.stderr


@pytest.mark.skipif(is_windows, reason="copy-on-write not supported on windows")
def test_working_copy_reflink(cli_runner, data_archive, check_tile_is_reflinked):
    # This test will show as passed if Kart's reflinks are working,
    # skipped if reflinks are not supported on this filesystem or if we can't detect them,
    # and failed if reflinks are supported but Kart fails to make use of them.

    with data_archive("point-cloud/auckland.tgz") as repo_path:
        repo = KartRepo(repo_path)

        # Extracting a repo that was tarred probably doesn't give you reflinks -
        # so we recreate the working copy so that we do get reflinks.
        cli_runner.invoke(["create-workingcopy", "--delete-existing"])

        for x in range(4):
            for y in range(4):
                check_tile_is_reflinked(
                    repo_path / "auckland" / f"auckland_{x}_{y}.copc.laz",
                    repo,
                    do_raise_skip=True,
                )
