import re
import shutil
import subprocess

import pygit2

from kart.cli_util import tool_environment
from kart.exceptions import WORKING_COPY_OR_IMPORT_CONFLICT
from kart.repo import KartRepo
from kart.point_cloud.metadata_util import extract_pc_tile_metadata
from .fixtures import requires_pdal  # noqa
from . import assert_lines_almost_equal


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
            "-                              crs84Extent = 174.7494679662727,174.76045085377945,-36.84205418524233,-36.83288872459146,-1.48,35.15",
            "+                              crs84Extent = 174.73844833207193,174.74945404214898,-36.85123712200056,-36.84206322341377,-1.66,99.83",
            "-                             nativeExtent = 1755989.03,1756987.13,5921220.62,5922219.49,-1.48,35.15",
            "+                             nativeExtent = 1754987.85,1755987.77,5920219.76,5921219.64,-1.66,99.83",
            "-                               pointCount = 1558",
            "+                               pointCount = 4231",
            "-                                sourceOid = sha256:d89966fb10b30d6987955ae1b97c752ba875de89da1881e2b05820878d17eab9",
            "-                                      oid = sha256:add2d011a19b39c0c8d70ed2313ad4955b1e0faf9a24394ab1a103930580a267",
            "+                                      oid = sha256:a1862450841dede2759af665825403e458dfa551c095d9a65ea6e6765aeae0f7",
            "-                                     size = 24552",
            "+                                     size = 69590",
            "--- auckland:tile:auckland_3_3",
            "-                                     name = auckland_3_3.copc.laz",
            "-                              crs84Extent = 174.77264383982666,174.78196531690548,-36.82369124731785,-36.82346552753396,-1.28,9.8",
            "-                                   format = laz-1.4/copc-1.0",
            "-                             nativeExtent = 1758093.46,1758925.34,5923219.8,5923229.38,-1.28,9.8",
            "-                               pointCount = 29",
            "-                                sourceOid = sha256:4190c9056b732fadd6e86500e93047a787d88812f7a4af21c7759d92d1d48954",
            "-                                      oid = sha256:64895828ea03ce9cafaef4f387338aab8d498c8eccaef1503b8b3bd97e57c5a3",
            "-                                     size = 2319",
            "+++ auckland:tile:auckland_4_4",
            "+                                     name = auckland_4_4.copc.laz",
            "+                              crs84Extent = 174.77264383982666,174.78196531690548,-36.82369124731785,-36.82346552753396,-1.28,9.8",
            "+                                   format = laz-1.4/copc-1.0",
            "+                             nativeExtent = 1758093.46,1758925.34,5923219.8,5923229.38,-1.28,9.8",
            "+                               pointCount = 29",
            "+                                      oid = sha256:64895828ea03ce9cafaef4f387338aab8d498c8eccaef1503b8b3bd97e57c5a3",
            "+                                     size = 2319",
        ]

        r = cli_runner.invoke(["diff"])
        assert r.exit_code == 0, r.stderr
        assert_lines_almost_equal(r.stdout.splitlines(), EXPECTED_TILE_DIFF)

        r = cli_runner.invoke(["commit", "-m", "Edit point cloud tiles"])
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["show"])
        assert r.exit_code == 0, r.stderr
        assert_lines_almost_equal(
            r.stdout.splitlines()[4:],
            ["    Edit point cloud tiles", ""] + EXPECTED_TILE_DIFF,
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

            # The diff has conflicts as the user is adding a dile with a new CRS without deleting the tiles with the old CRS:
            # Same for the format and the schema.
            r = cli_runner.invoke(["diff"])
            assert r.exit_code == 0, r.stderr
            assert_lines_almost_equal(
                r.stdout.splitlines(),
                [
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
                    '-     "dataType": "float",',
                    '-     "size": 64',
                    "-   },",
                    "-   {",
                    '-     "name": "Y",',
                    '-     "dataType": "float",',
                    '-     "size": 64',
                    "-   },",
                    "-   {",
                    '-     "name": "Z",',
                    '-     "dataType": "float",',
                    '-     "size": 64',
                    "-   },",
                    "-   {",
                    '-     "name": "Intensity",',
                    '-     "dataType": "integer",',
                    '-     "size": 16',
                    "-   },",
                    "-   {",
                    '-     "name": "ReturnNumber",',
                    '-     "dataType": "integer",',
                    '-     "size": 8',
                    "-   },",
                    "-   {",
                    '-     "name": "NumberOfReturns",',
                    '-     "dataType": "integer",',
                    '-     "size": 8',
                    "-   },",
                    "-   {",
                    '-     "name": "ScanDirectionFlag",',
                    '-     "dataType": "integer",',
                    '-     "size": 8',
                    "-   },",
                    "-   {",
                    '-     "name": "EdgeOfFlightLine",',
                    '-     "dataType": "integer",',
                    '-     "size": 8',
                    "-   },",
                    "-   {",
                    '-     "name": "Classification",',
                    '-     "dataType": "integer",',
                    '-     "size": 8',
                    "-   },",
                    "-   {",
                    '-     "name": "ScanAngleRank",',
                    '-     "dataType": "float",',
                    '-     "size": 32',
                    "-   },",
                    "-   {",
                    '-     "name": "UserData",',
                    '-     "dataType": "integer",',
                    '-     "size": 8',
                    "-   },",
                    "-   {",
                    '-     "name": "PointSourceId",',
                    '-     "dataType": "integer",',
                    '-     "size": 16',
                    "-   },",
                    "-   {",
                    '-     "name": "GpsTime",',
                    '-     "dataType": "float",',
                    '-     "size": 64',
                    "-   },",
                    "-   {",
                    '-     "name": "ScanChannel",',
                    '-     "dataType": "integer",',
                    '-     "size": 8',
                    "-   },",
                    "-   {",
                    '-     "name": "ClassFlags",',
                    '-     "dataType": "integer",',
                    '-     "size": 8',
                    "-   },",
                    "-   {",
                    '-     "name": "Red",',
                    '-     "dataType": "integer",',
                    '-     "size": 16',
                    "-   },",
                    "-   {",
                    '-     "name": "Green",',
                    '-     "dataType": "integer",',
                    '-     "size": 16',
                    "-   },",
                    "-   {",
                    '-     "name": "Blue",',
                    '-     "dataType": "integer",',
                    '-     "size": 16',
                    "-   }",
                    "- ]",
                    "+ <<<<<<< ",
                    "+ [",
                    "+   {",
                    '+     "name": "X",',
                    '+     "dataType": "float",',
                    '+     "size": 64',
                    "+   },",
                    "+   {",
                    '+     "name": "Y",',
                    '+     "dataType": "float",',
                    '+     "size": 64',
                    "+   },",
                    "+   {",
                    '+     "name": "Z",',
                    '+     "dataType": "float",',
                    '+     "size": 64',
                    "+   },",
                    "+   {",
                    '+     "name": "Intensity",',
                    '+     "dataType": "integer",',
                    '+     "size": 16',
                    "+   },",
                    "+   {",
                    '+     "name": "ReturnNumber",',
                    '+     "dataType": "integer",',
                    '+     "size": 8',
                    "+   },",
                    "+   {",
                    '+     "name": "NumberOfReturns",',
                    '+     "dataType": "integer",',
                    '+     "size": 8',
                    "+   },",
                    "+   {",
                    '+     "name": "ScanDirectionFlag",',
                    '+     "dataType": "integer",',
                    '+     "size": 8',
                    "+   },",
                    "+   {",
                    '+     "name": "EdgeOfFlightLine",',
                    '+     "dataType": "integer",',
                    '+     "size": 8',
                    "+   },",
                    "+   {",
                    '+     "name": "Classification",',
                    '+     "dataType": "integer",',
                    '+     "size": 8',
                    "+   },",
                    "+   {",
                    '+     "name": "ScanAngleRank",',
                    '+     "dataType": "float",',
                    '+     "size": 32',
                    "+   },",
                    "+   {",
                    '+     "name": "UserData",',
                    '+     "dataType": "integer",',
                    '+     "size": 8',
                    "+   },",
                    "+   {",
                    '+     "name": "PointSourceId",',
                    '+     "dataType": "integer",',
                    '+     "size": 16',
                    "+   },",
                    "+   {",
                    '+     "name": "GpsTime",',
                    '+     "dataType": "float",',
                    '+     "size": 64',
                    "+   },",
                    "+   {",
                    '+     "name": "ScanChannel",',
                    '+     "dataType": "integer",',
                    '+     "size": 8',
                    "+   },",
                    "+   {",
                    '+     "name": "ClassFlags",',
                    '+     "dataType": "integer",',
                    '+     "size": 8',
                    "+   },",
                    "+   {",
                    '+     "name": "Red",',
                    '+     "dataType": "integer",',
                    '+     "size": 16',
                    "+   },",
                    "+   {",
                    '+     "name": "Green",',
                    '+     "dataType": "integer",',
                    '+     "size": 16',
                    "+   },",
                    "+   {",
                    '+     "name": "Blue",',
                    '+     "dataType": "integer",',
                    '+     "size": 16',
                    "+   }",
                    "+ ]",
                    "+ ======== ",
                    "+ [",
                    "+   {",
                    '+     "name": "X",',
                    '+     "dataType": "float",',
                    '+     "size": 64',
                    "+   },",
                    "+   {",
                    '+     "name": "Y",',
                    '+     "dataType": "float",',
                    '+     "size": 64',
                    "+   },",
                    "+   {",
                    '+     "name": "Z",',
                    '+     "dataType": "float",',
                    '+     "size": 64',
                    "+   },",
                    "+   {",
                    '+     "name": "Intensity",',
                    '+     "dataType": "integer",',
                    '+     "size": 16',
                    "+   },",
                    "+   {",
                    '+     "name": "ReturnNumber",',
                    '+     "dataType": "integer",',
                    '+     "size": 8',
                    "+   },",
                    "+   {",
                    '+     "name": "NumberOfReturns",',
                    '+     "dataType": "integer",',
                    '+     "size": 8',
                    "+   },",
                    "+   {",
                    '+     "name": "ScanDirectionFlag",',
                    '+     "dataType": "integer",',
                    '+     "size": 8',
                    "+   },",
                    "+   {",
                    '+     "name": "EdgeOfFlightLine",',
                    '+     "dataType": "integer",',
                    '+     "size": 8',
                    "+   },",
                    "+   {",
                    '+     "name": "Classification",',
                    '+     "dataType": "integer",',
                    '+     "size": 8',
                    "+   },",
                    "+   {",
                    '+     "name": "ScanAngleRank",',
                    '+     "dataType": "float",',
                    '+     "size": 32',
                    "+   },",
                    "+   {",
                    '+     "name": "UserData",',
                    '+     "dataType": "integer",',
                    '+     "size": 8',
                    "+   },",
                    "+   {",
                    '+     "name": "PointSourceId",',
                    '+     "dataType": "integer",',
                    '+     "size": 16',
                    "+   },",
                    "+   {",
                    '+     "name": "GpsTime",',
                    '+     "dataType": "float",',
                    '+     "size": 64',
                    "+   }",
                    "+ ]",
                    "+ >>>>>>> ",
                    "+++ auckland:tile:autzen",
                    "+                                     name = autzen.laz",
                    "+                              crs84Extent = -123.07486587848656,-123.06303511901734,44.049989810220765,44.062293063723445,407.35,536.84",
                    "+                                   format = laz-1.2",
                    "+                             nativeExtent = 635616.31,638864.6,848977.79,853362.37,407.35,536.84",
                    "+                               pointCount = 106",
                    "+                                      oid = sha256:751ec764325610dae8f37d7f4273e3b404e5acb64421676fd72e7e31468c6720",
                    "+                                     size = 2359",
                ],
            )

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
                "+ {",
                '+   "compression": "laz",',
                '+   "lasVersion": "1.2",',
                '+   "optimization": null,',
                '+   "optimizationVersion": null,',
                '+   "pointDataRecordFormat": 1,',
                '+   "pointDataRecordLength": 28',
                "+ }",
                "--- auckland:meta:schema.json",
                "+++ auckland:meta:schema.json",
                "  [",
                "    {",
                '      "name": "X",',
                '      "dataType": "float",',
                '      "size": 64',
                "    },",
                "    {",
                '      "name": "Y",',
                '      "dataType": "float",',
                '      "size": 64',
                "    },",
                "    {",
                '      "name": "Z",',
                '      "dataType": "float",',
                '      "size": 64',
                "    },",
                "    {",
                '      "name": "Intensity",',
                '      "dataType": "integer",',
                '      "size": 16',
                "    },",
                "    {",
                '      "name": "ReturnNumber",',
                '      "dataType": "integer",',
                '      "size": 8',
                "    },",
                "    {",
                '      "name": "NumberOfReturns",',
                '      "dataType": "integer",',
                '      "size": 8',
                "    },",
                "    {",
                '      "name": "ScanDirectionFlag",',
                '      "dataType": "integer",',
                '      "size": 8',
                "    },",
                "    {",
                '      "name": "EdgeOfFlightLine",',
                '      "dataType": "integer",',
                '      "size": 8',
                "    },",
                "    {",
                '      "name": "Classification",',
                '      "dataType": "integer",',
                '      "size": 8',
                "    },",
                "    {",
                '      "name": "ScanAngleRank",',
                '      "dataType": "float",',
                '      "size": 32',
                "    },",
                "    {",
                '      "name": "UserData",',
                '      "dataType": "integer",',
                '      "size": 8',
                "    },",
                "    {",
                '      "name": "PointSourceId",',
                '      "dataType": "integer",',
                '      "size": 16',
                "    },",
                "    {",
                '      "name": "GpsTime",',
                '      "dataType": "float",',
                '      "size": 64',
                "    },",
                "-   {",
                '-     "name": "ScanChannel",',
                '-     "dataType": "integer",',
                '-     "size": 8',
                "-   },",
                "-   {",
                '-     "name": "ClassFlags",',
                '-     "dataType": "integer",',
                '-     "size": 8',
                "-   },",
                "-   {",
                '-     "name": "Red",',
                '-     "dataType": "integer",',
                '-     "size": 16',
                "-   },",
                "-   {",
                '-     "name": "Green",',
                '-     "dataType": "integer",',
                '-     "size": 16',
                "-   },",
                "-   {",
                '-     "name": "Blue",',
                '-     "dataType": "integer",',
                '-     "size": 16',
                "-   },",
                "  ]",
            ]

            r = cli_runner.invoke(["diff"])
            assert r.exit_code == 0, r.stderr
            assert_lines_almost_equal(r.stdout.splitlines()[:169], EXPECTED_META_DIFF)

            r = cli_runner.invoke(["commit", "-m", "Edit meta items"])
            assert r.exit_code == 0, r.stderr

            r = cli_runner.invoke(["show"])
            assert_lines_almost_equal(
                r.stdout.splitlines()[4:175],
                ["    Edit meta items", ""] + EXPECTED_META_DIFF,
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
            assert (
                "Committing more than one 'format.json' for 'auckland' is not supported"
                in r.stderr
            )

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
            assert_lines_almost_equal(
                r.stdout.splitlines(),
                [
                    "+++ auckland:tile:new",
                    "+                                     name = new.copc.laz",
                    "+                               sourceName = new.laz",
                    "+                              crs84Extent = 174.73844833207193,174.74945404214898,-36.85123712200056,-36.84206322341377,-1.66,99.83",
                    "+                                   format = laz-1.4/copc-1.0",
                    "+                             sourceFormat = laz-1.2",
                    "+                             nativeExtent = 1754987.85,1755987.77,5920219.76,5921219.64,-1.66,99.83",
                    "+                               pointCount = 4231",
                    "+                                sourceOid = sha256:6b980ce4d7f4978afd3b01e39670e2071a792fba441aca45be69be81cb48b08c",
                    "+                               sourceSize = 51489",
                ],
            )

            r = cli_runner.invoke(["commit", "-m", "Commit new LAZ tile"])
            assert r.exit_code == WORKING_COPY_OR_IMPORT_CONFLICT

            r = cli_runner.invoke(
                ["commit", "--convert-to-dataset-format", "-m", "Commit new LAZ tile"]
            )
            assert r.exit_code == 0, r.stderr

            r = cli_runner.invoke(["show"])
            assert r.exit_code == 0, r.stderr
            output = r.stdout.splitlines()
            assert_lines_almost_equal(
                output[4:-2],
                [
                    "    Commit new LAZ tile",
                    "",
                    "+++ auckland:tile:new",
                    "+                                     name = new.copc.laz",
                    "+                              crs84Extent = 174.73844833207193,174.74945404214898,-36.85123712200056,-36.84206322341377,-1.66,99.83",
                    "+                                   format = laz-1.4/copc-1.0",
                    "+                             nativeExtent = 1754987.85,1755987.77,5920219.76,5921219.64,-1.66,99.83",
                    "+                               pointCount = 4231",
                    "+                                sourceOid = sha256:6b980ce4d7f4978afd3b01e39670e2071a792fba441aca45be69be81cb48b08c",
                ],
            )
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
        env = tool_environment()
        env["GIT_INDEX_FILE"] = str(repo.working_copy.workdir.index_path)

        def get_touched_files():
            # git diff-files never compares OIDs - it just lists files which appear
            # to be dirty based on a different mtime to the mtime in the index.
            cmd = ["git", "diff-files"]
            return (
                subprocess.check_output(
                    cmd, env=env, encoding="utf-8", cwd=repo.workdir_path
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
            "Running fetch with --dry-run: fetching 16 LFS blobs",
            "LFS blob OID:                                                    (Pointer file OID):",
            "11ba773069c7e935735f7076b2fa44334d0bb41c4742d8cd8111f575359a773c (9e46ecc5d5503a77d6b8604a92b1da1372f39c03)",
            "23c4bb0642bf467bb35ece586f5460f7f4d32288832796458bcbe1a928b32fb4 (8b5c7ceaad04b6c459ee4b090c77069a07374f84)",
            "3ba3a4bd4629af7c934c61fa132021bf2b3bdd1a52d981315ce5ecb09d71e10a (194ffbeaafc7f19be592fdd434baf3126189e3b7)",
            "467dbced134249ad341e762737ca42e731f92dadd2d290cf093b68c788aa0067 (d0cc8a1cacb1f9dd1d79542324fd0f3551de2ec3)",
            "64895828ea03ce9cafaef4f387338aab8d498c8eccaef1503b8b3bd97e57c5a3 (ba01f6e0d8a64b920e1d8dbaa563a7a641c164b6)",
            "7041a3ee11a33d750289d44ef4096fd7efcc195958d52f56ab363415f9363e61 (847a284d68b10387ac85ff1aae000f712677b8e4)",
            "7d160940ad3087f610ccf6d41f5b7a49a4425bae61bf0ca59e3693910b5b11d4 (adbbcfa66956fa01cca20e0df97da0f8a40a63b0)",
            "817b6ddadd95166012143df55fa73dd6c5a8b42b603c33d1b6c38f187261096e (364046ba21d4a0154c77a2544348bea9fd6baa93)",
            "9c49d1b59f33fa3f46ca6caf8cfc26e13e7e951758b41d811a9b734918ad1711 (7dcc82fd2e182075b6ece5599aca915ce2df8faf)",
            "a1862450841dede2759af665825403e458dfa551c095d9a65ea6e6765aeae0f7 (8bb24273e1fc68e68dbe40b2e182f6e743af74cf)",
            "a968f575322d6de93ebc10f972a4b20a36f918f4f8f76891da4d67232f3976e4 (3ce5e7e1006946fd03cc9b870a1a70bb73f81901)",
            "add2d011a19b39c0c8d70ed2313ad4955b1e0faf9a24394ab1a103930580a267 (5d62415f8d4d1c314a78ff0725534a3633343cbb)",
            "bf4210be91ea2013ff13961a885cc9b16cb631a5b54cc89276010d1e4adf74e2 (0b89060b81491a2ade1448417ba1509aa73a0a51)",
            "c7874972e856eaff4d28fa851b9abc72be9056caa41187211de0258b5ac30f28 (4d79b37fdfdb80166965c4f7d65ffec5f8ed0f86)",
            "d380a98414ab209f36c7fba4734b02f67de519756e341837217716c5b4768339 (f866ac0ecf4326931d10aaa16140e2240eeada90)",
            "ec80af6cae31be5318f9380cd953b25469bd8ecda25086deca2b831bbb89168a (c76e89f23f512214063d31e7a9c85657f0cf8fb6)",
        ]


def test_lfs_gc(cli_runner, data_archive, monkeypatch):
    with data_archive("point-cloud/auckland.tgz") as repo_path:
        # Delete everything in the local LFS cache.
        for file in (repo_path / "auckland").glob("auckland_3_*.copc.laz"):
            file.unlink()

        r = cli_runner.invoke(["lfs+", "gc", "--dry-run"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            "Running gc with --dry-run: deleting 0 LFS blobs (0B) from the cache"
        ]

        r = cli_runner.invoke(["commit", "-m", "Delete auckland_3_*"])
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["lfs+", "gc", "--dry-run"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            "Can't delete 4 LFS blobs (100KiB) from the cache since they have not been pushed to the remote",
            "Running gc with --dry-run: deleting 0 LFS blobs (0B) from the cache",
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
            "Running gc with --dry-run: deleting 4 LFS blobs (100KiB) from the cache",
            "64895828ea03ce9cafaef4f387338aab8d498c8eccaef1503b8b3bd97e57c5a3",
            "817b6ddadd95166012143df55fa73dd6c5a8b42b603c33d1b6c38f187261096e",
            "d380a98414ab209f36c7fba4734b02f67de519756e341837217716c5b4768339",
            "ec80af6cae31be5318f9380cd953b25469bd8ecda25086deca2b831bbb89168a",
        ]

        r = cli_runner.invoke(["lfs+", "gc"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            "Deleting 4 LFS blobs (100KiB) from the cache..."
        ]

        r = cli_runner.invoke(["lfs+", "gc"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == ["Deleting 0 LFS blobs (0B) from the cache..."]

        r = cli_runner.invoke(["lfs+", "fetch", "HEAD^", "--dry-run"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            "Running fetch with --dry-run: fetching 4 LFS blobs",
            "LFS blob OID:                                                    (Pointer file OID):",
            "64895828ea03ce9cafaef4f387338aab8d498c8eccaef1503b8b3bd97e57c5a3 (ba01f6e0d8a64b920e1d8dbaa563a7a641c164b6)",
            "817b6ddadd95166012143df55fa73dd6c5a8b42b603c33d1b6c38f187261096e (364046ba21d4a0154c77a2544348bea9fd6baa93)",
            "d380a98414ab209f36c7fba4734b02f67de519756e341837217716c5b4768339 (f866ac0ecf4326931d10aaa16140e2240eeada90)",
            "ec80af6cae31be5318f9380cd953b25469bd8ecda25086deca2b831bbb89168a (c76e89f23f512214063d31e7a9c85657f0cf8fb6)",
        ]


def _remove_copy_on_write_warning(stderr_output):
    if len(stderr_output) > 3 and stderr_output[0:3] == [
        "Copy-on-write is not supported on this filesystem.",
        "Currently Kart must create two copies of point cloud tiles to support full distributed version control features.",
        "For more info, see https://docs.kartproject.org/en/latest/pages/git_lfs.html#disk-usage",
    ]:
        return stderr_output[3:]
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
