import pytest

from kart.exceptions import INVALID_ARGUMENT, NO_CHANGES
from kart.lfs_util import get_hash_and_size_of_file
from kart.repo import KartRepo
from .fixtures import requires_gdal_info  # noqa

AERIAL_CRS_DIFF = [
    "+++ aerial:meta:crs.wkt",
    '+ PROJCRS["NZGD2000 / New Zealand Transverse Mercator 2000",',
    '+     BASEGEOGCRS["NZGD2000",',
    '+         DATUM["New Zealand Geodetic Datum 2000",',
    '+             ELLIPSOID["GRS 1980", 6378137, 298.257222101,',
    '+                 LENGTHUNIT["metre", 1]]],',
    '+         PRIMEM["Greenwich", 0,',
    '+             ANGLEUNIT["degree", 0.0174532925199433]],',
    '+         ID["EPSG", 4167]],',
    '+     CONVERSION["New Zealand Transverse Mercator 2000",',
    '+         METHOD["Transverse Mercator",',
    '+             ID["EPSG", 9807]],',
    '+         PARAMETER["Latitude of natural origin", 0,',
    '+             ANGLEUNIT["degree", 0.0174532925199433],',
    '+             ID["EPSG", 8801]],',
    '+         PARAMETER["Longitude of natural origin", 173,',
    '+             ANGLEUNIT["degree", 0.0174532925199433],',
    '+             ID["EPSG", 8802]],',
    '+         PARAMETER["Scale factor at natural origin", 0.9996,',
    '+             SCALEUNIT["unity", 1],',
    '+             ID["EPSG", 8805]],',
    '+         PARAMETER["False easting", 1600000,',
    '+             LENGTHUNIT["metre", 1],',
    '+             ID["EPSG", 8806]],',
    '+         PARAMETER["False northing", 10000000,',
    '+             LENGTHUNIT["metre", 1],',
    '+             ID["EPSG", 8807]]],',
    "+     CS[Cartesian, 2],",
    '+     AXIS["northing (N)", north,',
    "+         ORDER[1],",
    '+         LENGTHUNIT["metre", 1]],',
    '+     AXIS["easting (E)", east,',
    "+         ORDER[2],",
    '+         LENGTHUNIT["metre", 1]],',
    '+     USAGE[SCOPE["Engineering survey, topographic mapping."],',
    '+     AREA["New Zealand - North Island, South Island, Stewart Island - onshore."],',
    "+     BBOX[-47.33, 166.37, -34.1, 178.63]],",
    '+     ID["EPSG", 2193]]',
    "+ ",
]

AERIAL_SCHEMA_DIFF = [
    "+++ aerial:meta:schema.json",
    "+ [",
    "+   {",
    '+     "dataType": "integer",',
    '+     "size": 8,',
    '+     "interpretation": "red",',
    '+     "unsigned": true',
    "+   },",
    "+   {",
    '+     "dataType": "integer",',
    '+     "size": 8,',
    '+     "interpretation": "green",',
    '+     "unsigned": true',
    "+   },",
    "+   {",
    '+     "dataType": "integer",',
    '+     "size": 8,',
    '+     "interpretation": "blue",',
    '+     "unsigned": true',
    "+   },",
    "+   {",
    '+     "dataType": "integer",',
    '+     "size": 8,',
    '+     "interpretation": "alpha",',
    '+     "unsigned": true',
    "+   }",
    "+ ]",
]


@pytest.mark.parametrize(
    "convert_option,convert_prompt_input",
    [
        ("--preserve-format", None),
        (None, "no"),
        ("--convert-to-cog", None),
        (None, "yes"),
    ],
)
def test_import_single_non_cog_geotiff(
    convert_option,
    convert_prompt_input,
    tmp_path,
    chdir,
    cli_runner,
    data_archive_readonly,
    check_lfs_hashes,
    requires_gdal_info,
    requires_git_lfs,
):
    with data_archive_readonly("raster/tif-aerial.tgz") as aerial:
        repo_path = tmp_path / "raster-repo"
        r = cli_runner.invoke(["init", repo_path])
        assert r.exit_code == 0, r.stderr

        repo = KartRepo(repo_path)

        with chdir(repo_path):
            import_cmd = ["import", f"{aerial}/aerial.tif"]
            if convert_option is not None:
                import_cmd.append(convert_option)

            r = cli_runner.invoke(import_cmd, input=convert_prompt_input)
            assert r.exit_code == 0, r.stderr

            check_lfs_hashes(repo, 1)

            r = cli_runner.invoke(["data", "ls"])
            assert r.exit_code == 0, r.stderr
            assert r.stdout.splitlines() == ["aerial"]

            r = cli_runner.invoke(["show"])
            assert r.exit_code == 0, r.stderr

            if convert_option == "--preserve-format" or convert_prompt_input == "no":
                assert r.stdout.splitlines()[6:] == AERIAL_CRS_DIFF + [
                    "+++ aerial:meta:format.json",
                    "+ {",
                    '+   "fileType": "geotiff"',
                    "+ }",
                ] + AERIAL_SCHEMA_DIFF + [
                    "+++ aerial:tile:aerial",
                    "+                                     name = aerial.tif",
                    "+                              crs84Extent = POLYGON((175.1890852 -36.7923968,175.1892991 -36.7999096,175.1988427 -36.7997334,175.1986279 -36.7922207,175.1890852 -36.7923968,175.1890852 -36.7923968))",
                    "+                               dimensions = 426x417",
                    "+                                   format = geotiff",
                    "+                             nativeExtent = POLYGON((1795318.0 5925922.0,1795318.0 5925088.0,1796170.0 5925088.0,1796170.0 5925922.0,1795318.0 5925922.0))",
                    "+                                      oid = sha256:bdbb58a399b60231f7a017fd76659efb0f5c1d82ab892248123d14d9a1e838e1",
                    "+                                     size = 393860",
                ]

            elif convert_option == "--convert-to-cog" or convert_prompt_input == "yes":
                assert r.stdout.splitlines()[6:] == AERIAL_CRS_DIFF + [
                    "+++ aerial:meta:format.json",
                    "+ {",
                    '+   "fileType": "geotiff",',
                    '+   "profile": "cloud-optimized"',
                    "+ }",
                ] + AERIAL_SCHEMA_DIFF + [
                    "+++ aerial:tile:aerial",
                    "+                                     name = aerial.tif",
                    "+                              crs84Extent = POLYGON((175.1890852 -36.7923968,175.1892991 -36.7999096,175.1988427 -36.7997334,175.1986279 -36.7922207,175.1890852 -36.7923968,175.1890852 -36.7923968))",
                    "+                               dimensions = 426x417",
                    "+                                   format = geotiff/cog",
                    "+                             nativeExtent = POLYGON((1795318.0 5925922.0,1795318.0 5925088.0,1796170.0 5925088.0,1796170.0 5925922.0,1795318.0 5925922.0))",
                    "+                                sourceOid = sha256:bdbb58a399b60231f7a017fd76659efb0f5c1d82ab892248123d14d9a1e838e1",
                    "+                                      oid = sha256:b5a949f332d2d5afbfe9c164a4060e130c7d95d77aa3d48780c2adffc12ff36b",
                    "+                                     size = 552340",
                ]

            import_cmd += ["--replace-existing"]
            r = cli_runner.invoke(import_cmd, input=convert_prompt_input)
            assert r.exit_code == NO_CHANGES


def test_import_no_decision_on_cog(
    tmp_path,
    chdir,
    cli_runner,
    data_archive_readonly,
):
    with data_archive_readonly("raster/tif-aerial.tgz") as aerial:
        repo_path = tmp_path / "raster-repo"
        r = cli_runner.invoke(["init", repo_path])
        assert r.exit_code == 0, r.stderr

        with chdir(repo_path):
            r = cli_runner.invoke(["import", f"{aerial}/aerial.tif"])
            assert r.exit_code == INVALID_ARGUMENT
            assert "Choose dataset subtype" in r.stderr


def test_import_single_cogtiff(
    tmp_path,
    chdir,
    cli_runner,
    data_archive_readonly,
    check_lfs_hashes,
    requires_gdal_info,
    requires_git_lfs,
):
    with data_archive_readonly("raster/cog-aerial.tgz") as aerial:
        repo_path = tmp_path / "raster-repo"
        r = cli_runner.invoke(["init", repo_path])
        assert r.exit_code == 0, r.stderr

        repo = KartRepo(repo_path)
        with chdir(repo_path):
            r = cli_runner.invoke(
                ["import", f"{aerial}/aerial.tif", "--convert-to-cog"]
            )
            assert r.exit_code == 0, r.stderr

            check_lfs_hashes(repo, 1)

            r = cli_runner.invoke(["data", "ls"])
            assert r.exit_code == 0, r.stderr
            assert r.stdout.splitlines() == ["aerial"]

            r = cli_runner.invoke(["show"])
            assert r.exit_code == 0, r.stderr

            assert r.stdout.splitlines()[6:] == AERIAL_CRS_DIFF + [
                "+++ aerial:meta:format.json",
                "+ {",
                '+   "fileType": "geotiff",',
                '+   "profile": "cloud-optimized"',
                "+ }",
            ] + AERIAL_SCHEMA_DIFF + [
                "+++ aerial:tile:aerial",
                "+                                     name = aerial.tif",
                "+                              crs84Extent = POLYGON((175.1890852 -36.7923968,175.1892991 -36.7999096,175.1988427 -36.7997334,175.1986279 -36.7922207,175.1890852 -36.7923968,175.1890852 -36.7923968))",
                "+                               dimensions = 426x417",
                "+                                   format = geotiff/cog",
                "+                             nativeExtent = POLYGON((1795318.0 5925922.0,1795318.0 5925088.0,1796170.0 5925088.0,1796170.0 5925922.0,1795318.0 5925922.0))",
                "+                                      oid = sha256:e6cbc8210f9cae3c8b72985e553e97af51fb9c20d17f5a06b7579943fed57b2c",
                "+                                     size = 516216",
            ]


@pytest.mark.parametrize(
    "pam_filename",
    [
        "erorisk_silcdb4.tif.aux.xml",
        "ERORISK_SILCDB4.tif.aux.xml",
        "erorisk_silcdb4.TIF.AUX.XML",
    ],
)
def test_import_single_geotiff_with_rat(
    pam_filename,
    tmp_path,
    chdir,
    cli_runner,
    data_archive,
    check_lfs_hashes,
    requires_gdal_info,
    requires_git_lfs,
):
    with data_archive("raster/cog-erosion.tgz") as erosion:
        # The PAM file should be found in a case-insensitive way
        # but always imported to have a name that perfectly matches the TIF file.
        (erosion / "erorisk_silcdb4.tif.aux.xml").rename(erosion / pam_filename)

        repo_path = tmp_path / "raster-repo"
        r = cli_runner.invoke(["init", repo_path])
        assert r.exit_code == 0, r.stderr

        repo = KartRepo(repo_path)
        with chdir(repo_path):
            r = cli_runner.invoke(
                ["import", f"{erosion}/erorisk_silcdb4.tif", "--convert-to-cog"]
            )
            assert r.exit_code == 0, r.stderr

            check_lfs_hashes(repo, 2)

            r = cli_runner.invoke(["data", "ls"])
            assert r.exit_code == 0, r.stderr
            assert r.stdout.splitlines() == ["erorisk_silcdb4"]

            r = cli_runner.invoke(["show"])
            assert r.exit_code == 0, r.stderr

            assert r.stdout.splitlines()[6:] == [
                "+++ erorisk_silcdb4:meta:band/band-1-categories.json",
                "+ {",
                '+   "1": "High landslide risk - delivery to stream",',
                '+   "2": "High landslide risk - non-delivery to steam",',
                '+   "3": "Moderate earthflow risk",',
                '+   "4": "Severe earthflow risk",',
                '+   "5": "Gully risk"',
                "+ }",
                "+++ erorisk_silcdb4:meta:band/band-1-rat.xml",
                '+ <GDALRasterAttributeTable Row0Min="0" BinSize="1" tableType="thematic">',
                '+     <FieldDefn index="0">',
                "+         <Name>Histogram</Name>",
                "+         <Type>1</Type>",
                "+         <Usage>1</Usage>",
                "+     </FieldDefn>",
                '+     <FieldDefn index="1">',
                "+         <Name>Class_Names</Name>",
                "+         <Type>2</Type>",
                "+         <Usage>2</Usage>",
                "+     </FieldDefn>",
                '+     <FieldDefn index="2">',
                "+         <Name>Red</Name>",
                "+         <Type>0</Type>",
                "+         <Usage>6</Usage>",
                "+     </FieldDefn>",
                '+     <FieldDefn index="3">',
                "+         <Name>Green</Name>",
                "+         <Type>0</Type>",
                "+         <Usage>7</Usage>",
                "+     </FieldDefn>",
                '+     <FieldDefn index="4">',
                "+         <Name>Blue</Name>",
                "+         <Type>0</Type>",
                "+         <Usage>8</Usage>",
                "+     </FieldDefn>",
                '+     <FieldDefn index="5">',
                "+         <Name>Opacity</Name>",
                "+         <Type>0</Type>",
                "+         <Usage>9</Usage>",
                "+     </FieldDefn>",
                "+ </GDALRasterAttributeTable>",
                "+ ",
                "+++ erorisk_silcdb4:meta:crs.wkt",
                '+ PROJCRS["NZGD2000 / New Zealand Transverse Mercator 2000",',
                '+     BASEGEOGCRS["NZGD2000",',
                '+         DATUM["New Zealand Geodetic Datum 2000",',
                '+             ELLIPSOID["GRS 1980", 6378137, 298.257222101,',
                '+                 LENGTHUNIT["metre", 1]]],',
                '+         PRIMEM["Greenwich", 0,',
                '+             ANGLEUNIT["degree", 0.0174532925199433]],',
                '+         ID["EPSG", 4167]],',
                '+     CONVERSION["New Zealand Transverse Mercator 2000",',
                '+         METHOD["Transverse Mercator",',
                '+             ID["EPSG", 9807]],',
                '+         PARAMETER["Latitude of natural origin", 0,',
                '+             ANGLEUNIT["degree", 0.0174532925199433],',
                '+             ID["EPSG", 8801]],',
                '+         PARAMETER["Longitude of natural origin", 173,',
                '+             ANGLEUNIT["degree", 0.0174532925199433],',
                '+             ID["EPSG", 8802]],',
                '+         PARAMETER["Scale factor at natural origin", 0.9996,',
                '+             SCALEUNIT["unity", 1],',
                '+             ID["EPSG", 8805]],',
                '+         PARAMETER["False easting", 1600000,',
                '+             LENGTHUNIT["metre", 1],',
                '+             ID["EPSG", 8806]],',
                '+         PARAMETER["False northing", 10000000,',
                '+             LENGTHUNIT["metre", 1],',
                '+             ID["EPSG", 8807]]],',
                "+     CS[Cartesian, 2],",
                '+     AXIS["northing (N)", north,',
                "+         ORDER[1],",
                '+         LENGTHUNIT["metre", 1]],',
                '+     AXIS["easting (E)", east,',
                "+         ORDER[2],",
                '+         LENGTHUNIT["metre", 1]],',
                '+     USAGE[SCOPE["Engineering survey, topographic mapping."],',
                '+     AREA["New Zealand - North Island, South Island, Stewart Island - onshore."],',
                "+     BBOX[-47.33, 166.37, -34.1, 178.63]],",
                '+     ID["EPSG", 2193]]',
                "+ ",
                "+++ erorisk_silcdb4:meta:format.json",
                "+ {",
                '+   "fileType": "geotiff",',
                '+   "profile": "cloud-optimized"',
                "+ }",
                "+++ erorisk_silcdb4:meta:schema.json",
                "+ [",
                "+   {",
                '+     "dataType": "integer",',
                '+     "size": 8,',
                '+     "description": "erorisk_si",',
                '+     "interpretation": "palette",',
                '+     "unsigned": true',
                "+   }",
                "+ ]",
                "+++ erorisk_silcdb4:tile:erorisk_silcdb4",
                "+                                     name = erorisk_silcdb4.tif",
                "+                              crs84Extent = POLYGON((172.6754107 -43.7555641,172.6748326 -43.8622096,172.8170036 -43.8625257,172.8173289 -43.755879,172.6754107 -43.7555641,172.6754107 -43.7555641))",
                "+                               dimensions = 762x790",
                "+                                   format = geotiff/cog",
                "+                             nativeExtent = POLYGON((1573869.73 5155224.347,1573869.73 5143379.674,1585294.591 5143379.674,1585294.591 5155224.347,1573869.73 5155224.347))",
                "+                                      oid = sha256:c4bbea4d7cfd54f4cdbca887a1b358a81710e820a6aed97cdf3337fd3e14f5aa",
                "+                                     size = 604652",
                "+                                  pamName = erorisk_silcdb4.tif.aux.xml",
                "+                                   pamOid = sha256:d8f514e654a81bdcd7428886a15e300c56b5a5ff92898315d16757562d2968ca",
                "+                                  pamSize = 36908",
            ]

            tif = repo_path / "erorisk_silcdb4" / "erorisk_silcdb4.tif"
            assert tif.is_file()
            assert get_hash_and_size_of_file(tif) == (
                "c4bbea4d7cfd54f4cdbca887a1b358a81710e820a6aed97cdf3337fd3e14f5aa",
                604652,
            )
            # At this point the weird case of the PAM file has been normalised, since we
            # a) forced its basename to match the basename of the TIFF file, and
            # b) normalise all extensions in the working copy (to lowercase).
            pam = repo_path / "erorisk_silcdb4" / "erorisk_silcdb4.tif.aux.xml"
            assert pam.is_file()
            assert get_hash_and_size_of_file(pam) == (
                "d8f514e654a81bdcd7428886a15e300c56b5a5ff92898315d16757562d2968ca",
                36908,
            )
