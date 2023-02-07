from kart.repo import KartRepo


def test_import_single_geotiff(
    tmp_path,
    chdir,
    cli_runner,
    data_archive_readonly,
    check_lfs_hashes,
    requires_git_lfs,
):
    with data_archive_readonly("raster/aerial.tgz") as aerial:
        repo_path = tmp_path / "raster-repo"
        r = cli_runner.invoke(["init", repo_path])
        assert r.exit_code == 0, r.stderr

        repo = KartRepo(repo_path)
        with chdir(repo_path):
            r = cli_runner.invoke(["import", f"{aerial}/aerial.tif"])
            assert r.exit_code == 0, r.stderr

            check_lfs_hashes(repo, 1)

            r = cli_runner.invoke(["data", "ls"])
            assert r.exit_code == 0, r.stderr
            assert r.stdout.splitlines() == ["aerial"]

            r = cli_runner.invoke(["show"])
            assert r.exit_code == 0, r.stderr

            # NOTE: this particular format is still subject to change.
            assert r.stdout.splitlines()[6:] == [
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
                "+++ aerial:meta:format.json",
                "+ {",
                '+   "fileType": "image/tiff; application=geotiff"',
                "+ }",
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
                "+++ aerial:tile:aerial",
                "+                                     name = aerial.tiff",
                "+                              crs84Extent = POLYGON((175.1890852 -36.7923968,175.1892991 -36.7999096,175.1988427 -36.7997334,175.1986279 -36.7922207,175.1890852 -36.7923968))",
                "+                                   extent = POLYGON((1795318.0 5925922.0,1795318.0 5925088.0,1796170.0 5925088.0,1796170.0 5925922.0))",
                "+                                   format = geotiff",
                "+                                   pixels = 426x417",
                "+                                      oid = sha256:e6cbc8210f9cae3c8b72985e553e97af51fb9c20d17f5a06b7579943fed57b2c",
                "+                                     size = 516216",
            ]
