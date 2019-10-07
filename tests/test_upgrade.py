import pytest


H = pytest.helpers.helpers()


@pytest.mark.slow
@pytest.mark.parametrize(
    "archive,layer",
    [
        pytest.param("points0.snow", H.POINTS_LAYER, id="points"),
        pytest.param("polygons0.snow", H.POLYGONS_LAYER, id="polygons-pk"),
        pytest.param("table0.snow", H.TABLE_LAYER, id="table"),
    ],
)
def test_upgrade(archive, layer, data_archive, cli_runner, tmp_path, chdir):
    with data_archive(archive) as source_path:
        r = cli_runner.invoke(["upgrade", "00-02", source_path, tmp_path / 'dest', layer])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines()[-1] == "Upgrade complete"

    with chdir(tmp_path / 'dest'):
        r = cli_runner.invoke(["log"])
        assert r.exit_code == 0, r

        if layer == H.POINTS_LAYER:
            assert r.stdout.splitlines() == [
                "commit e60e7190fb3b6d5b79dec648f3e4133b0edd9815",
                "Author: Robert Coup <robert@coup.net.nz>",
                "Date:   Thu Jun 20 15:28:33 2019 +0100",
                "",
                "    Improve naming on Coromandel East coast",
                "",
                "commit 761512322dd6a19767d95673bf14824d451d01bf",
                "Author: Robert Coup <robert@coup.net.nz>",
                "Date:   Tue Jun 11 12:03:58 2019 +0100",
                "",
                "    Import from nz-pa-points-topo-150k.gpkg",
            ]
