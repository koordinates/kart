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
                "commit a91ac9bf11cf6a07647fdc8700dbcfab95685804",
                "Author: Robert Coup <robert@coup.net.nz>",
                "Date:   Thu Jun 20 15:28:33 2019 +0100",
                "",
                "    Improve naming on Coromandel East coast",
                "",
                "commit 4c2fabf6c58ecfede957f2205caa4f85ac6b56ad",
                "Author: Robert Coup <robert@coup.net.nz>",
                "Date:   Tue Jun 11 12:03:58 2019 +0100",
                "",
                "    Import from nz-pa-points-topo-150k.gpkg",
            ]


def test_upgrade_list(cli_runner):
    r = cli_runner.invoke(["upgrade"])
    assert r.exit_code == 0, r
    assert r.stdout.splitlines()[-1] == "  00-02  Upgrade a v0.0/v0.1 Snowdrop repository to Sno v0.2"
