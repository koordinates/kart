import pytest


H = pytest.helpers.helpers()


@pytest.mark.slow
@pytest.mark.parametrize(
    "archive,layer",
    [
        pytest.param("points0.snow", H.POINTS.LAYER, id="points"),
        pytest.param("polygons0.snow", H.POLYGONS.LAYER, id="polygons-pk"),
        pytest.param("table0.snow", H.TABLE.LAYER, id="table"),
    ],
)
def test_upgrade_00_02(archive, layer, data_archive, cli_runner, tmp_path, chdir):
    with data_archive(archive) as source_path:
        r = cli_runner.invoke(
            ["upgrade", "00-02", source_path, tmp_path / "dest", layer]
        )
        assert r.exit_code == 0, r
        assert r.stdout.splitlines()[-1] == "Upgrade complete"

    with chdir(tmp_path / "dest"):
        r = cli_runner.invoke(["log"])
        assert r.exit_code == 0, r

        if layer == H.POINTS.LAYER:
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


@pytest.mark.slow
@pytest.mark.parametrize(
    "archive,layer",
    [
        pytest.param("points", H.POINTS.LAYER, id="points"),
        pytest.param("polygons", H.POLYGONS.LAYER, id="polygons-pk"),
        pytest.param("table", H.TABLE.LAYER, id="table"),
    ],
)
def test_upgrade_02_05(archive, layer, data_archive, cli_runner, tmp_path, chdir):
    with data_archive(archive) as source_path:
        r = cli_runner.invoke(["upgrade", "02-05", source_path, tmp_path / "dest"])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines()[-1] == "Upgrade complete"

    with chdir(tmp_path / "dest"):
        r = cli_runner.invoke(["log"])
        assert r.exit_code == 0, r

        if layer == H.POINTS.LAYER:
            assert r.stdout.splitlines() == [
                "commit 6e2585c58a9294829aa5e6c17b31b9e53506846b",
                "Author: Robert Coup <robert@coup.net.nz>",
                "Date:   Thu Jun 20 15:28:33 2019 +0100",
                "",
                "    Improve naming on Coromandel East coast",
                "",
                "commit 324d00318085d2e7226b584916c862bcc41d43cd",
                "Author: Robert Coup <robert@coup.net.nz>",
                "Date:   Tue Jun 11 12:03:58 2019 +0100",
                "",
                "    Import from nz-pa-points-topo-150k.gpkg",
            ]


def test_upgrade_list(cli_runner):
    r = cli_runner.invoke(["upgrade"])
    assert r.exit_code == 0, r
    assert r.stdout.splitlines()[-3:] == [
        "Commands:",
        "  00-02  Upgrade a v0.0/v0.1 Sno repository to Sno v0.2",
        "  02-05  Upgrade a v0.2/v0.3/v0.4 Sno repository to Sno v0.5",
    ]
