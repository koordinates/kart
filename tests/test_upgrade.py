import json
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
        r = cli_runner.invoke(["status", "--output-format=json"])
        assert r.exit_code == 0, r
        src_branch = json.loads(r.stdout)["sno.status/v1"]["branch"]

        r = cli_runner.invoke(["upgrade", "02-05", source_path, tmp_path / "dest"])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines()[-1] == "Upgrade complete"

    with chdir(tmp_path / "dest"):
        r = cli_runner.invoke(["log"])
        assert r.exit_code == 0, r

        if layer == H.POINTS.LAYER:
            assert r.stdout.splitlines() == [
                'commit 0c64d8211c072a08d5fc6e6fe898cbb59fc83d16',
                'Author: Robert Coup <robert@coup.net.nz>',
                'Date:   Thu Jun 20 15:28:33 2019 +0100',
                '',
                '    Improve naming on Coromandel East coast',
                '',
                'commit 7bc3b56f20d1559208bcf5bb56860dda6e190b70',
                'Author: Robert Coup <robert@coup.net.nz>',
                'Date:   Tue Jun 11 12:03:58 2019 +0100',
                '',
                '    Import from nz-pa-points-topo-150k.gpkg',
            ]

        r = cli_runner.invoke(["status", "--output-format=json"])
        assert r.exit_code == 0, r
        dest_branch = json.loads(r.stdout)["sno.status/v1"]["branch"]
        assert dest_branch == src_branch


def test_upgrade_list(cli_runner):
    r = cli_runner.invoke(["upgrade"])
    assert r.exit_code == 0, r
    assert r.stdout.splitlines()[-3:] == [
        "Commands:",
        "  00-02  Upgrade a v0.0/v0.1 Sno repository to Sno v0.2",
        "  02-05  Upgrade a v0.2/v0.3/v0.4 Sno repository to Sno v0.5",
    ]
