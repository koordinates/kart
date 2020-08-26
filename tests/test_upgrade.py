import json
import pytest


H = pytest.helpers.helpers()


@pytest.mark.slow
@pytest.mark.parametrize(
    "archive",
    [
        pytest.param("points0.snow", id="points"),
        pytest.param("polygons0.snow", id="polygons-pk"),
        pytest.param("table0.snow", id="table"),
    ],
)
def test_upgrade_v0(archive, data_archive, cli_runner, tmp_path, chdir):
    with data_archive(archive) as source_path:
        r = cli_runner.invoke(["upgrade", source_path, tmp_path / "dest"])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines()[-1] == "Upgrade complete"

    with chdir(tmp_path / "dest"):
        r = cli_runner.invoke(["log"])
        assert r.exit_code == 0, r

        if archive == "points0.snow":
            assert r.stdout.splitlines() == [
                "commit 0c64d8211c072a08d5fc6e6fe898cbb59fc83d16",
                "Author: Robert Coup <robert@coup.net.nz>",
                "Date:   Thu Jun 20 15:28:33 2019 +0100",
                "",
                "    Improve naming on Coromandel East coast",
                "",
                "commit 7bc3b56f20d1559208bcf5bb56860dda6e190b70",
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
def test_upgrade_v1(archive, layer, data_archive, cli_runner, tmp_path, chdir):
    with data_archive(archive) as source_path:
        r = cli_runner.invoke(["status", "--output-format=json"])
        assert r.exit_code == 0, r
        src_branch = json.loads(r.stdout)["sno.status/v1"]["branch"]

        r = cli_runner.invoke(["upgrade", source_path, tmp_path / "dest"])
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
