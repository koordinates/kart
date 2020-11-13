import json
import pytest

from sno.sno_repo import SnoRepo


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
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines()[-1] == "Upgrade complete"

    with chdir(tmp_path / "dest"):
        r = cli_runner.invoke(["log"])
        assert r.exit_code == 0, r.stderr

        if archive == "points0.snow":
            assert r.stdout.splitlines() == [
                "commit e0f39729ffe37b9f858afe0783ff0a29c98d699d",
                "Author: Robert Coup <robert@coup.net.nz>",
                "Date:   Thu Jun 20 15:28:33 2019 +0100",
                "",
                "    Improve naming on Coromandel East coast",
                "",
                "commit 31f8edfc3bfb660c36e0568d67722affd71813eb",
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
        assert r.exit_code == 0, r.stderr
        src_branch = json.loads(r.stdout)["sno.status/v1"]["branch"]

        r = cli_runner.invoke(["upgrade", source_path, tmp_path / "dest"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines()[-1] == "Upgrade complete"

    with chdir(tmp_path / "dest"):
        r = cli_runner.invoke(["log"])
        assert r.exit_code == 0, r.stderr

        if layer == H.POINTS.LAYER:
            assert r.stdout.splitlines() == [
                "commit e0f39729ffe37b9f858afe0783ff0a29c98d699d",
                "Author: Robert Coup <robert@coup.net.nz>",
                "Date:   Thu Jun 20 15:28:33 2019 +0100",
                "",
                "    Improve naming on Coromandel East coast",
                "",
                "commit 31f8edfc3bfb660c36e0568d67722affd71813eb",
                "Author: Robert Coup <robert@coup.net.nz>",
                "Date:   Tue Jun 11 12:03:58 2019 +0100",
                "",
                "    Import from nz-pa-points-topo-150k.gpkg",
            ]

        r = cli_runner.invoke(["status", "--output-format=json"])
        assert r.exit_code == 0, r
        dest_branch = json.loads(r.stdout)["sno.status/v1"]["branch"]
        assert dest_branch == src_branch


@pytest.mark.parametrize(
    "archive,layer",
    [
        pytest.param("points", H.POINTS.LAYER, id="points"),
        pytest.param("polygons", H.POLYGONS.LAYER, id="polygons-pk"),
        pytest.param("table", H.TABLE.LAYER, id="table"),
    ],
)
def test_upgrade_to_tidy(archive, layer, data_archive, cli_runner, chdir):
    with data_archive(archive) as source_path:
        r = cli_runner.invoke(["upgrade-to-tidy", source_path])
        assert r.exit_code == 0, r.stderr
        assert (
            r.stdout.splitlines()[-1] == "In-place upgrade complete: repo is now tidy"
        )

        repo = SnoRepo(source_path)
        assert repo.is_tidy_style_sno_repo()

        with chdir(source_path):
            r = cli_runner.invoke(["log"])
            assert r.exit_code == 0, r

            if layer == H.POINTS.LAYER:
                assert r.stdout.splitlines() == [
                    "commit 2a1b7be8bdef32aea1510668e3edccbc6d454852",
                    "Author: Robert Coup <robert@coup.net.nz>",
                    "Date:   Thu Jun 20 15:28:33 2019 +0100",
                    "",
                    "    Improve naming on Coromandel East coast",
                    "",
                    "commit 63a9492dd785b1f04dfc446330fa017f9459db4f",
                    "Author: Robert Coup <robert@coup.net.nz>",
                    "Date:   Tue Jun 11 12:03:58 2019 +0100",
                    "",
                    "    Import from nz-pa-points-topo-150k.gpkg",
                ]

        children = set(child.name for child in source_path.iterdir())
        assert children == {".git", ".sno"}

        assert (source_path / ".git").is_file()
        assert (source_path / ".sno").is_dir()
        assert (source_path / ".sno" / "HEAD").is_file()
