import json
import pytest
from pathlib import Path

from sno.cli import get_version
from sno.exceptions import UNSUPPORTED_VERSION
from sno.repo import SnoRepo


H = pytest.helpers.helpers()


@pytest.mark.slow
@pytest.mark.parametrize(
    "archive",
    [
        pytest.param("points0.snow.tgz", id="points"),
        pytest.param("polygons0.snow.tgz", id="polygons"),
        pytest.param("table0.snow.tgz", id="table"),
    ],
)
def test_upgrade_v0(archive, data_archive_readonly, cli_runner, tmp_path, chdir):
    archive_path = Path("upgrade") / "v0" / archive
    with data_archive_readonly(archive_path) as source_path:
        r = cli_runner.invoke(["data", "version", "--output-format=json"])
        assert r.exit_code == 0, r.stderr
        assert json.loads(r.stdout) == {"kart.data.version": 0}

        r = cli_runner.invoke(["log"])
        assert r.exit_code == UNSUPPORTED_VERSION
        assert "This Kart repo uses Datasets v0" in r.stderr
        assert f"Kart {get_version()} only supports Datasets v2" in r.stderr

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
        pytest.param("points.tgz", H.POINTS.LAYER, id="points"),
        pytest.param("polygons.tgz", H.POLYGONS.LAYER, id="polygons"),
        pytest.param("table.tgz", H.TABLE.LAYER, id="table"),
    ],
)
def test_upgrade_v1(archive, layer, data_archive_readonly, cli_runner, tmp_path, chdir):
    archive_path = Path("upgrade") / "v1" / archive
    with data_archive_readonly(archive_path) as source_path:
        r = cli_runner.invoke(["data", "version", "--output-format=json"])
        assert r.exit_code == 0, r.stderr
        assert json.loads(r.stdout) == {"kart.data.version": 1}

        r = cli_runner.invoke(["log"])
        assert r.exit_code == UNSUPPORTED_VERSION
        assert "This Kart repo uses Datasets v1" in r.stderr
        assert f"Kart {get_version()} only supports Datasets v2" in r.stderr

        r = cli_runner.invoke(["upgrade", source_path, tmp_path / "dest"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines()[-1] == "Upgrade complete"

    with chdir(tmp_path / "dest"):
        r = cli_runner.invoke(["log"])
        assert r.exit_code == 0, r.stderr

        if layer == H.POINTS.LAYER:
            assert r.stdout.splitlines() == [
                "commit e04c6466e7420fe8e4ea2180bbaf5a041a8bff0f",
                "Author: Robert Coup <robert@coup.net.nz>",
                "Date:   Thu Jun 20 15:28:33 2019 +0100",
                "",
                "    Improve naming on Coromandel East coast",
                "",
                "commit 0924b05a94c26e86cfbbae98416513e39e17a2be",
                "Author: Robert Coup <robert@coup.net.nz>",
                "Date:   Tue Jun 11 12:03:58 2019 +0100",
                "",
                "    Import from nz-pa-points-topo-150k.gpkg",
            ]

        r = cli_runner.invoke(["status", "--output-format=json"])
        assert r.exit_code == 0, r


def test_upgrade_to_tidy(data_archive, cli_runner, chdir):
    with data_archive("old-bare") as source_path:
        r = cli_runner.invoke(["upgrade-to-tidy", source_path])
        assert r.exit_code == 0, r.stderr
        assert (
            r.stdout.splitlines()[-1] == "In-place upgrade complete: repo is now tidy"
        )

        repo = SnoRepo(source_path)
        assert repo.is_tidy_style

        with chdir(source_path):
            r = cli_runner.invoke(["log"])
            assert r.exit_code == 0, r

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

        children = set(child.name for child in source_path.iterdir())
        assert children == {".git", ".sno", "old-bare.gpkg"}

        assert (source_path / ".git").is_file()
        assert (source_path / ".sno").is_dir()
        assert (source_path / ".sno" / "HEAD").is_file()


def test_upgrade_to_kart(data_working_copy, cli_runner, chdir):
    with data_working_copy("polygons") as (source_path, wc_path):
        r = cli_runner.invoke(["upgrade-to-kart", source_path])
        assert r.exit_code == 0, r.stderr
        assert (
            r.stdout.splitlines()[-1]
            == "In-place upgrade complete: Sno repo is now Kart repo"
        )

        repo = SnoRepo(source_path)
        assert repo.is_kart_branded
        assert repo.config["kart.repostructure.version"] == "2"

        with chdir(source_path):
            r = cli_runner.invoke(["status"])
            assert r.exit_code == 0, r
            assert r.stdout.splitlines() == [
                "On branch main",
                "",
                "Nothing to commit, working copy clean",
            ]

            r = cli_runner.invoke(["log"])
            assert r.exit_code == 0, r

            assert r.stdout.splitlines() == [
                "commit 5bb25a2da966b15ae7743db4666c1599001e2443",
                "Author: Robert Coup <robert@coup.net.nz>",
                "Date:   Mon Jul 22 12:05:39 2019 +0100",
                "",
                "    Import from nz-waca-adjustments.gpkg",
            ]

        children = set(child.name for child in source_path.iterdir())
        assert children == {"KART_README.txt", ".git", ".kart"}

        assert (source_path / ".git").is_file()
        assert (source_path / ".kart").is_dir()
        assert (source_path / ".kart" / "HEAD").is_file()
        assert (source_path / "KART_README.txt").is_file()

        with repo.working_copy.session() as sess:
            assert (
                sess.scalar(
                    "SELECT count(*) FROM sqlite_master WHERE type='table' AND name LIKE ('gpkg_kart%');",
                )
                == 2
            )

            assert (
                sess.scalar(
                    "SELECT count(*) FROM sqlite_master WHERE type='table' AND name LIKE ('gpkg_sno%');",
                )
                == 0
            )
