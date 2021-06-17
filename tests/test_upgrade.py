import json
import subprocess
from pathlib import Path

import pytest

from kart.cli import get_version
from kart.exceptions import UNSUPPORTED_VERSION
from kart.repo import KartRepo


H = pytest.helpers.helpers()

POINTS_UPGRADE_RESULT = [
    "commit 1582725544d9122251acd4b3fc75b5c88ac3fd17",
    "Author: Robert Coup <robert@coup.net.nz>",
    "Date:   Thu Jun 20 15:28:33 2019 +0100",
    "",
    "    Improve naming on Coromandel East coast",
    "",
    "commit 6e2984a28150330a6c51019a70f9e8fcfe405e8c",
    "Author: Robert Coup <robert@coup.net.nz>",
    "Date:   Tue Jun 11 12:03:58 2019 +0100",
    "",
    "    Import from nz-pa-points-topo-150k.gpkg",
]


@pytest.mark.slow
@pytest.mark.parametrize(
    "archive,layer",
    [
        pytest.param("points0.snow.tgz", H.POINTS.LAYER, id="points"),
        pytest.param("polygons0.snow.tgz", H.POLYGONS.LAYER, id="polygons"),
        pytest.param("table0.snow.tgz", H.TABLE.LAYER, id="table"),
    ],
)
def test_upgrade_v0(archive, layer, data_archive_readonly, cli_runner, tmp_path, chdir):
    archive_path = Path("upgrade") / "v0" / archive
    with data_archive_readonly(archive_path) as source_path:
        r = cli_runner.invoke(["data", "version", "--output-format=json"])
        assert r.exit_code == 0, r.stderr
        assert json.loads(r.stdout) == {
            "repostructure.version": 0,
            "localconfig.branding": "sno",
        }

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

        if layer == H.POINTS.LAYER:
            assert r.stdout.splitlines() == POINTS_UPGRADE_RESULT


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
        assert json.loads(r.stdout) == {
            "repostructure.version": 1,
            "localconfig.branding": "sno",
        }

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
            assert r.stdout.splitlines() == POINTS_UPGRADE_RESULT

        r = cli_runner.invoke(["status", "--output-format=json"])
        assert r.exit_code == 0, r


@pytest.mark.slow
@pytest.mark.parametrize(
    "archive,layer",
    [
        pytest.param("points.tgz", H.POINTS.LAYER, id="points"),
        pytest.param("polygons.tgz", H.POLYGONS.LAYER, id="polygons"),
        pytest.param("table.tgz", H.TABLE.LAYER, id="table"),
    ],
)
@pytest.mark.parametrize(
    "branding",
    [
        pytest.param("sno"),
        pytest.param("kart"),
    ],
)
def test_upgrade_v2(
    branding, archive, layer, data_archive_readonly, cli_runner, tmp_path, chdir
):
    archive_path = Path("upgrade") / f"v2.{branding}" / archive
    with data_archive_readonly(archive_path) as source_path:
        r = cli_runner.invoke(["data", "version", "--output-format=json"])
        assert r.exit_code == 0, r.stderr
        assert json.loads(r.stdout) == {
            "repostructure.version": 2,
            "localconfig.branding": branding,
        }

        r = cli_runner.invoke(["log"])
        assert r.exit_code == 0  # V2 is still supported

        r = cli_runner.invoke(["upgrade", source_path, tmp_path / "dest"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines()[-1] == "Upgrade complete"

    with chdir(tmp_path / "dest"):
        r = cli_runner.invoke(["log"])
        assert r.exit_code == 0, r.stderr

        if layer == H.POINTS.LAYER:
            assert r.stdout.splitlines() == POINTS_UPGRADE_RESULT

        r = cli_runner.invoke(["status", "--output-format=json"])
        assert r.exit_code == 0, r


@pytest.mark.slow
@pytest.mark.parametrize(
    "archive,layer",
    [
        pytest.param("points.tgz", H.POINTS.LAYER, id="points"),
        pytest.param("polygons.tgz", H.POLYGONS.LAYER, id="polygons"),
        pytest.param("table.tgz", H.TABLE.LAYER, id="table"),
    ],
)
def test_upgrade_v2_in_place(archive, layer, data_archive, cli_runner, tmp_path, chdir):
    archive_path = Path("upgrade") / "v2.kart" / archive
    with data_archive(archive_path) as source_path:
        r = cli_runner.invoke(["data", "version", "--output-format=json"])
        assert r.exit_code == 0, r.stderr
        assert json.loads(r.stdout) == {
            "repostructure.version": 2,
            "localconfig.branding": "kart",
        }

        r = cli_runner.invoke(["log"])
        assert r.exit_code == 0  # V2 is still supported

        r = cli_runner.invoke(["upgrade", "--in-place", source_path, source_path])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines()[-1] == "Upgrade complete"

        r = cli_runner.invoke(["data", "version", "--output-format=json"])
        assert r.exit_code == 0, r.stderr
        assert json.loads(r.stdout) == {
            "repostructure.version": 3,
            "localconfig.branding": "kart",
        }

        r = cli_runner.invoke(["log"])
        assert r.exit_code == 0, r.stderr

        if layer == H.POINTS.LAYER:
            assert r.stdout.splitlines() == POINTS_UPGRADE_RESULT


def test_upgrade_to_tidy(data_archive, cli_runner, chdir):
    archive_path = Path("upgrade") / "old-bare.tgz"
    with data_archive(archive_path) as source_path:
        r = cli_runner.invoke(["upgrade-to-tidy", source_path])
        assert r.exit_code == 0, r.stderr
        assert (
            r.stdout.splitlines()[-1] == "In-place upgrade complete: repo is now tidy"
        )

        repo = KartRepo(source_path)
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
    archive_path = Path("upgrade") / "v2.sno" / "polygons"
    with data_working_copy(archive_path) as (source_path, wc_path):
        r = cli_runner.invoke(["upgrade-to-kart", source_path])
        assert r.exit_code == 0, r.stderr
        assert (
            r.stdout.splitlines()[-1]
            == "In-place upgrade complete: Sno repo is now Kart repo"
        )

        repo = KartRepo(source_path)
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


def test_upgrade_preserves_refs(data_archive, cli_runner, tmp_path):
    with data_archive("upgrade/v2.kart/points") as source_path:
        # first make a new branch, and remove 'main'
        subprocess.check_call(["git", "branch", "-m", "main", "newbranch"])

        # upgrade it
        dest = tmp_path / "dest"
        r = cli_runner.invoke(["upgrade", source_path, dest])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines()[-1] == "Upgrade complete"

        # check that the refs are the same as before
        repo = KartRepo(dest)
        assert set(repo.references) == {"refs/heads/newbranch"}
