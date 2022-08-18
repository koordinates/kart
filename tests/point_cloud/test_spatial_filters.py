from .fixtures import requires_pdal  # noqa

from kart.geometry import ring_as_wkt
from kart.repo import KartRepo

CRS = "EPSG:4326"

SOUTH_EAST_TRIANGLE = (
    "POLYGON("
    + ring_as_wkt(
        (174.738, -36.851),
        (174.782, -36.821),
        (174.782, -36.851),
        (174.738, -36.851),
    )
    + ")"
)

SOUTH_WEST_TRIANGLE = (
    "POLYGON("
    + ring_as_wkt(
        (174.738, -36.851),
        (174.738, -36.821),
        (174.782, -36.851),
        (174.738, -36.851),
    )
    + ")"
)

SOUTH_EAST_TILES = {
    "auckland_0_0.copc.laz",
    "auckland_1_0.copc.laz",
    "auckland_1_1.copc.laz",
    "auckland_2_0.copc.laz",
    "auckland_2_1.copc.laz",
    "auckland_2_2.copc.laz",
    "auckland_3_0.copc.laz",
    "auckland_3_1.copc.laz",
    "auckland_3_2.copc.laz",
    "auckland_3_3.copc.laz",
}

SOUTH_WEST_TILES = {
    "auckland_0_0.copc.laz",
    "auckland_0_1.copc.laz",
    "auckland_0_2.copc.laz",
    "auckland_0_3.copc.laz",
    "auckland_1_0.copc.laz",
    "auckland_1_1.copc.laz",
    "auckland_1_2.copc.laz",
    "auckland_2_0.copc.laz",
    "auckland_2_1.copc.laz",
    "auckland_3_0.copc.laz",
}


def _tile_filenames_in_workdir(repo, ds_path):
    return set(f.name for f in (repo.workdir_path / ds_path).iterdir())


def _count_files_in_lfs_cache(repo):
    return sum(1 for f in (repo.gitdir_path / "lfs").glob("**/*") if f.is_file())


def test_clone_pc_with_spatial_filter(
    data_archive, cli_runner, tmp_path, monkeypatch, requires_pdal
):
    monkeypatch.setenv("X_KART_POINT_CLOUDS", "1")

    file_path = (tmp_path / "spatialfilter.txt").resolve()
    file_path.write_text(f"{CRS}\n\n{SOUTH_EAST_TRIANGLE}\n", encoding="utf-8")

    with data_archive("point-cloud/auckland.tgz") as repo1_path:
        repo1_url = f"file://{repo1_path.resolve()}"
        # Clone repo using spatial filter
        repo2_path = tmp_path / "repo2"
        r = cli_runner.invoke(
            [
                "clone",
                repo1_url,
                repo2_path,
                f"--spatial-filter=@{file_path}",
                # spatial-filter-after-clone doesn't affect point-cloud datasets,
                # which never apply a filter to the initial clone operation anyway -
                # but it means running this test doesn't need git filter extensions
                "--spatial-filter-after-clone",
            ]
        )
        assert r.exit_code == 0, r.stderr

        # The resulting repo has the spatial filter configured locally.
        repo2 = KartRepo(repo2_path)
        assert repo2.config["kart.spatialfilter.geometry"].startswith(
            "POLYGON ((174.738 -36.851,"
        )
        assert repo2.config["kart.spatialfilter.crs"] == CRS

        assert _tile_filenames_in_workdir(repo2, "auckland") == SOUTH_EAST_TILES
        assert _count_files_in_lfs_cache(repo2) == len(SOUTH_EAST_TILES)


def test_reclone_pc_with_larger_spatial_filter(
    data_archive, cli_runner, tmp_path, monkeypatch, requires_pdal
):
    monkeypatch.setenv("X_KART_POINT_CLOUDS", "1")

    with data_archive("point-cloud/auckland.tgz") as repo1_path:
        repo1_url = f"file://{repo1_path.resolve()}"
        # Clone repo using spatial filter
        repo2_path = tmp_path / "repo2"

        EMPTY_SPATIAL_FILTER = "EPSG:4326;POLYGON((0 0,0 1,1 1,1 0,0 0))"
        r = cli_runner.invoke(
            [
                "clone",
                repo1_url,
                repo2_path,
                f"--spatial-filter={EMPTY_SPATIAL_FILTER}",
                # spatial-filter-after-clone doesn't affect point-cloud datasets,
                # which never apply a filter to the initial clone operation anyway -
                # but it means running this test doesn't need git filter extensions
                "--spatial-filter-after-clone",
            ]
        )
        assert r.exit_code == 0, r.stderr

        repo2 = KartRepo(repo2_path)
        assert _tile_filenames_in_workdir(repo2, "auckland") == set()
        assert _count_files_in_lfs_cache(repo2) == 0

        file_path = (tmp_path / "spatialfilter.txt").resolve()
        file_path.write_text(f"{CRS}\n\n{SOUTH_EAST_TRIANGLE}\n", encoding="utf-8")

        r = cli_runner.invoke(
            ["-C", repo2_path, "checkout", f"--spatial-filter=@{file_path}"]
        )
        assert r.exit_code == 0, r.stderr

        assert _tile_filenames_in_workdir(repo2, "auckland") == SOUTH_EAST_TILES
        assert _count_files_in_lfs_cache(repo2) == len(SOUTH_EAST_TILES)

        file_path = (tmp_path / "spatialfilter.txt").resolve()
        file_path.write_text(f"{CRS}\n\n{SOUTH_WEST_TRIANGLE}\n", encoding="utf-8")

        r = cli_runner.invoke(
            ["-C", repo2_path, "checkout", f"--spatial-filter=@{file_path}"]
        )
        assert r.exit_code == 0, r.stderr

        assert _tile_filenames_in_workdir(repo2, "auckland") == SOUTH_WEST_TILES
        # Both sets of tiles are still in the LFS cache:
        assert _count_files_in_lfs_cache(repo2) == len(
            SOUTH_EAST_TILES | SOUTH_WEST_TILES
        )
