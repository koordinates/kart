import shutil

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
        repo1_url = str(repo1_path.resolve())
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
        repo1_url = str(repo1_path.resolve())
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


def test_spatial_filtered_diff(
    data_archive, cli_runner, tmp_path, monkeypatch, requires_pdal
):
    monkeypatch.setenv("X_KART_POINT_CLOUDS", "1")

    file_path = (tmp_path / "spatialfilter.txt").resolve()
    file_path.write_text(f"{CRS}\n\n{SOUTH_EAST_TRIANGLE}\n", encoding="utf-8")

    with data_archive("point-cloud/auckland.tgz") as repo_path:
        r = cli_runner.invoke(["checkout", f"--spatial-filter=@{file_path}"])
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["show"])
        assert r.exit_code == 0, r.stderr
        tile_lines = [
            line for line in r.stdout.splitlines() if "auckland:tile:" in line
        ]
        assert tile_lines == [
            "+++ auckland:tile:auckland_0_0",
            "+++ auckland:tile:auckland_1_0",
            "+++ auckland:tile:auckland_1_1",
            "+++ auckland:tile:auckland_2_0",
            "+++ auckland:tile:auckland_2_1",
            "+++ auckland:tile:auckland_2_2",
            "+++ auckland:tile:auckland_3_0",
            "+++ auckland:tile:auckland_3_1",
            "+++ auckland:tile:auckland_3_2",
            "+++ auckland:tile:auckland_3_3",
        ]

        # Give a tile the same name as an existing tile that has been filtered away:
        shutil.copy(
            repo_path / "auckland" / "auckland_0_0.copc.laz",
            repo_path / "auckland" / "auckland_0_3.copc.laz",
        )

        r = cli_runner.invoke(["diff"])
        assert r.exit_code == 0, r.stderr

        # For a working copy diff, both old and new versions are always shown, even if one or other is outside the
        # spatial filter:
        assert r.stdout.splitlines() == [
            "--- auckland:tile:auckland_0_3",
            "+++ auckland:tile:auckland_0_3",
            "-                              crs84Extent = 174.73891001915146,174.74882538957996,-36.82418662552334,-36.82395715927626,-1.77,28.39",
            "+                              crs84Extent = 174.73844833207193,174.74945404214898,-36.85123712200056,-36.84206322341377,-1.66,99.83",
            "-                             nativeExtent = 1755083.64,1755968.48,5923220.12,5923229.44,-1.77,28.39",
            "+                             nativeExtent = 1754987.85,1755987.77,5920219.76,5921219.64,-1.66,99.83",
            "-                               pointCount = 20",
            "+                               pointCount = 4231",
            "-                                sourceOid = sha256:a4acd08ca3763823df67fc0d4e45ce0e39525b49e31d8f20babc74d208e481a5",
            "-                                      oid = sha256:11ba773069c7e935735f7076b2fa44334d0bb41c4742d8cd8111f575359a773c",
            "+                                      oid = sha256:a1862450841dede2759af665825403e458dfa551c095d9a65ea6e6765aeae0f7",
            "-                                     size = 2190",
            "+                                     size = 69590",
        ]

        assert r.stderr.splitlines() == [
            "Warning: Some names of newly-inserted tiles in the working copy conflict with other tiles outside the spatial filter - if committed, they would overwrite those tiles.",
            "  In dataset auckland the conflicting names are: auckland_0_3",
            "  To continue, change the names of those tiles.",
        ]

        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            "On branch main",
            "",
            "A spatial filter is active, limiting repo to a specific region inside [174.738, -36.851, 174.782, -36.821]",
            "",
            "Changes in working copy:",
            '  (use "kart commit" to commit)',
            '  (use "kart restore" to discard changes)',
            "",
            "  auckland:",
            "    tile:",
            "      1 spatial filter conflicts",
        ]
