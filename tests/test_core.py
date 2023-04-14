import os
import re
import subprocess
from pathlib import Path

import click
import pygit2

import pytest

from kart import is_windows
from kart.core import walk_tree, check_git_user
from kart.repo import KartRepo


def test_walk_tree_1(data_archive):
    with data_archive("points"):
        r = KartRepo(".")
        root_tree = r.head_tree

        for i, (tree, path, dirs, blobs) in enumerate(walk_tree(root_tree, "root")):
            # print(tree, path, dirs, blobs)
            if i == 0:
                assert path == "root"
                assert tree == root_tree
                assert dirs == ["nz_pa_points_topo_150k"]
                assert blobs == [".kart.repostructure.version"]
            elif i == 1:
                assert path == "/".join(["root", "nz_pa_points_topo_150k"])
                assert tree == (root_tree / "nz_pa_points_topo_150k")
                assert dirs == [
                    ".table-dataset",
                ]
                assert blobs == ["metadata.xml"]
            elif i == 2:
                assert path == "/".join(
                    ["root", "nz_pa_points_topo_150k", ".table-dataset"]
                )
                assert tree == (root_tree / "nz_pa_points_topo_150k" / ".table-dataset")
                assert set(dirs) == set(["meta", "feature"])
            elif i == 3:
                assert path == "/".join(
                    ["root", "nz_pa_points_topo_150k", ".table-dataset", "feature"]
                )
                assert tree == (
                    root_tree / "nz_pa_points_topo_150k" / ".table-dataset" / "feature"
                )
                assert dirs == ["A"]
                assert blobs == []
            elif i == 7:
                assert path == "/".join(
                    [
                        "root",
                        "nz_pa_points_topo_150k",
                        ".table-dataset",
                        "feature",
                        "A",
                        "A",
                        "A",
                        "A",
                    ]
                )
                assert tree == (
                    root_tree
                    / "nz_pa_points_topo_150k"
                    / ".table-dataset"
                    / "feature"
                    / "A"
                    / "A"
                    / "A"
                    / "A"
                )
                assert dirs == []
                assert blobs[0:5] == ["kQ0=", "kQ4=", "kQ8=", "kQE=", "kQI="]

        o = subprocess.check_output(["git", "ls-tree", "-r", "-d", "HEAD"])
        count = len(o.splitlines())
        assert i == count


def test_walk_tree_2(data_archive):
    with data_archive("points"):
        r = KartRepo(".")
        root_tree = r.head_tree

        path_list = []
        for i, (tree, path, dirs, blobs) in enumerate(walk_tree(root_tree, "")):
            # print(tree, path, dirs, blobs)
            if i == 0:
                assert tree == root_tree
                assert path == ""
                assert dirs == ["nz_pa_points_topo_150k"]
                assert blobs == [".kart.repostructure.version"]
            elif i == 2:
                assert tree == (root_tree / "nz_pa_points_topo_150k" / ".table-dataset")
                assert path == "/".join(["nz_pa_points_topo_150k", ".table-dataset"])
                assert "meta" in dirs
                assert blobs == []

                # prune the walks after this
                dirs[:] = ["meta"]
            elif i == 3:
                assert tree == (
                    root_tree / "nz_pa_points_topo_150k" / ".table-dataset" / "meta"
                )
                assert path == "/".join(
                    ["nz_pa_points_topo_150k", ".table-dataset", "meta"]
                )
                assert dirs == ["crs", "legend"]
                assert blobs == [
                    "description",
                    "path-structure.json",
                    "schema.json",
                    "title",
                ]

            path_list.append(path)
            if path:
                path_list += ["/".join([path, b]) for b in blobs]
            else:
                path_list += blobs

        o = subprocess.check_output(
            [
                "git",
                "ls-tree",
                "-r",
                "-t",
                "HEAD",
                ".kart.repostructure.version",
                "nz_pa_points_topo_150k/metadata.xml",
                "nz_pa_points_topo_150k/.table-dataset/meta",
            ]
        )
        git_paths = [""] + [
            m
            for m in re.findall(
                r"^\d{6} (?:blob|tree) [0-9a-f]{40}\t(.+)$",
                o.decode("utf-8"),
                re.MULTILINE,
            )
        ]
        assert set(path_list) == set(git_paths)


def test_walk_tree_3(data_archive):
    with data_archive("points"):
        r = KartRepo(".")
        root_tree = r.head_tree

        for i, (tree, path, dirs, blobs) in enumerate(
            walk_tree(root_tree, "root", topdown=False)
        ):
            # print(i, tree, path, dirs, blobs)
            if i == 0:
                assert path == "/".join(
                    [
                        "root",
                        "nz_pa_points_topo_150k",
                        ".table-dataset",
                        "feature",
                        "A",
                        "A",
                        "A",
                        "A",
                    ]
                )
                assert tree == (
                    root_tree
                    / "nz_pa_points_topo_150k"
                    / ".table-dataset"
                    / "feature"
                    / "A"
                    / "A"
                    / "A"
                    / "A"
                )
                assert dirs == []
                assert blobs[0:5] == ["kQ0=", "kQ4=", "kQ8=", "kQE=", "kQI="]
            elif i == 1:
                assert path == "/".join(
                    [
                        "root",
                        "nz_pa_points_topo_150k",
                        ".table-dataset",
                        "feature",
                        "A",
                        "A",
                        "A",
                        "B",
                    ]
                )
                assert tree == (
                    root_tree
                    / "nz_pa_points_topo_150k"
                    / ".table-dataset"
                    / "feature"
                    / "A"
                    / "A"
                    / "A"
                    / "B"
                )
                assert dirs == []
                assert blobs[0:5] == ["kU0=", "kU4=", "kU8=", "kUA=", "kUE="]
            elif i == 34:
                assert path == "/".join(
                    [
                        "root",
                        "nz_pa_points_topo_150k",
                        ".table-dataset",
                        "feature",
                        "A",
                        "A",
                        "A",
                    ]
                )
                assert tree == (
                    root_tree
                    / "nz_pa_points_topo_150k"
                    / ".table-dataset"
                    / "feature"
                    / "A"
                    / "A"
                    / "A"
                )
                assert "".join(dirs) == "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefgh"
                assert blobs == []

        o = subprocess.check_output(["git", "ls-tree", "-r", "-d", "HEAD"])
        count = len(o.splitlines())
        assert i == count


@pytest.mark.skipif(
    os.name == "posix" and os.geteuid() == 0, reason="doesn't work as root"
)
def test_check_user_config(git_user_config, monkeypatch, data_archive, tmp_path):
    # this is set by the global git_user_config fixture
    u_email, u_name = check_git_user(repo=None)
    assert u_email == git_user_config[0]
    assert u_name == git_user_config[1]

    # clear home
    monkeypatch.setenv("HOME", str(tmp_path))
    prev_home = pygit2.option(
        pygit2.GIT_OPT_GET_SEARCH_PATH, pygit2.GIT_CONFIG_LEVEL_GLOBAL
    )
    try:
        pygit2.option(
            pygit2.GIT_OPT_SET_SEARCH_PATH,
            pygit2.GIT_CONFIG_LEVEL_GLOBAL,
            str(tmp_path),
        )

        with data_archive("points"):
            r = KartRepo(".")
            with pytest.raises(click.ClickException) as e:
                check_git_user(repo=r)
            assert "Please tell me who you are" in str(e)

            subprocess.check_call(["git", "config", "--local", "user.name", "Alice"])
            subprocess.check_call(
                ["git", "config", "--local", "user.email", "alice@example.com"]
            )

            check_git_user(repo=r)

        with pytest.raises(click.ClickException) as e:
            check_git_user(repo=None)
        assert "Please tell me who you are" in str(e)

        # The check should always pass if the user is setting these type of env variables:
        monkeypatch.setenv("GIT_AUTHOR_EMAIL", "user@example.com")
        check_git_user(repo=None)

    finally:
        pygit2.option(
            pygit2.GIT_OPT_SET_SEARCH_PATH, pygit2.GIT_CONFIG_LEVEL_GLOBAL, prev_home
        )


def test_gdal_data_exists():
    import kart  # noqa
    from osgeo import gdal

    # GDAL_DATA
    assert "GDAL_DATA" in os.environ
    assert gdal.GetConfigOption("GDAL_DATA") == os.environ["GDAL_DATA"]
    assert (Path(gdal.GetConfigOption("GDAL_DATA")) / "gdalvrt.xsd").exists()

    # PROJ_LIB
    assert "PROJ_LIB" in os.environ
    assert (Path(os.environ["PROJ_LIB"]) / "proj.db").exists()


def test_proj_transformation_grid():
    import kart  # noqa
    from osgeo import osr

    # note kart.__init__ sets PROJ_NETWORK=ON and PROJ_LIB to the proj data files
    # during tests $HOME is changed to a temporary dir, so downloaded grids in the
    # user proj data dir will be empty/missing
    # TODO: except for windows where somehow it's buggy and we get a network error
    print("PROJ/GDAL-related environment variables:")
    for k, v in os.environ.items():
        if re.match(r"^(GDAL|PROJ|CPL)_", k):
            print(f"${k}={v}")

    print("osr.GetPROJSearchPaths():", osr.GetPROJSearchPaths())
    print("osr.GetPROJAuxDbPaths():", osr.GetPROJAuxDbPaths())
    # print("osr.GetPROJEnableNetwork():", osr.GetPROJEnableNetwork())  # not in the Python bindings yet

    # Do a test conversion to check the transformation grids are available
    nzgd49 = osr.SpatialReference()
    nzgd49.ImportFromEPSG(4272)  # NZGD1949
    nzgd2k = osr.SpatialReference()
    nzgd2k.ImportFromEPSG(4167)  # NZGD2000
    ct = osr.CreateCoordinateTransformation(nzgd49, nzgd2k)
    # Test point from: https://www.linz.govt.nz/data/geodetic-system/coordinate-conversion/geodetic-datum-conversions/datum-transformation-examples
    pt = ct.TransformPoint(-36.5, 175.0)

    # Equivalent commands:
    # $ echo 175.0 -36.5 | PROJ_DEBUG=2 CPL_DEBUG=ON gdaltransform -s_srs EPSG:4272 -t_srs EPSG:4167
    # $ echo -36.5 175.0 0 | PROJ_DEBUG=2 cs2cs -v -f "%.8f" EPSG:4272 EPSG:4167

    if not is_windows:
        # This is the (desired) accurate result expected when the transformation grid is available:
        assert pt == pytest.approx((-36.49819023, 175.00019297, 0.0), abs=1e-8)
    else:
        # This is the less accurate result which uses the 7-parameter transform,
        # which indicates that the transformation grid is not available
        #
        # Currently the Windows proj libraries are built without network
        # support, so we can't auto-fetch grids
        assert pt == pytest.approx((-36.49819267, 175.00018527, 0.0), abs=1e-8)
