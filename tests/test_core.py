import os
import re
import subprocess
from pathlib import Path

import click
import pygit2

import pytest

from sno.core import walk_tree, check_git_user
from sno.repo import SnoRepo


def test_walk_tree_1(data_archive):
    with data_archive("points"):
        r = SnoRepo(".")
        root_tree = r.head_tree

        for i, (tree, path, dirs, blobs) in enumerate(walk_tree(root_tree, "root")):
            # print(tree, path, dirs, blobs)
            if i == 0:
                assert path == "root"
                assert tree == root_tree
                assert dirs == ["nz_pa_points_topo_150k"]
                assert blobs == [".sno.repository.version"]
            elif i == 1:
                assert path == "/".join(["root", "nz_pa_points_topo_150k"])
                assert tree == (root_tree / "nz_pa_points_topo_150k")
                assert dirs == [
                    ".sno-dataset",
                ]
                assert blobs == []
            elif i == 2:
                assert path == "/".join(
                    ["root", "nz_pa_points_topo_150k", ".sno-dataset"]
                )
                assert tree == (root_tree / "nz_pa_points_topo_150k" / ".sno-dataset")
                assert set(dirs) == set(["meta", "feature"])
            elif i == 3:
                assert path == "/".join(
                    ["root", "nz_pa_points_topo_150k", ".sno-dataset", "feature"]
                )
                assert tree == (
                    root_tree / "nz_pa_points_topo_150k" / ".sno-dataset" / "feature"
                )
                assert set(dirs) == set([f"{x:02x}" for x in range(256)])
                assert blobs == []
            elif i == 5:
                assert path == "/".join(
                    [
                        "root",
                        "nz_pa_points_topo_150k",
                        ".sno-dataset",
                        "feature",
                        "00",
                        "01",
                    ]
                )
                assert tree == (
                    root_tree
                    / "nz_pa_points_topo_150k"
                    / ".sno-dataset"
                    / "feature"
                    / "00"
                    / "01"
                )
                assert dirs == []
                assert blobs == ["kc0BMA=="]

        o = subprocess.check_output(["git", "ls-tree", "-r", "-d", "HEAD"])
        count = len(o.splitlines())
        assert i == count


def test_walk_tree_2(data_archive):
    with data_archive("points"):
        r = SnoRepo(".")
        root_tree = r.head_tree

        path_list = []
        for i, (tree, path, dirs, blobs) in enumerate(walk_tree(root_tree, "")):
            # print(tree, path, dirs, blobs)
            if i == 0:
                assert tree == root_tree
                assert path == ""
                assert dirs == ["nz_pa_points_topo_150k"]
                assert blobs == [".sno.repository.version"]
            elif i == 2:
                assert tree == (root_tree / "nz_pa_points_topo_150k" / ".sno-dataset")
                assert path == "/".join(["nz_pa_points_topo_150k", ".sno-dataset"])
                assert "meta" in dirs
                assert blobs == []

                # prune the walks after this
                dirs[:] = ["meta"]
            elif i == 3:
                assert tree == (
                    root_tree / "nz_pa_points_topo_150k" / ".sno-dataset" / "meta"
                )
                assert path == "/".join(
                    ["nz_pa_points_topo_150k", ".sno-dataset", "meta"]
                )
                assert dirs == ["crs", "legend", "metadata"]
                assert blobs == ["description", "schema.json", "title"]

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
                ".sno.repository.version",
                "nz_pa_points_topo_150k/.sno-dataset/meta",
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
        r = SnoRepo(".")
        root_tree = r.head_tree

        for i, (tree, path, dirs, blobs) in enumerate(
            walk_tree(root_tree, "root", topdown=False)
        ):
            print(i, tree, path, dirs, blobs)
            if i == 0:
                assert path == "/".join(
                    [
                        "root",
                        "nz_pa_points_topo_150k",
                        ".sno-dataset",
                        "feature",
                        "00",
                        "01",
                    ]
                )
                assert tree == (
                    root_tree
                    / "nz_pa_points_topo_150k"
                    / ".sno-dataset"
                    / "feature"
                    / "00"
                    / "01"
                )
                assert dirs == []
                assert blobs == ["kc0BMA=="]
            elif i == 12:
                assert path == "/".join(
                    ["root", "nz_pa_points_topo_150k", ".sno-dataset", "feature", "00"]
                )
                assert tree == (
                    root_tree
                    / "nz_pa_points_topo_150k"
                    / ".sno-dataset"
                    / "feature"
                    / "00"
                )
                assert dirs == [
                    "01",
                    "09",
                    "20",
                    "21",
                    "34",
                    "4e",
                    "6c",
                    "81",
                    "85",
                    "a8",
                    "af",
                    "bb",
                ]
                assert blobs == []
            elif i == 13:
                assert path == "/".join(
                    [
                        "root",
                        "nz_pa_points_topo_150k",
                        ".sno-dataset",
                        "feature",
                        "01",
                        "15",
                    ]
                )
                assert tree == (
                    root_tree
                    / "nz_pa_points_topo_150k"
                    / ".sno-dataset"
                    / "feature"
                    / "01"
                    / "15"
                )
                assert dirs == []
                assert blobs == ["kc0Ekg==", "kc0GZw=="]
            elif i == 22:
                assert path == "/".join(
                    [
                        "root",
                        "nz_pa_points_topo_150k",
                        ".sno-dataset",
                        "feature",
                        "02",
                        "70",
                    ]
                )
                assert tree == (
                    root_tree
                    / "nz_pa_points_topo_150k"
                    / ".sno-dataset"
                    / "feature"
                    / "02"
                    / "70"
                )
                assert dirs == []
                assert blobs == ["kc0D0w=="]

        o = subprocess.check_output(["git", "ls-tree", "-r", "-d", "HEAD"])
        count = len(o.splitlines())
        assert i == count


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
            r = SnoRepo(".")
            with pytest.raises(click.ClickException) as e:
                check_git_user(repo=r)
            assert "Please tell me who you are" in str(e)

            subprocess.run(["git", "config", "user.name", "Alice"])
            subprocess.run(["git", "config", "user.email", "alice@example.com"])

            check_git_user(repo=r)

        with pytest.raises(click.ClickException) as e:
            check_git_user(repo=None)
        assert "Please tell me who you are" in str(e)
    finally:
        pygit2.option(
            pygit2.GIT_OPT_SET_SEARCH_PATH, pygit2.GIT_CONFIG_LEVEL_GLOBAL, prev_home
        )


def test_gdal_proj_data():
    import sno  # noqa
    from osgeo import gdal, osr

    # GDAL_DATA
    assert "GDAL_DATA" in os.environ
    assert gdal.GetConfigOption("GDAL_DATA") == os.environ["GDAL_DATA"]
    assert (Path(gdal.GetConfigOption("GDAL_DATA")) / "gdalvrt.xsd").exists()

    # PROJ_LIB
    assert "PROJ_LIB" in os.environ
    osr = osr.SpatialReference()
    osr.ImportFromEPSG(4167)
    assert "NZGD2000" in osr.ExportToWkt()
    assert (Path(os.environ["PROJ_LIB"]) / "proj.db").exists()
