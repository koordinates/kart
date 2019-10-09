import os
import re
import subprocess

import pygit2

from snowdrop.core import walk_tree


def test_walk_tree_1(data_archive):
    with data_archive("points"):
        r = pygit2.Repository(".")
        root_tree = r.head.peel(pygit2.Tree)

        for i, (tree, path, dirs, blobs) in enumerate(walk_tree(root_tree, "root")):
            # print(tree, path, dirs, blobs)
            if i == 0:
                assert path == "root"
                assert tree == root_tree
                assert dirs == ["nz_pa_points_topo_150k"]
                assert blobs == []
            elif i == 1:
                assert path == "root/nz_pa_points_topo_150k"
                assert tree == (root_tree / "nz_pa_points_topo_150k").obj
                assert dirs == [".sno-table",]
                assert blobs == []
            elif i == 2:
                assert path == "root/nz_pa_points_topo_150k/.sno-table"
                assert tree == (root_tree / "nz_pa_points_topo_150k" / ".sno-table").obj
                assert set(dirs) == set(["meta"] + [f"{x:02x}" for x in range(256)])
                assert blobs == []
            elif i == 4:
                assert path == "root/nz_pa_points_topo_150k/.sno-table/00/0e"
                assert tree == (root_tree / "nz_pa_points_topo_150k" / ".sno-table" / "00" / "0e").obj
                assert dirs == []
                assert blobs == ['zQZR']

        o = subprocess.check_output(["git", "ls-tree", "-r", "-d", "HEAD"])
        count = len(o.splitlines())
        assert i == count


def test_walk_tree_2(data_archive):
    with data_archive("points"):
        r = pygit2.Repository(".")
        root_tree = r.head.peel(pygit2.Tree)

        path_list = []
        for i, (tree, path, dirs, blobs) in enumerate(walk_tree(root_tree, "")):
            # print(tree, path, dirs, blobs)
            if i == 0:
                assert tree == root_tree
                assert path == ""
                assert dirs == ["nz_pa_points_topo_150k"]
                assert blobs == []
            elif i == 2:
                assert tree == (root_tree / "nz_pa_points_topo_150k" / ".sno-table").obj
                assert path == "nz_pa_points_topo_150k/.sno-table"
                assert "meta" in dirs
                assert blobs == []

                # prune the walks after this
                dirs[:] = ["meta"]
            elif i == 3:
                assert tree == (root_tree / "nz_pa_points_topo_150k" / ".sno-table" / "meta").obj
                assert path == "nz_pa_points_topo_150k/.sno-table/meta"
                assert dirs == ['fields']
                assert blobs == ['gpkg_contents', 'gpkg_geometry_columns', 'gpkg_metadata', 'gpkg_metadata_reference', 'gpkg_spatial_ref_sys', 'primary_key', 'sqlite_table_info', 'version']

            path_list.append(path)
            path_list += [os.path.join(path, b) for b in blobs]

        o = subprocess.check_output(["git", "ls-tree", "-r", "-t", "HEAD", "nz_pa_points_topo_150k/.sno-table/meta"])
        git_paths = [""] + [m for m in re.findall(r'^\d{6} (?:blob|tree) [0-9a-f]{40}\t(.+)$', o.decode('utf-8'), re.MULTILINE)]
        assert set(path_list) == set(git_paths)


def test_walk_tree_3(data_archive):
    with data_archive("points"):
        r = pygit2.Repository(".")
        root_tree = r.head.peel(pygit2.Tree)

        for i, (tree, path, dirs, blobs) in enumerate(walk_tree(root_tree, "root", topdown=False)):
            print(i, tree, path, dirs, blobs)
            if i == 0:
                assert path == "root/nz_pa_points_topo_150k/.sno-table/00/0e"
                assert tree == (root_tree / "nz_pa_points_topo_150k" / ".sno-table" / "00" / "0e").obj
                assert dirs == []
                assert blobs == ['zQZR']
            elif i == 13:
                assert path == "root/nz_pa_points_topo_150k/.sno-table/00"
                assert tree == (root_tree / "nz_pa_points_topo_150k" / ".sno-table" / "00").obj
                assert dirs == ['0e', '22', '28', '68', '7c', '87', 'a5', 'cb', 'e1', 'e4', 'f1', 'f7', 'fb']
                assert blobs == []
            elif i == 14:
                assert path == "root/nz_pa_points_topo_150k/.sno-table/01/00"
                assert tree == (root_tree / "nz_pa_points_topo_150k" / ".sno-table" / "01" / "00").obj
                assert dirs == []
                assert blobs == ['zQbL']
            elif i == 22:
                assert path == "root/nz_pa_points_topo_150k/.sno-table/02/58"
                assert tree == (root_tree / "nz_pa_points_topo_150k" / ".sno-table" / "02" / "58").obj
                assert dirs == []
                assert blobs == ['zQX5', 'zQee']

        o = subprocess.check_output(["git", "ls-tree", "-r", "-d", "HEAD"])
        count = len(o.splitlines())
        assert i == count
