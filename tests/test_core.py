import os
import re
import subprocess

import pygit2

from snowdrop.core import walk_tree


def test_walk_tree_1(data_archive):
    with data_archive("points.snow"):
        r = pygit2.Repository(".")
        root_tree = r.head.peel(pygit2.Tree)

        for i, (tree, path, dirs, blobs) in enumerate(walk_tree(root_tree, "root")):
            # print(tree, path, dirs, blobs)
            if i == 0:
                assert tree == root_tree
                assert path == "root"
                assert dirs == ["nz_pa_points_topo_150k"]
                assert blobs == []
            elif i == 1:
                assert tree == (root_tree / "nz_pa_points_topo_150k").obj
                assert path == "root/nz_pa_points_topo_150k"
                assert dirs == ["features", "meta"]
                assert blobs == []
            elif i == 4:
                assert tree == (root_tree / "nz_pa_points_topo_150k" / "features" / "001a" / "001a887a-5146-442e-a028-99105abdc2b2").obj
                assert path == "root/nz_pa_points_topo_150k/features/001a/001a887a-5146-442e-a028-99105abdc2b2"
                assert dirs == []
                assert blobs == ['fid', 'geom', 'macronated', 'name', 'name_ascii', 't50_fid']

        o = subprocess.check_output(["git", "ls-tree", "-r", "-d", "HEAD"])
        count = len(o.splitlines())
        assert i == count


def test_walk_tree_2(data_archive):
    with data_archive("points.snow"):
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
            elif i == 1:
                assert tree == (root_tree / "nz_pa_points_topo_150k").obj
                assert path == "nz_pa_points_topo_150k"
                assert dirs == ["features", "meta"]
                assert blobs == []

                # prune the walks after this
                dirs.remove("features")
            elif i == 2:
                assert tree == (root_tree / "nz_pa_points_topo_150k" / "meta").obj
                assert path == "nz_pa_points_topo_150k/meta"
                assert dirs == []
                assert blobs == ['gpkg_contents', 'gpkg_geometry_columns', 'gpkg_metadata', 'gpkg_metadata_reference', 'gpkg_spatial_ref_sys', 'sqlite_table_info', 'version']

            path_list.append(path)
            path_list += [os.path.join(path, b) for b in blobs]

        o = subprocess.check_output(["git", "ls-tree", "-r", "-t", "HEAD", "nz_pa_points_topo_150k/meta"])
        git_paths = [""] + [m for m in re.findall(r'^\d{6} (?:blob|tree) [0-9a-f]{40}\t(.+)$', o.decode('utf-8'), re.MULTILINE)]
        assert set(path_list) == set(git_paths)


def test_walk_tree_3(data_archive):
    with data_archive("points.snow"):
        r = pygit2.Repository(".")
        root_tree = r.head.peel(pygit2.Tree)

        for i, (tree, path, dirs, blobs) in enumerate(walk_tree(root_tree, "root", topdown=False)):
            print(tree, path, dirs, blobs)
            if i == 0:
                assert tree == (root_tree / "nz_pa_points_topo_150k" / "features" / "001a" / "001a887a-5146-442e-a028-99105abdc2b2").obj
                assert path == "root/nz_pa_points_topo_150k/features/001a/001a887a-5146-442e-a028-99105abdc2b2"
                assert dirs == []
                assert blobs == ['fid', 'geom', 'macronated', 'name', 'name_ascii', 't50_fid']
            elif i == 1:
                assert tree == (root_tree / "nz_pa_points_topo_150k" / "features" / "001a").obj
                assert path == "root/nz_pa_points_topo_150k/features/001a"
                assert dirs == ["001a887a-5146-442e-a028-99105abdc2b2"]
                assert blobs == []
            elif i == 2:
                assert tree == (root_tree / "nz_pa_points_topo_150k" / "features" / "002c" / "002ce47f-3a59-4173-a14f-8c70e4569faf").obj
                assert path == "root/nz_pa_points_topo_150k/features/002c/002ce47f-3a59-4173-a14f-8c70e4569faf"
                assert dirs == []
                assert blobs == ['fid', 'geom', 'macronated', 'name', 'name_ascii', 't50_fid']
            elif i == 3:
                assert tree == (root_tree / "nz_pa_points_topo_150k" / "features" / "002c").obj
                assert path == "root/nz_pa_points_topo_150k/features/002c"
                assert dirs == ["002ce47f-3a59-4173-a14f-8c70e4569faf"]
                assert blobs == []

        o = subprocess.check_output(["git", "ls-tree", "-r", "-d", "HEAD"])
        count = len(o.splitlines())
        assert i == count
