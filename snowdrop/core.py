import json
import os
import typing
import uuid
from pathlib import Path

import pygit2
from osgeo import gdal, ogr, osr  # noqa

from . import gpkg


gdal.UseExceptions()


class WorkingCopyInfo(typing.NamedTuple):
    path: str
    fmt: str
    layer: str


class WorkingCopyMismatch(ValueError):
    def __init__(self, working_copy_tree_id, match_tree_id):
        self.working_copy_tree_id = working_copy_tree_id
        self.match_tree_id = match_tree_id

    def __str__(self):
        return f"Working Copy is tree {self.working_copy_tree_id}; expecting {self.match_tree_id}"


def get_working_copy(repo):
    repo_cfg = repo.config
    if "kx.workingcopy" in repo_cfg:
        fmt, path, layer = repo_cfg["kx.workingcopy"].split(":")
        if not os.path.isfile(path):
            raise FileNotFoundError(f"Working copy missing? {path}")
        return WorkingCopyInfo(fmt=fmt, path=path, layer=layer)
    else:
        return None


def set_working_copy(repo, *, path, fmt=None, layer=None):
    repo_cfg = repo.config
    if "kx.workingcopy" in repo_cfg:
        ofmt, opath, olayer = repo_cfg["kx.workingcopy"].split(":")
        fmt = fmt or ofmt
        layer = layer or olayer
    elif not (fmt and layer):
        raise ValueError("No existing workingcopy to update, specify fmt & layer")

    new_path = Path(path)
    if not new_path.is_absolute():
        new_path = os.path.relpath(new_path, Path(repo.path).resolve())

    repo.config["kx.workingcopy"] = f"{fmt}:{new_path}:{layer}"


def feature_blobs_to_dict(tree_entries, geom_column_name, ogr_geoms=False):
    o = {}
    for te in tree_entries:
        assert te.type == "blob"

        blob = te.obj
        if geom_column_name is not None and te.name == geom_column_name and blob.data != b'null':
            if ogr_geoms:
                value = gpkg.geom_to_ogr(blob.data)
            else:
                value = blob.data
        else:
            value = json.loads(blob.data)
        o[te.name] = value
    return o


def assert_db_tree_match(db, table, tree):
    dbcur = db.cursor()
    dbcur.execute(
        "SELECT value FROM __kxg_meta WHERE table_name=? AND key=?;", (table, "tree")
    )
    wc_tree_id = dbcur.fetchone()[0]

    tree_sha = tree.hex

    if wc_tree_id != tree_sha:
        raise WorkingCopyMismatch(wc_tree_id, tree_sha)
    return wc_tree_id


def db_to_index(db, layer, tree):
    # Create an in-memory index, and populate it from:
    # 1. the tree
    # 2. then the current DB (meta info and changes from __kxg_map)
    index = pygit2.Index()
    if tree:
        index.read_tree(tree)

    dbcur = db.cursor()
    table = layer
    pk_field = gpkg.pk(db, table)

    for name, mv_new in gpkg.get_meta_info(db, layer):
        blob_id = pygit2.hash(mv_new)
        entry = pygit2.IndexEntry(
            f"{layer}/meta/{name}", blob_id, pygit2.GIT_FILEMODE_BLOB
        )
        index.add(entry)

    diff_sql = f"""
        SELECT M.feature_key AS __fk, M.state AS __s, M.feature_id AS __pk, T.*
        FROM __kxg_map AS M
            LEFT OUTER JOIN {gpkg.ident(table)} AS T
            ON (M.feature_id = T.{gpkg.ident(pk_field)})
        WHERE
            M.table_name = ?
            AND M.state != 0
            AND NOT (M.feature_key IS NULL AND M.state < 0)  -- ignore INSERT then DELETE
        ORDER BY M.feature_key;
    """

    for i, row in enumerate(dbcur.execute(diff_sql, (table,))):
        o = {k: row[k] for k in row.keys() if not k.startswith("__")}

        feature_key = row["__fk"] or str(uuid.uuid4())

        for k, value in o.items():
            object_path = f"{layer}/features/{feature_key[:4]}/{feature_key}/{k}"

            if row["__s"] == -1:
                index.remove(object_path)
            else:
                if not isinstance(value, bytes):  # blob
                    value = json.dumps(value).encode("utf8")

                blob_id = pygit2.hash(value)
                entry = pygit2.IndexEntry(
                    object_path, blob_id, pygit2.GIT_FILEMODE_BLOB
                )
                index.add(entry)

    return index


def walk_tree(top, path='', topdown=True):
    """
    Corollary of os.walk() for git Tree objects:

    For each subtree in the tree rooted at top (including top itself),
    yields a 4-tuple:
        top_tree, top_path, subtree_names, blob_names

    top_tree is a Tree object
    top_path is a string, the path to top_tree with respect to the root path.
    subtree_names is a list of names for the subtrees in top_tree
    blob_names is a list of names for the blobs in top_tree.

    To get a full path (which begins with top_path) to a blob or subtree in
    top_path, do `os.path.join(top_path, name)`.

    To get a TreeEntry object, do `top_tree / name`
    To get a Blob or Tree object, do `(top_tree / name).obj`

    If optional arg `topdown` is true or not specified, the tuple for a
    subtree is generated before the tuples for any of its subtrees
    (pre-order traversal).  If topdown is false, the tuple
    for a subtree is generated after the tuples for all of its
    subtrees (post-order traversal).

    When topdown is true, the caller can modify the subtree_names list in-place
    (e.g., via del or slice assignment), and walk will only recurse into the
    subtrees whose names remain; this can be used to prune the
    search, or to impose a specific order of visiting.  Modifying subtree_names when
    topdown is false is ineffective, since the directories in subtree_names have
    already been generated by the time subtree_names itself is generated. No matter
    the value of topdown, the list of subtrees is retrieved before the
    tuples for the tree and its subtrees are generated.
    """
    subtree_names = []
    blob_names = []

    for entry in top:
        is_tree = (entry.type == 'tree')

        if is_tree:
            subtree_names.append(entry.name)
        elif entry.type == 'blob':
            blob_names.append(entry.name)
        else:
            pass

    if topdown:
        yield top, path, subtree_names, blob_names
        for name in subtree_names:
            subtree_path = os.path.join(path, name)
            subtree = (top / name).obj
            yield from walk_tree(subtree, subtree_path, topdown=topdown)
    else:
        for name in subtree_names:
            subtree_path = os.path.join(path, name)
            subtree = (top / name).obj
            yield from walk_tree(subtree, subtree_path, topdown=topdown)
        yield top, path, subtree_names, blob_names
