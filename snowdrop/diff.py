import json
import logging

import click
import pygit2

from . import core, gpkg
from .working_copy import WorkingCopy
from .structure import RepositoryStructure, Dataset00


L = logging.getLogger('snowdrop.diff')


@click.command()
@click.pass_context
def diff(ctx):
    """ Show changes between commits, commit and working tree, etc """
    repo_dir = ctx.obj["repo_dir"]
    repo = pygit2.Repository(repo_dir)
    if not repo:
        raise click.BadParameter("Not an existing repository", param_hint="--repo")

    # working_copy = core.get_working_copy(repo)
    # if not working_copy:
    #     raise click.ClickException("No working copy? Try `snow checkout`")

    # db = gpkg.db(working_copy.path, isolation_level="DEFERRED")
    # with db:
    #     pk_field = gpkg.pk(db, working_copy.layer)

    #     head_tree = repo.head.peel(pygit2.Tree)
    #     core.assert_db_tree_match(db, working_copy.layer, head_tree)
    #     diff = db_to_tree(repo, working_copy.layer, db)

    working_copy = WorkingCopy.open(repo)
    if not working_copy:
        raise click.UsageError("No working copy, use 'checkout'")

    for dataset in RepositoryStructure(repo):
        working_copy.assert_db_tree_match(repo.head.peel(pygit2.Tree))

        path = dataset.path
        pk_field = dataset.primary_key

        diff = working_copy.diff_db_to_tree(dataset)

        is_v0 = isinstance(dataset, Dataset00)
        prefix = '' if is_v0 else f'{path}:'
        repr_excl = [] if is_v0 else [pk_field]

        for k, (v_old, v_new) in diff["META"].items():
            click.secho(f"--- {prefix}meta/{k}\n+++ {prefix}meta/{k}", bold=True)

            s_old = set(v_old.items())
            s_new = set(v_new.items())

            diff_add = dict(s_new - s_old)
            diff_del = dict(s_old - s_new)
            all_keys = set(diff_del.keys()) | set(diff_add.keys())

            for k in all_keys:
                if k in diff_del:
                    click.secho(_repr_row({k: diff_del[k]}, prefix="- ", exclude=repr_excl), fg="red")
                if k in diff_add:
                    click.secho(_repr_row({k: diff_add[k]}, prefix="+ ", exclude=repr_excl), fg="green")

        prefix = '' if is_v0 else f'{path}:{pk_field}='

        for k, v_old in diff["D"].items():
            click.secho(f"--- {prefix}{k}", bold=True)
            click.secho(_repr_row(v_old, prefix="- ", exclude=repr_excl), fg="red")

        for o in diff["I"]:
            if is_v0:
                click.secho(f"+++ {prefix}{{new feature}}", bold=True)
            else:
                click.secho(f"+++ {prefix}{o[pk_field]}", bold=True)
            click.secho(_repr_row(o, prefix="+ ", exclude=repr_excl), fg="green")

        for feature_key, (v_old, v_new) in diff["U"].items():
            if is_v0:
                click.secho(f"--- {prefix}{feature_key}\n+++ {prefix}{feature_key}", bold=True)
            else:
                click.secho(f"--- {prefix}{v_old[pk_field]}\n+++ {prefix}{v_new[pk_field]}", bold=True)

            s_old = set(v_old.items())
            s_new = set(v_new.items())

            diff_add = dict(s_new - s_old)
            diff_del = dict(s_old - s_new)
            all_keys = sorted(set(diff_del.keys()) | set(diff_add.keys()))

            if is_v0 and (pk_field not in all_keys):
                click.echo(_repr_row({pk_field: v_new[pk_field]}, prefix="  ", exclude=repr_excl))

            for k in all_keys:
                if k in diff_del:
                    rk = _repr_row({k: diff_del[k]}, prefix="- ", exclude=repr_excl)
                    if rk:
                        click.secho(rk, fg="red")
                if k in diff_add:
                    rk = _repr_row({k: diff_add[k]}, prefix="+ ", exclude=repr_excl)
                    if rk:
                        click.secho(rk, fg="green")


def db_to_tree(repo, layer, db, tree=None):
    """ Generates a diff between a working copy DB and the underlying repository tree """
    table = layer
    dbcur = db.cursor()

    if tree:
        layer_tree = tree
    else:
        dbcur.execute(
            "SELECT value FROM __kxg_meta WHERE table_name=? AND key=?;",
            (table, "tree"),
        )
        tree = repo[dbcur.fetchone()[0]]
        assert tree.type == pygit2.GIT_OBJ_TREE, tree.type

        layer_tree = tree / layer

    meta_tree = layer_tree / "meta"

    meta_diff = {}
    for name, mv_new in gpkg.get_meta_info(db, layer):
        if name in meta_tree:
            mv_old = json.loads(repo[(meta_tree / name).id].data)
        else:
            mv_old = []
        mv_new = json.loads(mv_new)
        if mv_old != mv_new:
            meta_diff[name] = (mv_old, mv_new)

    meta_geom = json.loads((meta_tree / "gpkg_geometry_columns").obj.data)
    pk_field = gpkg.pk(db, table)
    geom_column_name = meta_geom["column_name"] if meta_geom else None

    candidates = {"I": [], "U": {}, "D": {}}

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
    for row in dbcur.execute(diff_sql, (table,)):
        o = {k: row[k] for k in row.keys() if not k.startswith("__")}
        if row["__s"] < 0:
            candidates["D"][row["__fk"]] = {}
        elif row["__fk"] is None:
            candidates["I"].append(o)
        else:
            candidates["U"][row["__fk"]] = o

    results = {"META": meta_diff, "I": candidates["I"], "D": candidates["D"], "U": {}}

    features_tree = layer_tree / "features"
    for op in ("U", "D"):
        for feature_key, db_obj in candidates[op].items():
            ftree = (features_tree / feature_key[:4] / feature_key).obj
            assert ftree.type == pygit2.GIT_OBJ_TREE

            repo_obj = core.feature_blobs_to_dict(
                tree_entries=ftree, geom_column_name=geom_column_name
            )

            s_old = set(repo_obj.items())
            s_new = set(db_obj.items())

            if s_old ^ s_new:
                results[op][feature_key] = (repo_obj, db_obj)

    results["D"] = {k: v[0] for k, v in results["D"].items()}

    return results


def _repr_row(row, prefix="", exclude=None):
    m = []
    exclude = exclude or set()
    for k in row.keys():
        if k.startswith("__") or k in exclude:
            continue

        v = row[k]

        if isinstance(v, bytes):
            g = gpkg.geom_to_ogr(v)
            v = f"{g.GetGeometryName()}(...)"
            del g

        v = "␀" if v is None else v
        m.append("{prefix}{k:>40} = {v}".format(k=k, v=v, prefix=prefix))

    return "\n".join(m)
