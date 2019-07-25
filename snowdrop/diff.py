import json

import click
import pygit2

from . import core, gpkg


@click.command()
@click.pass_context
def diff(ctx):
    """ Show changes between commits, commit and working tree, etc """
    repo_dir = ctx.obj["repo_dir"]
    repo = pygit2.Repository(repo_dir)
    if not repo or not repo.is_bare:
        raise click.BadParameter("Not an existing repository", param_hint="--repo")

    working_copy = core.get_working_copy(repo)
    if not working_copy:
        raise click.ClickException("No working copy? Try `snow checkout`")

    db = gpkg.db(working_copy.path, isolation_level="DEFERRED")
    with db:
        pk_field = gpkg.pk(db, working_copy.layer)

        head_tree = repo.head.peel(pygit2.Tree)
        core.assert_db_tree_match(db, working_copy.layer, head_tree)
        diff = db_to_tree(repo, working_copy.layer, db)

    for k, (v_old, v_new) in diff["META"].items():
        click.secho(f"--- meta/{k}\n+++ meta/{k}", bold=True)

        s_old = set(v_old.items())
        s_new = set(v_new.items())

        diff_add = dict(s_new - s_old)
        diff_del = dict(s_old - s_new)
        all_keys = set(diff_del.keys()) | set(diff_add.keys())

        for k in all_keys:
            if k in diff_del:
                click.secho(_repr_row({k: diff_del[k]}, prefix="- "), fg="red")
            if k in diff_add:
                click.secho(_repr_row({k: diff_add[k]}, prefix="+ "), fg="green")

    for k, (v_old, v_new) in diff["D"].items():
        click.secho(f"--- {k}", bold=True)
        click.secho(_repr_row(v_old, prefix="- "), fg="red")

    for o in diff["I"]:
        click.secho("+++ {new feature}", bold=True)
        click.secho(_repr_row(o, prefix="+ "), fg="green")

    for feature_key, (v_old, v_new) in diff["U"].items():
        click.secho(f"--- {feature_key}\n+++ {feature_key}", bold=True)

        s_old = set(v_old.items())
        s_new = set(v_new.items())

        diff_add = dict(s_new - s_old)
        diff_del = dict(s_old - s_new)
        all_keys = sorted(set(diff_del.keys()) | set(diff_add.keys()))

        if pk_field not in all_keys:
            click.echo(_repr_row({pk_field: v_new[pk_field]}, prefix="  "))

        for k in all_keys:
            if k in diff_del:
                click.secho(_repr_row({k: diff_del[k]}, prefix="- "), fg="red")
            if k in diff_add:
                click.secho(_repr_row({k: diff_add[k]}, prefix="+ "), fg="green")


def db_to_tree(repo, layer, db, tree=None):
    """ Generates a diff between a working copy DB and the underlying repository tree """
    table = layer
    dbcur = db.cursor()

    if not tree:
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

    features_tree = tree / layer / "features"
    for op in ("U", "D"):
        for feature_key, db_obj in candidates[op].items():
            ftree = (features_tree / feature_key[:4] / feature_key).obj
            assert ftree.type == pygit2.GIT_OBJ_TREE

            repo_obj = core.feature_blobs_to_dict(
                repo=repo, tree_entries=ftree, geom_column_name=geom_column_name
            )

            s_old = set(repo_obj.items())
            s_new = set(db_obj.items())

            if s_old ^ s_new:
                results[op][feature_key] = (repo_obj, db_obj)

    return results


def _repr_row(row, prefix=""):
    m = []
    for k in row.keys():
        if k.startswith("__"):
            continue

        v = row[k]

        if isinstance(v, bytes):
            g = gpkg.geom_to_ogr(v)
            v = f"{g.GetGeometryName()}(...)"
            del g

        v = "␀" if v is None else v
        m.append("{prefix}{k:>40} = {v}".format(k=k, v=v, prefix=prefix))

    return "\n".join(m)
