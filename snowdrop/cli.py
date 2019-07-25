#!/usr/bin/env python3
import json
import os
import subprocess
import sys
import uuid
from pathlib import Path

import click
import pygit2


from .core import ogr
from . import core, gpkg
from . import init, checkout


def print_version(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return

    import osgeo
    import pkg_resources  # part of setuptools
    version = pkg_resources.require("snowdrop")[0].version

    click.echo(f"Project Snowdrop v{version}")
    click.echo(f"GDAL v{osgeo._gdal.__version__}")
    click.echo(f"PyGit2 v{pygit2.__version__}; Libgit2 v{pygit2.LIBGIT2_VERSION}")
    ctx.exit()


@click.group()
@click.option(
    "repo_dir",
    "--repo",
    type=click.Path(file_okay=False, dir_okay=True),
    default=os.curdir,
    metavar="PATH",
)
@click.option(
    "--version",
    is_flag=True,
    callback=print_version,
    expose_value=False,
    is_eager=True,
    help="Show version information and exit.",
)
@click.pass_context
def cli(ctx, repo_dir):
    ctx.ensure_object(dict)
    ctx.obj["repo_dir"] = repo_dir


def _execvp(file, args):
    if "_SNOWDROP_NO_EXEC" in os.environ:
        # used in testing. This is pretty hackzy
        p = subprocess.run([file] + args[1:], capture_output=True, encoding="utf-8")
        sys.stdout.write(p.stdout)
        sys.stderr.write(p.stderr)
        sys.exit(p.returncode)
    else:
        os.execvp(file, args)


def _pc(count):
    """ Simple pluraliser for commit/commits """
    if count == 1:
        return "commit"
    else:
        return "commits"


def _pf(count):
    """ Simple pluraliser for feature/features """
    if count == 1:
        return "feature"
    else:
        return "features"


# commands from modules
cli.add_command(init.import_gpkg)
cli.add_command(checkout.checkout)


OFTMap = {
    "INTEGER": ogr.OFTInteger,
    "MEDIUMINT": ogr.OFTInteger,
    "TEXT": ogr.OFTString,
    "REAL": ogr.OFTReal,
}


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


def _build_db_diff(repo, layer, db, tree=None):
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


@cli.command()
@click.pass_context
def diff(ctx):
    """ Show changes between commits, commit and working tree, etc """
    repo_dir = ctx.obj["repo_dir"]
    repo = pygit2.Repository(repo_dir)
    if not repo or not repo.is_bare:
        raise click.BadParameter(
            "Not an existing repository", param_hint="--repo"
        )

    working_copy = core.get_working_copy(repo)
    if not working_copy:
        raise click.ClickException("No working copy? Try `snow checkout`")

    db = gpkg.db(working_copy.path, isolation_level="DEFERRED")
    with db:
        pk_field = gpkg.pk(db, working_copy.layer)

        head_tree = repo.head.peel(pygit2.Tree)
        core.assert_db_tree_match(db, working_copy.layer, head_tree)
        diff = _build_db_diff(repo, working_copy.layer, db)

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


@cli.command()
@click.pass_context
@click.option("--message", "-m", required=True)
def commit(ctx, message):
    """ Record changes to the repository """
    repo_dir = ctx.obj["repo_dir"]
    repo = pygit2.Repository(repo_dir)
    if not repo or not repo.is_bare:
        raise click.BadParameter(
            "Not an existing repository", param_hint="--repo"
        )
    commit = repo.head.peel(pygit2.Commit)
    tree = commit.tree

    if "kx.workingcopy" not in repo.config:
        raise click.UsageError("No working-copy, use 'checkout'")

    fmt, working_copy, layer = repo.config["kx.workingcopy"].split(":")
    assert os.path.isfile(working_copy), f"Working copy missing? {working_copy}"

    table = layer

    db = gpkg.db(working_copy, isolation_level="DEFERRED")
    with db:
        core.assert_db_tree_match(db, table, tree)

        diff = _build_db_diff(repo, layer, db)
        if not any(diff.values()):
            raise click.ClickException("No changes to commit")

        dbcur = db.cursor()

        git_index = pygit2.Index()
        git_index.read_tree(tree)

        for k, (obj_old, obj_new) in diff["META"].items():
            object_path = f"{layer}/meta/{k}"
            value = json.dumps(obj_new).encode("utf8")

            blob = repo.create_blob(value)
            idx_entry = pygit2.IndexEntry(object_path, blob, pygit2.GIT_FILEMODE_BLOB)
            git_index.add(idx_entry)
            click.secho(f"Δ {object_path}", fg="yellow")

        pk_field = gpkg.pk(db, table)

        for feature_key in diff["D"].keys():
            object_path = f"{layer}/features/{feature_key[:4]}/{feature_key}"
            git_index.remove_all([f"{object_path}/**"])
            click.secho(f"- {object_path}", fg="red")

            dbcur.execute(
                "DELETE FROM __kxg_map WHERE table_name=? AND feature_key=?",
                (table, feature_key),
            )
            assert (
                dbcur.rowcount == 1
            ), f"__kxg_map delete: expected 1Δ, got {dbcur.rowcount}"

        for obj in diff["I"]:
            feature_key = str(uuid.uuid4())
            for k, value in obj.items():
                object_path = f"{layer}/features/{feature_key[:4]}/{feature_key}/{k}"
                if not isinstance(value, bytes):  # blob
                    value = json.dumps(value).encode("utf8")

                blob = repo.create_blob(value)
                idx_entry = pygit2.IndexEntry(
                    object_path, blob, pygit2.GIT_FILEMODE_BLOB
                )
                git_index.add(idx_entry)
                click.secho(f"+ {object_path}", fg="green")

            dbcur.execute(
                "INSERT INTO __kxg_map (table_name, feature_key, feature_id, state) VALUES (?,?,?,0);",
                (table, feature_key, obj[pk_field]),
            )
        dbcur.execute(
            "DELETE FROM __kxg_map WHERE table_name=? AND feature_key IS NULL;",
            (table,),
        )

        for feature_key, (obj_old, obj_new) in diff["U"].items():
            s_old = set(obj_old.items())
            s_new = set(obj_new.items())

            diff_add = dict(s_new - s_old)
            diff_del = dict(s_old - s_new)
            all_keys = set(diff_del.keys()) | set(diff_add.keys())

            for k in all_keys:
                object_path = f"{layer}/features/{feature_key[:4]}/{feature_key}/{k}"
                if k in diff_add:
                    value = obj_new[k]
                    if not isinstance(value, bytes):  # blob
                        value = json.dumps(value).encode("utf8")

                    blob = repo.create_blob(value)
                    idx_entry = pygit2.IndexEntry(
                        object_path, blob, pygit2.GIT_FILEMODE_BLOB
                    )
                    git_index.add(idx_entry)
                    click.secho(f"Δ {object_path}", fg="yellow")
                else:
                    git_index.remove(object_path)
                    click.secho(f"- {object_path}", fg="red")

        dbcur.execute(
            "UPDATE __kxg_map SET state=0 WHERE table_name=? AND state != 0;", (table,)
        )

        print("Writing tree...")
        new_tree = git_index.write_tree(repo)
        print(f"Tree sha: {new_tree}")

        dbcur.execute(
            "UPDATE __kxg_meta SET value=? WHERE table_name=? AND key='tree';",
            (str(new_tree), table),
        )
        assert (
            dbcur.rowcount == 1
        ), f"__kxg_meta update: expected 1Δ, got {dbcur.rowcount}"

        print("Committing...")
        user = repo.default_signature
        # this will also update the ref (branch) to point to the current commit
        new_commit = repo.create_commit(
            "HEAD",  # reference_name
            user,  # author
            user,  # committer
            message,  # message
            new_tree,  # tree
            [repo.head.target],  # parents
        )
        print(f"Commit: {new_commit}")

        # TODO: update reflog


@cli.command()
@click.option(
    "--ff/--no-ff",
    default=True,
    help=(
        "When the merge resolves as a fast-forward, only update the branch pointer, without creating a merge commit. "
        "With --no-ff create a merge commit even when the merge resolves as a fast-forward."
    ),
)
@click.option(
    "--ff-only",
    default=False,
    is_flag=True,
    help=(
        "Refuse to merge and exit with a non-zero status unless the current HEAD is already up to date or the merge can be resolved as a fast-forward."
    ),
)
@click.argument("commit", required=True, metavar="COMMIT")
@click.pass_context
def merge(ctx, ff, ff_only, commit):
    """ Incorporates changes from the named commits (usually other branch heads) into the current branch. """
    repo_dir = ctx.obj["repo_dir"]
    repo = pygit2.Repository(repo_dir)

    if ff_only and not ff:
        raise click.BadParameter(
            "Conflicting parameters: --no-ff & --ff-only", param_hint="--ff-only"
        )

    c_base = repo[repo.head.target]

    # accept ref-ish things (refspec, branch, commit)
    c_head, r_head = repo.resolve_refish(commit)

    print(f"Merging {c_head.id} to {c_base.id} ...")
    merge_base = repo.merge_base(c_base.oid, c_head.oid)
    print(f"Found merge base: {merge_base}")

    # We're up-to-date if we're trying to merge our own common ancestor.
    if merge_base == c_head.oid:
        print("Already merged!")
        return

    # We're fastforwardable if we're our own common ancestor.
    can_ff = merge_base == c_base.id

    if ff_only and not can_ff:
        print("Can't resolve as a fast-forward merge and --ff-only specified")
        ctx.exit(1)

    if can_ff and ff:
        # do fast-forward merge
        repo.head.set_target(c_head.id, "merge: Fast-forward")
        commit_id = c_head.id
        print("Fast-forward")
    else:
        ancestor_tree = repo[merge_base].tree

        merge_index = repo.merge_trees(
            ancestor=ancestor_tree, ours=c_base.tree, theirs=c_head.tree
        )
        if merge_index.conflicts:
            print("Merge conflicts!")
            for path, (ancestor, ours, theirs) in merge_index.conflicts:
                print(f"Conflict: {path:60} {ancestor} | {ours} | {theirs}")
            ctx.exit(1)

        print("No conflicts!")
        merge_tree_id = merge_index.write_tree(repo)
        print(f"Merge tree: {merge_tree_id}")

        user = repo.default_signature
        merge_message = "Merge '{}'".format(r_head.shorthand if r_head else c_head.id)
        commit_id = repo.create_commit(
            repo.head.name,
            user,
            user,
            merge_message,
            merge_tree_id,
            [c_base.oid, c_head.oid],
        )
        print(f"Merge commit: {commit_id}")

    # update our working copy
    wc = core.get_working_copy(repo)
    click.echo(f"Updating {wc.path} ...")
    commit = repo[commit_id]
    return checkout.checkout_update(repo, wc.path, wc.layer, commit, base_commit=c_base)


def _fsck_reset(repo, working_copy, layer):
    db = gpkg.db(working_copy, isolation_level="DEFERRED")
    db.execute("PRAGMA synchronous = OFF;")
    db.execute("PRAGMA locking_mode = EXCLUSIVE;")

    db.execute("BEGIN")
    db.execute("PRAGMA defer_foreign_keys = ON;")
    db.execute("DELETE FROM __kxg_meta WHERE table_name=?;", [layer])
    db.execute("DELETE FROM __kxg_map WHERE table_name=?;", [layer])
    db.execute("DELETE FROM gpkg_metadata WHERE id IN (SELECT md_file_id FROM gpkg_metadata_reference WHERE table_name=?);", [layer])
    db.execute("DELETE FROM gpkg_metadata_reference WHERE table_name=?;", [layer])
    db.execute("DELETE FROM gpkg_geometry_columns WHERE table_name=?;", [layer])
    db.execute("DELETE FROM gpkg_contents WHERE table_name=?;", [layer])
    db.execute(f"DELETE FROM {gpkg.ident(layer)};")

    db.execute("PRAGMA defer_foreign_keys = OFF;")
    checkout.checkout_new(repo, working_copy, layer, repo.head.peel(pygit2.Commit), "GPKG", skip_create=True, db=db)


@cli.command(
    context_settings=dict(ignore_unknown_options=True),
)
@click.pass_context
@click.option("--reset-layer", default=False, is_flag=True, help="Reset the working copy for this layer")
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def fsck(ctx, reset_layer, args):
    """ Verifies the connectivity and validity of the objects in the database """
    repo_dir = ctx.obj["repo_dir"] or "."
    repo = pygit2.Repository(repo_dir)
    if not repo or not repo.is_bare:
        raise click.BadParameter(
            "Not an existing repository", param_hint="--repo"
        )

    click.echo("Checking repository integrity...")
    r = subprocess.call(["git", "-C", repo_dir, "fsck"] + list(args))
    if r:
        click.Abort()

    # now check our stuff:
    # 1. working copy

    if "kx.workingcopy" not in repo.config:
        click.echo("No working-copy configured")
        return

    fmt, working_copy, layer = repo.config["kx.workingcopy"].split(":")
    if not os.path.isfile(working_copy):
        raise click.ClickException(
            click.style(f"Working copy missing: {working_copy}", fg="red")
        )

    click.secho(f"✔︎ Working copy: {working_copy}", fg="green")
    click.echo(f"Layer: {layer}")

    if reset_layer:
        click.secho(f"Resetting working copy for {layer}...", bold=True)
        return _fsck_reset(repo, working_copy, layer)

    db = gpkg.db(working_copy, isolation_level="DEFERRED")
    with db:
        pk_field = gpkg.pk(db, layer)
        click.echo(f'Primary key field for {layer}: "{pk_field}"')

        tree = repo.head.peel(pygit2.Tree)

        # compare repo tree id to what's in the DB
        try:
            oid = core.assert_db_tree_match(db, layer, tree)
            click.secho(
                f"✔︎ Working Copy tree id matches repository: {oid}", fg="green"
            )
        except checkout.WorkingCopyMismatch as e:
            # try and find the tree we _do_ have
            click.secho(f"✘ Repository tree is: {tree.id}", fg="red")
            click.secho(f"✘ Working Copy tree is: {e.working_copy_tree_id}", fg="red")
            click.echo("This might be fixable via `checkout --force`")
            raise click.Abort()

        q = db.execute(f"SELECT COUNT(*) FROM {gpkg.ident(layer)};")
        row_count = q.fetchone()[0]
        click.echo(f"{row_count} features in {layer}")

        # __kxg_map
        click.echo("__kxg_map rows:")
        q = db.execute(
            """
            SELECT state, COUNT(*) AS count
            FROM __kxg_map
            WHERE
                table_name = ?
            GROUP BY state;
        """,
            [layer],
        )
        MAP_STATUS = {-1: "Deleted", 0: "Unchanged", 1: "Added/Updated"}
        map_state_counts = {k: 0 for k in MAP_STATUS}
        for state, count in q.fetchall():
            map_state_counts[state] = count
            click.echo(f"  {MAP_STATUS[state]}: {count}")
        map_row_count = sum(map_state_counts.values())
        click.echo(f"  Total: {map_row_count}")
        map_cur_count = map_row_count - map_state_counts[-1]  # non-deleted

        if map_row_count == row_count:
            click.secho(f"✔︎ Row counts match between __kxg_map & table", fg="green")
        elif map_cur_count != row_count:
            raise click.ClickException(click.style(f"✘ Row count mismatch between __kxg_map ({map_cur_count}) & table ({row_count})", fg="red"))
        else:
            pass

        # compare the DB to the index (meta & __kxg_map)
        index = core.db_to_index(db, layer, tree)
        diff_index = tree.diff_to_index(index)
        num_changes = len(diff_index)
        if num_changes:
            click.secho(
                f"! Working copy appears dirty according to the index: {num_changes} change(s)", fg="yellow"
            )

        meta_prefix = f"{layer}/meta/"
        meta_changes = [
            dd
            for dd in diff_index.deltas
            if dd.old_file.path.startswith(meta_prefix)
            or dd.new_file.path.startswith(meta_prefix)
        ]
        if meta_changes:
            click.secho(f"! {meta_prefix} ({len(meta_changes)}):", fg="yellow")

            for dd in meta_changes:
                m = f"  {dd.status_char()}  {dd.old_file.path}"
                if dd.new_file.path != dd.old_file.path:
                    m += f" → {dd.new_file.path}"
                click.echo(m)

        feat_prefix = f"{layer}/features/"
        feat_changes = sorted(
            [
                dd
                for dd in diff_index.deltas
                if dd.old_file.path.startswith(feat_prefix)
                or dd.new_file.path.startswith(feat_prefix)
            ],
            key=lambda d: d.old_file.path,
        )
        if feat_changes:
            click.secho(f"! {feat_prefix} ({len(feat_changes)}):", fg="yellow")

            for dd in feat_changes:
                m = f"  {dd.status_char()}  {dd.old_file.path}"
                if dd.new_file.path != dd.old_file.path:
                    m += f" → {dd.new_file.path}"
                click.echo(m)

        if num_changes:
            # can't proceed with content comparison for dirty working copies
            click.echo("Can't do any further checks")
            return

        click.echo("Checking features...")
        q = db.execute(
            f"""
            SELECT M.feature_key AS __fk, M.feature_id AS __pk, T.*
            FROM __kxg_map AS M
                LEFT OUTER JOIN {gpkg.ident(layer)} AS T
                ON (M.feature_id = T.{gpkg.ident(pk_field)})
            WHERE
                M.table_name = ?
            UNION ALL
            SELECT M.feature_key AS __fk, M.feature_id AS __pk, T.*
            FROM {gpkg.ident(layer)} AS T
                LEFT OUTER JOIN __kxg_map AS M
                ON (T.{gpkg.ident(pk_field)} = M.feature_id)
            WHERE
                M.table_name = ?
                AND M.feature_id IS NULL
            ORDER BY M.feature_key;
        """,
            [layer, layer],
        )
        has_err = False
        feature_tree = tree / layer / "features"
        for i, row in enumerate(q):
            if i and i % 1000 == 0:
                click.echo(f"  {i}...")

            fkey = row["__fk"]
            pk_m = row["__pk"]
            pk_t = row[pk_field]

            if pk_m is None:
                click.secho(f"  ✘ Missing __kxg_map feature ({pk_field}={pk_t})", fg="red")
                has_err = True
                continue
            elif pk_t is None:
                click.secho(
                    f"  ✘ Missing {layer} feature {fkey} ({pk_field}={pk_m})", fg="red"
                )
                has_err = True
                continue

            try:
                obj_tree = feature_tree / fkey[:4] / fkey
            except KeyError:
                click.secho(
                    f"  ✘ Feature {fkey} ({pk_field}={pk_m}) not found in repository",
                    fg="red",
                )
                has_err = True
                continue

            for field in row.keys():
                if field.startswith("__"):
                    continue

                try:
                    blob = (obj_tree / field).obj
                except KeyError:
                    click.secho(
                        f"  ✘ Feature {fkey} ({pk_field}={pk_m}) not found in repository",
                        fg="red",
                    )
                    has_err = True
                    continue

                value = row[field]
                if not isinstance(value, bytes):  # blob
                    value = json.dumps(value).encode("utf8")

                if blob.id != pygit2.hash(value):
                    click.secho(f"  ✘ Field value mismatch: {fkey}/{field}", fg="red")
                    has_err = True
                    continue

        if has_err:
            raise click.Abort()

    click.secho("✔︎ Everything looks good", fg="green")


@cli.command()
@click.option(
    "--ff/--no-ff",
    default=True,
    help=(
        "When the merge resolves as a fast-forward, only update the branch pointer, without creating a merge commit. "
        "With --no-ff create a merge commit even when the merge resolves as a fast-forward."
    ),
)
@click.option(
    "--ff-only",
    default=False,
    is_flag=True,
    help=(
        "Refuse to merge and exit with a non-zero status unless the current HEAD is already up to date or the merge can be resolved as a fast-forward."
    ),
)
@click.option(
    "--ff-only",
    default=False,
    is_flag=True,
    help=(
        "Refuse to merge and exit with a non-zero status unless the current HEAD is already up to date or the merge can be resolved as a fast-forward."
    ),
)
@click.argument("repository", required=False, metavar="REMOTE")
@click.argument("refspecs", nargs=-1, required=False, metavar="REFISH")
@click.pass_context
def pull(ctx, ff, ff_only, repository, refspecs):
    """ Fetch from and integrate with another repository or a local branch """
    repo_dir = ctx.obj["repo_dir"] or "."
    repo = pygit2.Repository(repo_dir)
    if not repo or not repo.is_bare:
        raise click.BadParameter(
            "Not an existing repository", param_hint="--repo"
        )

    if repository is None:
        # matches git-pull behaviour
        if repo.head_is_detached:
            raise click.UsageError((
                "You are not currently on a branch. "
                "Please specify which branch you want to merge with."
            ))

        # git-fetch:
        # When no remote is specified, by default the origin remote will be used,
        # unless there's an upstream branch configured for the current branch.

        current_branch = repo.branches[repo.head.shorthand]
        if current_branch.upstream:
            repository = current_branch.upstream.remote_name
        else:
            try:
                repository = repo.remotes['origin'].name
            except KeyError:
                # git-pull seems to just exit 0 here...?
                raise click.BadParameter("Please specify the remote you want to fetch from", param_hint="repository")

    remote = repo.remotes[repository]

    # do the fetch
    print("Running fetch:", repository, refspecs)
    remote.fetch((refspecs or None))
    # subprocess.check_call(["git", "-C", ctx.obj['repo_dir'], 'fetch', repository] + list(refspecs))

    # now merge with FETCH_HEAD
    print("Running merge:", {'ff': ff, 'ff_only': ff_only, 'commit': "FETCH_HEAD"})
    ctx.invoke(merge, ff=ff, ff_only=ff_only, commit="FETCH_HEAD")


@cli.command()
@click.pass_context
def status(ctx):
    """ Show the working copy status """
    repo_dir = ctx.obj["repo_dir"] or "."
    repo = pygit2.Repository(repo_dir)
    if not repo or not repo.is_bare:
        raise click.BadParameter(
            "Not an existing repository", param_hint="--repo"
        )

    commit = repo.head.peel(pygit2.Commit)

    if repo.head_is_detached:
        click.echo(f"{click.style('HEAD detached at', fg='red')} {commit.short_id}")
    else:
        branch = repo.branches[repo.head.shorthand]
        click.echo(f"On branch {branch.shorthand}")

        if branch.upstream:
            upstream_head = branch.upstream.peel(pygit2.Commit)
            n_ahead, n_behind = repo.ahead_behind(commit.id, upstream_head.id)
            if n_ahead == n_behind == 0:
                click.echo(f"Your branch is up to date with '{branch.upstream.shorthand}'.")
            elif n_ahead > 0 and n_behind > 0:
                click.echo((
                    f"Your branch and '{branch.upstream.shorthand}' have diverged,\n"
                    f"and have {n_ahead} and {n_behind} different commits each, respectively.\n"
                    "  (use \"snow pull\" to merge the remote branch into yours)"
                ))
            elif n_ahead > 0:
                click.echo((
                    f"Your branch is ahead of '{branch.upstream.shorthand}' by {n_ahead} {_pc(n_ahead)}.\n"
                    "  (use \"snow push\" to publish your local commits)"
                ))
            elif n_behind > 0:
                click.echo((
                    f"Your branch is behind '{branch.upstream.shorthand}' by {n_behind} {_pc(n_behind)}, "
                    "and can be fast-forwarded.\n"
                    "  (use \"snow pull\" to update your local branch)"
                ))

    # working copy state
    working_copy = core.get_working_copy(repo)
    if not working_copy:
        click.echo('\nNo working copy.\n  (use "snow checkout" to create a working copy)')
        return

    db = gpkg.db(working_copy.path, isolation_level="DEFERRED")
    with db:
        dbcur = db.cursor()

        sql = """
            SELECT state, COUNT(feature_key) AS mod, COUNT(*) AS count
            FROM __kxg_map
            WHERE
                table_name = ?
                AND state != 0
                AND NOT (feature_key IS NULL AND state < 0)  -- ignore INSERT then DELETE
            GROUP BY state;
        """
        dbcur.execute(sql, [working_copy.layer])
        change_counts = {r['state']: (r['mod'], r['count']) for r in dbcur.fetchall() if r['state'] is not None}

        # TODO: check meta/ tree

        if not change_counts:
            click.echo("\nNothing to commit, working copy clean")
        else:
            click.echo((
                "\nChanges in working copy:\n"
                '  (use "snow commit" to commit)\n'
                '  (use "snow reset" to discard changes)\n'
            ))

            if 1 in change_counts:
                n_mod = change_counts[1][0]
                n_add = change_counts[1][1] - n_mod
                if n_mod:
                    click.echo(f"    modified:   {n_mod} {_pf(n_mod)}")
                if n_add:
                    click.echo(f"    new:        {n_add} {_pf(n_add)}")

            if -1 in change_counts:
                n_del = change_counts[-1][1]
                click.echo(f"    deleted:    {n_del} {_pf(n_del)}")


@cli.command('workingcopy-set-path')
@click.pass_context
@click.argument("new", nargs=1, type=click.Path(exists=True, dir_okay=False))
def workingcopy_set_path(ctx, new):
    """ Change the path to the working-copy """
    repo_dir = ctx.obj["repo_dir"] or "."
    repo = pygit2.Repository(repo_dir)
    if not repo or not repo.is_bare:
        raise click.BadParameter(
            "Not an existing repository", param_hint="--repo"
        )

    repo_cfg = repo.config
    if "kx.workingcopy" in repo_cfg:
        fmt, path, layer = repo_cfg["kx.workingcopy"].split(":")
    else:
        raise click.ClickException("No working copy? Try `snow checkout`")

    new = Path(new)
    if not new.is_absolute():
        new = os.path.relpath(new, repo_dir)

    repo.config["kx.workingcopy"] = f"{fmt}:{new}:{layer}"


# aliases/shortcuts


@cli.command()
@click.pass_context
def show(ctx):
    """ Show the current commit """
    ctx.invoke(log, args=["-1"])


@cli.command()
@click.pass_context
def reset(ctx):
    """ Discard changes made in the working copy (ie. reset to HEAD """
    ctx.invoke(checkout.checkout, force=True, refish="HEAD")


# straight process-replace commands

@cli.command(context_settings=dict(ignore_unknown_options=True))
@click.pass_context
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def log(ctx, args):
    """ Show commit logs """
    repo_dir = ctx.obj["repo_dir"] or "."
    repo = pygit2.Repository(repo_dir)
    if not repo or not repo.is_bare:
        raise click.BadParameter(
            "Not an existing repository", param_hint="--repo"
        )

    _execvp("git", ["git", "-C", repo_dir, "log"] + list(args))


@cli.command(context_settings=dict(ignore_unknown_options=True))
@click.pass_context
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def push(ctx, args):
    """ Update remote refs along with associated objects """
    repo_dir = ctx.obj["repo_dir"] or "."
    repo = pygit2.Repository(repo_dir)
    if not repo or not repo.is_bare:
        raise click.BadParameter(
            "Not an existing repository", param_hint="--repo"
        )

    _execvp("git", ["git", "-C", repo_dir, "push"] + list(args))


@cli.command(context_settings=dict(ignore_unknown_options=True))
@click.pass_context
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def fetch(ctx, args):
    """ Download objects and refs from another repository """
    repo_dir = ctx.obj["repo_dir"] or "."
    repo = pygit2.Repository(repo_dir)
    if not repo or not repo.is_bare:
        raise click.BadParameter(
            "Not an existing repository", param_hint="--repo"
        )

    _execvp("git", ["git", "-C", repo_dir, "fetch"] + list(args))


@cli.command(context_settings=dict(ignore_unknown_options=True))
@click.pass_context
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def branch(ctx, args):
    """ List, create, or delete branches """
    repo_dir = ctx.obj["repo_dir"] or "."
    repo = pygit2.Repository(repo_dir)
    if not repo or not repo.is_bare:
        raise click.BadParameter(
            "Not an existing repository", param_hint="--repo"
        )

    _execvp("git", ["git", "-C", repo_dir, "branch"] + list(args))


@cli.command(context_settings=dict(ignore_unknown_options=True))
@click.pass_context
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def remote(ctx, args):
    """ Manage set of tracked repositories """
    repo_dir = ctx.obj["repo_dir"] or "."
    repo = pygit2.Repository(repo_dir)
    if not repo or not repo.is_bare:
        raise click.BadParameter(
            "Not an existing repository", param_hint="--repo"
        )

    _execvp("git", ["git", "-C", repo_dir, "remote"] + list(args))


@cli.command(context_settings=dict(ignore_unknown_options=True))
@click.pass_context
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def tag(ctx, args):
    """ Create, list, delete or verify a tag object signed with GPG """
    repo_dir = ctx.obj["repo_dir"] or "."
    repo = pygit2.Repository(repo_dir)
    if not repo or not repo.is_bare:
        raise click.BadParameter(
            "Not an existing repository", param_hint="--repo"
        )

    _execvp("git", ["git", "-C", repo_dir, "tag"] + list(args))


@cli.command(context_settings=dict(ignore_unknown_options=True))
@click.argument("repository", nargs=1)
@click.argument("directory", required=False)
def clone(repository, directory):
    """ Clone a repository into a new directory """
    repo_dir = directory or os.path.split(repository)[1]
    if not repo_dir.endswith(".snow") or len(repo_dir) == 4:
        raise click.BadParameter("Repository should be myproject.snow")

    subprocess.check_call(["git", "clone", "--bare", repository, repo_dir])
    subprocess.check_call(
        [
            "git",
            "-C",
            repo_dir,
            "config",
            "--local",
            "--add",
            "remote.origin.fetch",
            "+refs/heads/*:refs/remotes/origin/*",
        ]
    )
    subprocess.check_call(["git", "-C", repo_dir, "fetch"])

    repo = pygit2.Repository(repo_dir)
    head_ref = repo.head.shorthand  # master
    subprocess.check_call(
        [
            "git",
            "-C",
            repo_dir,
            "config",
            "--local",
            f"branch.{head_ref}.remote",
            "origin",
        ]
    )
    subprocess.check_call(
        [
            "git",
            "-C",
            repo_dir,
            "config",
            "--local",
            f"branch.{head_ref}.merge",
            "refs/heads/master",
        ]
    )


if __name__ == "__main__":
    cli()
