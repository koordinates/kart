import json
import os
import subprocess

import click
import pygit2

from . import core, checkout, gpkg


def _fsck_reset(repo, working_copy, layer):
    db = gpkg.db(working_copy, isolation_level="DEFERRED")
    db.execute("PRAGMA synchronous = OFF;")
    db.execute("PRAGMA locking_mode = EXCLUSIVE;")

    db.execute("BEGIN")
    db.execute("PRAGMA defer_foreign_keys = ON;")
    db.execute("DELETE FROM __kxg_meta WHERE table_name=?;", [layer])
    db.execute("DELETE FROM __kxg_map WHERE table_name=?;", [layer])
    db.execute(
        "DELETE FROM gpkg_metadata WHERE id IN (SELECT md_file_id FROM gpkg_metadata_reference WHERE table_name=?);",
        [layer],
    )
    db.execute("DELETE FROM gpkg_metadata_reference WHERE table_name=?;", [layer])
    db.execute("DELETE FROM gpkg_geometry_columns WHERE table_name=?;", [layer])
    db.execute("DELETE FROM gpkg_contents WHERE table_name=?;", [layer])
    db.execute(f"DELETE FROM {gpkg.ident(layer)};")

    db.execute("PRAGMA defer_foreign_keys = OFF;")
    checkout.checkout_new(
        repo,
        working_copy,
        layer,
        repo.head.peel(pygit2.Commit),
        "GPKG",
        skip_create=True,
        db=db,
    )


@click.command(context_settings=dict(ignore_unknown_options=True))
@click.pass_context
@click.option(
    "--reset-layer",
    default=False,
    is_flag=True,
    help="Reset the working copy for this layer",
)
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def fsck(ctx, reset_layer, args):
    """ Verifies the connectivity and validity of the objects in the database """
    repo_dir = ctx.obj["repo_dir"] or "."
    repo = pygit2.Repository(repo_dir)
    if not repo or not repo.is_bare:
        raise click.BadParameter("Not an existing repository", param_hint="--repo")

    click.echo("Checking repository integrity...")
    r = subprocess.call(["git", "-C", repo_dir, "fsck"] + list(args))
    if r:
        click.Abort()

    # now check our stuff:
    # 1. working copy

    if "snow.workingcopy" not in repo.config:
        click.echo("No working-copy configured")
        return

    fmt, working_copy, layer = repo.config["snow.workingcopy"].split(":")
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
            raise click.ClickException(
                click.style(
                    f"✘ Row count mismatch between __kxg_map ({map_cur_count}) & table ({row_count})",
                    fg="red",
                )
            )
        else:
            pass

        # compare the DB to the index (meta & __kxg_map)
        index = core.db_to_index(db, layer, tree)
        diff_index = tree.diff_to_index(index)
        num_changes = len(diff_index)
        if num_changes:
            click.secho(
                f"! Working copy appears dirty according to the index: {num_changes} change(s)",
                fg="yellow",
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
                click.secho(
                    f"  ✘ Missing __kxg_map feature ({pk_field}={pk_t})", fg="red"
                )
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
