import contextlib
import itertools
import json
import re
from datetime import datetime

import click
import pygit2

from . import gpkg, core


@click.command()
@click.pass_context
@click.option("branch", "-b", help="Name for new branch")
@click.option("fmt", "--format", type=click.Choice(["GPKG"]))
@click.option("layer", "--layer")
@click.option("--force", "-f", is_flag=True)
@click.option("--working-copy", type=click.Path(writable=True, dir_okay=False))
@click.argument("refish", default=None, required=False)
def checkout(ctx, branch, refish, working_copy, layer, force, fmt):
    """ Switch branches or restore working tree files """
    repo_dir = ctx.obj["repo_dir"]
    repo = pygit2.Repository(repo_dir)
    if not repo or not repo.is_bare:
        raise click.BadParameter("Not an existing repository", param_hint="--repo")

    # refish could be:
    # - branch name
    # - tag name
    # - remote branch
    # - HEAD
    # - HEAD~1/etc
    # - 'c0ffee' commit ref
    # - 'refs/tags/1.2.3' some other refspec

    base_commit = repo.head.peel(pygit2.Commit)
    head_ref = None

    if refish:
        commit, ref = repo.resolve_refish(refish)
        head_ref = ref.name if ref else commit.id
    else:
        commit = base_commit
        head_ref = repo.head.name

    if branch:
        if branch in repo.branches:
            raise click.BadParameter(
                f"A branch named '{branch}' already exists.", param_hint="branch"
            )

        if refish and refish in repo.branches.remote:
            print(f"Creating new branch '{branch}' to track '{refish}'...")
            new_branch = repo.create_branch(branch, commit, force)
            new_branch.upstream = repo.branches.remote[refish]
        elif refish and refish in repo.branches:
            print(f"Creating new branch '{branch}' from '{refish}'...")
            new_branch = repo.create_branch(branch, commit, force)
        else:
            print(f"Creating new branch '{branch}'...")
            new_branch = repo.create_branch(branch, commit, force)

        head_ref = new_branch.name

    repo.set_head(head_ref)

    wc = core.get_working_copy(repo)
    if wc:
        if working_copy is not None:
            raise click.BadParameter(
                f"This repository already has a working copy at: {wc.path}",
                param_hint="WORKING_COPY",
            )

        click.echo(f"Updating {wc.path} ...")
        return checkout_update(
            repo, wc.path, wc.layer, commit, force=force, base_commit=base_commit
        )

    # new working-copy path
    if not working_copy:
        raise click.BadParameter(
            "No existing working copy, specify --working-copy path",
            param_hint="--working-copy",
        )
    if not layer:
        raise click.BadParameter(
            "No existing working copy, specify layer", param_hint="--layer"
        )

    if not fmt:
        fmt = "GPKG"

    click.echo(f'Checkout {layer}@{refish or "HEAD"} to {working_copy} as {fmt} ...')

    checkout_new(repo, working_copy, layer, commit, fmt)


def checkout_new(repo, working_copy, layer, commit, fmt, skip_create=False, db=None):
    if fmt != "GPKG":
        raise NotImplementedError(fmt)

    repo.reset(commit.id, pygit2.GIT_RESET_SOFT)

    tree = commit.tree
    click.echo(f"Commit: {commit.hex} Tree: {tree.hex}")

    layer_tree = (commit.tree / layer).obj

    from .working_copy import WorkingCopy
    from .structure import DatasetStructure

    ds = DatasetStructure.instantiate(layer_tree, layer)

    wc = WorkingCopy.new(repo, working_copy, version=0, table=layer)
    wc.create()
    wc.write_full(commit, ds)
    wc.save_config()


def checkout_update(repo, working_copy, layer, commit, force=False, base_commit=None):
    table = layer
    tree = commit.tree

    db = gpkg.db(working_copy, isolation_level="DEFERRED")
    db.execute("PRAGMA synchronous = OFF;")
    with db:
        dbcur = db.cursor()

        # this is where we're starting from
        if not base_commit:
            base_commit = repo.head.peel(pygit2.Commit)
        base_tree = base_commit.tree
        try:
            core.assert_db_tree_match(db, table, base_tree.id)
        except core.WorkingCopyMismatch as e:
            if force:
                try:
                    # try and find the tree we _do_ have
                    base_tree = repo[e.working_copy_tree_id]
                    assert isinstance(base_tree, pygit2.Tree)
                    print(f"Warning: {e}")
                except ValueError:
                    raise e
            else:
                raise

        re_obj_feature_path = re.compile(
            f"[0-9a-f]{{4}}/(?P<fk>[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}})/"
        )
        re_obj_full_path = re.compile(
            f"{re.escape(layer)}/features/[0-9a-f]{{4}}/(?P<fk>[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}})/"
        )

        def _get_feature_key_a(diff):
            m = re_obj_feature_path.match(diff.old_file.path)
            assert (
                m
            ), f"Diff object path doesn't match expected path pattern? '{diff.old_file.path}'"
            return m.group("fk")

        def _get_feature_key_a_full(diff):
            m = re_obj_full_path.match(diff.old_file.path)
            assert (
                m
            ), f"Diff object path doesn't match expected path pattern? '{diff.old_file.path}'"
            return m.group("fk")

        def _get_feature_key_b(diff):
            m = re_obj_feature_path.match(diff.new_file.path)
            assert (
                m
            ), f"Diff object path doesn't match expected path pattern? '{diff.new_file.path}'"
            return m.group("fk")

        def _filter_delta_status(delta_list, *statuses):
            return filter(lambda d: d.status in statuses, delta_list)

        # todo: suspend/remove spatial index
        with suspend_triggers(db, table):
            # check for dirty working copy
            dbcur.execute("SELECT COUNT(*) FROM __kxg_map WHERE state != 0;")
            is_dirty = dbcur.fetchone()[0]
            if is_dirty and not force:
                raise click.ClickException(
                    "You have uncommitted changes in your working copy. Commit or use --force to discard."
                )

            # check for schema differences
            # TODO: libgit2 supports pathspec, pygit2 doesn't
            base_meta_tree = (base_tree / layer / "meta").obj
            meta_tree = (tree / layer / "meta").obj
            if base_meta_tree.diff_to_tree(meta_tree):
                raise NotImplementedError(
                    "Sorry, no way to do changeset/meta/schema updates yet"
                )

            meta_tree = commit.tree / layer / "meta"
            meta_cols = json.loads((meta_tree / "sqlite_table_info").obj.data)
            meta_geom = json.loads((meta_tree / "gpkg_geometry_columns").obj.data)
            geom_column_name = meta_geom["column_name"] if meta_geom else None

            cols, pk_field = _get_columns(meta_cols)
            col_names = cols.keys()

            sql_insert_feature = f"INSERT INTO {gpkg.ident(table)} ({','.join([gpkg.ident(k) for k in col_names])}) VALUES ({','.join(['?']*len(col_names))});"
            sql_insert_id = "INSERT INTO __kxg_map (table_name, feature_key, feature_id, state) VALUES (?,?,?,0);"

            sql_delete_feature = (
                f"DELETE FROM {gpkg.ident(table)} WHERE {gpkg.ident(pk_field)}=?;"
            )
            sql_delete_id = (
                f"DELETE FROM __kxg_map WHERE table_name=? AND feature_key=?;"
            )

            if is_dirty:
                # force: reset changes
                index = core.db_to_index(db, layer, base_tree)
                diff_index = base_tree.diff_to_index(index)
                diff_index_list = list(diff_index.deltas)
                diff_index_list.sort(key=lambda d: (d.old_file.path, d.new_file.path))

                wip_features = []
                for feature_key, feature_diffs in itertools.groupby(
                    _filter_delta_status(diff_index_list, pygit2.GIT_DELTA_DELETED),
                    _get_feature_key_a_full,
                ):
                    feature = diff_feature_to_dict(
                        repo, feature_diffs, geom_column_name, select="old"
                    )
                    wip_features.append([feature[c] for c in col_names])

                if wip_features:
                    dbcur.executemany(sql_insert_feature, wip_features)
                    assert dbcur.rowcount == len(
                        wip_features
                    ), f"checkout-reset delete: expected Δ{len(wip_features)} changes, got {dbcur.rowcount}"

                # updates
                for feature_key, feature_diffs in itertools.groupby(
                    _filter_delta_status(diff_index_list, pygit2.GIT_DELTA_MODIFIED),
                    _get_feature_key_a_full,
                ):
                    feature = diff_feature_to_dict(
                        repo, feature_diffs, geom_column_name, select="old"
                    )

                    if feature:
                        sql_update_feature = f"""
                            UPDATE {gpkg.ident(table)}
                            SET {','.join([f'{gpkg.ident(k)}=?' for k in feature.keys()])}
                            WHERE {gpkg.ident(pk_field)}=(SELECT feature_id FROM __kxg_map WHERE table_name=? AND feature_key=?);
                        """
                        params = list(feature.values()) + [table, feature_key]
                        dbcur.execute(sql_update_feature, params)
                        assert (
                            dbcur.rowcount == 1
                        ), f"checkout-reset update: expected Δ1, got {dbcur.rowcount}"

                        if pk_field in feature:
                            # pk change
                            sql_update_id = f"UPDATE __kxg_map SET feature_id=? WHERE table_name=? AND feature_key=?;"
                            dbcur.execute(
                                sql_update_id, (feature[pk_field], table, feature_key)
                            )
                            assert (
                                dbcur.rowcount == 1
                            ), f"checkout update-id: expected Δ1, got {dbcur.rowcount}"

                # unexpected things
                unsupported_deltas = _filter_delta_status(
                    diff_index_list,
                    pygit2.GIT_DELTA_COPIED,
                    pygit2.GIT_DELTA_IGNORED,
                    pygit2.GIT_DELTA_RENAMED,
                    pygit2.GIT_DELTA_TYPECHANGE,
                    pygit2.GIT_DELTA_UNMODIFIED,
                    pygit2.GIT_DELTA_UNREADABLE,
                    pygit2.GIT_DELTA_UNTRACKED,
                )
                if any(unsupported_deltas):
                    raise NotImplementedError(
                        "Deltas for unsupported diff states:\n"
                        + diff_index.stats.format(
                            pygit2.GIT_DIFF_STATS_FULL
                            | pygit2.GIT_DIFF_STATS_INCLUDE_SUMMARY,
                            80,
                        )
                    )

                # delete added features
                dbcur.execute(
                    f"""
                    DELETE FROM {gpkg.ident(table)}
                    WHERE {gpkg.ident(pk_field)} IN (
                        SELECT feature_id FROM __kxg_map WHERE state != 0 AND feature_key IS NULL
                    );
                """
                )
                dbcur.execute(
                    f"""
                    DELETE FROM __kxg_map
                    WHERE state != 0 AND feature_key IS NULL;
                """
                )

                # reset other changes
                dbcur.execute(
                    f"""
                    UPDATE __kxg_map SET state = 0;
                """
                )

            # feature diff
            base_index_tree = (base_tree / layer / "features").obj
            index_tree = (tree / layer / "features").obj
            diff_index = base_index_tree.diff_to_tree(index_tree)
            diff_index_list = list(diff_index.deltas)
            diff_index_list.sort(key=lambda d: (d.old_file.path, d.new_file.path))

            # deletes
            wip_features = []
            wip_idmap = []
            for feature_key, feature_diffs in itertools.groupby(
                _filter_delta_status(diff_index_list, pygit2.GIT_DELTA_DELETED),
                _get_feature_key_a,
            ):
                feature = diff_feature_to_dict(
                    repo, feature_diffs, geom_column_name, select="old"
                )
                wip_features.append((feature[pk_field],))
                wip_idmap.append((table, feature_key))

            if wip_features:
                dbcur.executemany(sql_delete_feature, wip_features)
                assert dbcur.rowcount == len(
                    wip_features
                ), f"checkout delete: expected Δ{len(wip_features)} changes, got {dbcur.rowcount}"
                dbcur.executemany(sql_delete_id, wip_idmap)
                assert dbcur.rowcount == len(
                    wip_features
                ), f"checkout delete-id: expected Δ{len(wip_features)} changes, got {dbcur.rowcount}"

            # updates
            for feature_key, feature_diffs in itertools.groupby(
                _filter_delta_status(diff_index_list, pygit2.GIT_DELTA_MODIFIED),
                _get_feature_key_a,
            ):
                feature = diff_feature_to_dict(
                    repo, feature_diffs, geom_column_name, select="new"
                )

                if feature:
                    sql_update_feature = f"""
                        UPDATE {gpkg.ident(table)}
                        SET {','.join([f'{gpkg.ident(k)}=?' for k in feature.keys()])}
                        WHERE {gpkg.ident(pk_field)}=(SELECT feature_id FROM __kxg_map WHERE table_name=? AND feature_key=?);
                    """
                    params = list(feature.values()) + [table, feature_key]
                    dbcur.execute(sql_update_feature, params)
                    assert (
                        dbcur.rowcount == 1
                    ), f"checkout update: expected Δ1, got {dbcur.rowcount}"

                    if pk_field in feature:
                        # pk change
                        sql_update_id = f"UPDATE __kxg_map SET feature_id=? WHERE table_name=? AND feature_key=?;"
                        dbcur.execute(
                            sql_update_id, (feature[pk_field], table, feature_key)
                        )
                        assert (
                            dbcur.rowcount == 1
                        ), f"checkout update-id: expected Δ1, got {dbcur.rowcount}"

            # adds/inserts
            wip_features = []
            wip_idmap = []
            for feature_key, feature_diffs in itertools.groupby(
                _filter_delta_status(diff_index_list, pygit2.GIT_DELTA_ADDED),
                _get_feature_key_b,
            ):
                feature = diff_feature_to_dict(
                    repo, feature_diffs, geom_column_name, select="new"
                )
                wip_features.append([feature[c] for c in col_names])
                wip_idmap.append((table, feature_key, feature[pk_field]))

            if wip_features:
                dbcur.executemany(sql_insert_feature, wip_features)
                dbcur.executemany(sql_insert_id, wip_idmap)

            # unexpected things
            unsupported_deltas = _filter_delta_status(
                diff_index_list,
                pygit2.GIT_DELTA_COPIED,
                pygit2.GIT_DELTA_IGNORED,
                pygit2.GIT_DELTA_RENAMED,
                pygit2.GIT_DELTA_TYPECHANGE,
                pygit2.GIT_DELTA_UNMODIFIED,
                pygit2.GIT_DELTA_UNREADABLE,
                pygit2.GIT_DELTA_UNTRACKED,
            )
            if any(unsupported_deltas):
                raise NotImplementedError(
                    "Deltas for unsupported diff states:\n"
                    + diff_index.stats.format(
                        pygit2.GIT_DIFF_STATS_FULL
                        | pygit2.GIT_DIFF_STATS_INCLUDE_SUMMARY,
                        80,
                    )
                )

            # Update gpkg_contents
            commit_time = datetime.utcfromtimestamp(commit.commit_time)
            if geom_column_name is not None:
                dbcur.execute(
                    f"""
                    UPDATE gpkg_contents
                    SET
                        last_change=?,
                        min_x=(SELECT ST_MinX({gpkg.ident(geom_column_name)}) FROM {gpkg.ident(table)}),
                        min_y=(SELECT ST_MinY({gpkg.ident(geom_column_name)}) FROM {gpkg.ident(table)}),
                        max_x=(SELECT ST_MaxX({gpkg.ident(geom_column_name)}) FROM {gpkg.ident(table)}),
                        max_y=(SELECT ST_MaxY({gpkg.ident(geom_column_name)}) FROM {gpkg.ident(table)})
                    WHERE
                        table_name=?;
                    """,
                    (
                        commit_time.strftime(
                            "%Y-%m-%dT%H:%M:%S.%fZ"
                        ),  # GPKG Spec Req.15
                        table,
                    ),
                )
            else:
                dbcur.execute(
                    f"""
                    UPDATE gpkg_contents
                    SET
                        last_change=?
                    WHERE
                        table_name=?;
                    """,
                    (
                        commit_time.strftime(
                            "%Y-%m-%dT%H:%M:%S.%fZ"
                        ),  # GPKG Spec Req.15
                        table,
                    ),
                )

            assert (
                dbcur.rowcount == 1
            ), f"gpkg_contents update: expected 1Δ, got {dbcur.rowcount}"

            # update the tree id
            db.execute(
                "UPDATE __kxg_meta SET value=? WHERE table_name=? AND key='tree';",
                (tree.hex, table),
            )

            repo.reset(commit.oid, pygit2.GIT_RESET_SOFT)


def _get_columns(meta_cols):
    pk_field = None
    cols = {}
    for col in meta_cols:
        col_spec = f"{gpkg.ident(col['name'])} {col['type']}"
        if col["pk"]:
            col_spec += " PRIMARY KEY"
            pk_field = col["name"]
        if col["notnull"]:
            col_spec += " NOT NULL"
        cols[col["name"]] = col_spec

    return cols, pk_field


@contextlib.contextmanager
def suspend_triggers(db, table):
    """
    Context manager to suspend triggers (drop & recreate)
    Switches the DB into exclusive locking mode if it isn't already.
    Starts a transaction if we're not in one already
    """
    if not db.in_transaction:
        cm = db
    else:
        cm = contextlib.nullcontext()

    with cm:
        dbcur = db.cursor()
        dbcur.execute("PRAGMA locking_mode;")
        orig_locking = dbcur.fetchone()[0]

        if orig_locking.lower() != "exclusive":
            dbcur.execute("PRAGMA locking_mode=EXCLUSIVE;")

        try:
            # if we error here just bail out, we're in a transaction anyway
            drop_triggers(db, table)
            yield
            create_triggers(db, table)
        finally:
            dbcur.execute(f"PRAGMA locking_mode={orig_locking};")
            # Simply setting the locking-mode to NORMAL is not enough
            # - locks are not released until the next time the database file is accessed.
            dbcur.execute(f"SELECT table_name FROM gpkg_contents LIMIT 1;")


def drop_triggers(dbcur, table):
    dbcur.execute(
        f"""
        DROP TRIGGER IF EXISTS {gpkg.ident(f"__kxg_{table}_ins")};
    """
    )
    dbcur.execute(
        f"""
        DROP TRIGGER IF EXISTS {gpkg.ident(f"__kxg_{table}_upd")};
    """
    )
    dbcur.execute(
        f"""
        DROP TRIGGER IF EXISTS {gpkg.ident(f"__kxg_{table}_del")};
    """
    )


def create_triggers(dbcur, table):
    # sqlite doesn't let you do param substitutions in CREATE TRIGGER
    pk = gpkg.pk(dbcur, table)

    dbcur.execute(
        f"""
        CREATE TRIGGER {gpkg.ident(f"__kxg_{table}_ins")}
           AFTER INSERT
           ON {gpkg.ident(table)}
        BEGIN
            INSERT INTO __kxg_map (table_name, feature_key, feature_id, state)
                VALUES ({gpkg.param_str(table)}, NULL, NEW.{gpkg.ident(pk)}, 1);
        END;
    """
    )
    dbcur.execute(
        f"""
        CREATE TRIGGER {gpkg.ident(f"__kxg_{table}_upd")}
           AFTER UPDATE
           ON {gpkg.ident(table)}
        BEGIN
            UPDATE __kxg_map
                SET state=1, feature_id=NEW.{gpkg.ident(pk)}
                WHERE table_name={gpkg.param_str(table)}
                    AND feature_id=OLD.{gpkg.ident(pk)}
                    AND state >= 0;
        END;
    """
    )
    dbcur.execute(
        f"""
        CREATE TRIGGER {gpkg.ident(f"__kxg_{table}_del")}
           AFTER DELETE
           ON {gpkg.ident(table)}
        BEGIN
            UPDATE __kxg_map
            SET state=-1
            WHERE table_name={gpkg.param_str(table)}
                AND feature_id=OLD.{gpkg.ident(pk)};
        END;
    """
    )


def diff_feature_to_dict(repo, diff_deltas, geom_column_name, select):
    o = {}
    for dd in diff_deltas:
        if select == "old":
            df = dd.old_file
        elif select == "new":
            df = dd.new_file
        else:
            raise ValueError("select should be 'old' or 'new'")

        blob = repo[df.id]
        assert isinstance(blob, pygit2.Blob)

        name = df.path.rsplit("/", 1)[-1]
        if geom_column_name is not None and name == geom_column_name:
            value = blob.data
        else:
            value = json.loads(blob.data)
        o[name] = value
    return o


@click.command()
@click.pass_context
@click.option("--create", "-c", help="Create a new branch")
@click.option("--force-create", "-C", help="Similar to --create except that if <new-branch> already exists, it will be reset to <start-point>")
@click.option("--discard-changes", is_flag=True, help="Discard local changes")
@click.argument("refish", default=None, required=False)
def switch(ctx, create, force_create, discard_changes, refish):
    """
    Switch branches

    Switch to a specified branch. The working copy and the index are updated
    to match the branch. All new commits will be added to the tip of this
    branch.

    Optionally a new branch could be created with either -c, -C, automatically
    from a remote branch of same name.

    REFISH is either the branch name to switch to, or start-point of new branch for -c/--create.
    """
    from .structure import RepositoryStructure

    repo_dir = ctx.obj["repo_dir"]
    repo = pygit2.Repository(repo_dir)
    if not repo:
        raise click.BadParameter("Not an existing repository", param_hint="--repo")

    if create and force_create:
        raise click.BadParameter("-c/--create and -C/--force-create are incompatible")

    elif create or force_create:
        # New Branch
        new_branch = force_create or create
        is_force = bool(force_create)

        # refish could be:
        # - branch name
        # - tag name
        # - remote branch
        # - HEAD
        # - HEAD~1/etc
        # - 'c0ffee' commit ref
        # - 'refs/tags/1.2.3' some other refspec
        start_point = refish
        if start_point:
            commit, ref = repo.resolve_refish(start_point)
        else:
            commit = repo.head.peel(pygit2.Commit)

        if new_branch in repo.branches and not force_create:
            raise click.BadParameter(
                f"A branch named '{new_branch}' already exists.", param_hint="create"
            )

        if start_point and start_point in repo.branches.remote:
            print(f"Creating new branch '{new_branch}' to track '{start_point}'...")
            b_new = repo.create_branch(new_branch, commit, is_force)
            b_new.upstream = repo.branches.remote[start_point]
        elif start_point and start_point in repo.branches:
            print(f"Creating new branch '{new_branch}' from '{start_point}'...")
            b_new = repo.create_branch(new_branch, commit, is_force)
        else:
            print(f"Creating new branch '{new_branch}'...")
            b_new = repo.create_branch(new_branch, commit, is_force)

        head_ref = b_new.name

    else:
        # Switch to existing branch
        #
        # refish could be:
        # - branch name
        try:
            branch = repo.branches[refish]
        except KeyError:
            raise click.BadParameter(
                f"Branch '{refish}' not found."
            )

        commit = branch.peel(pygit2.Commit)
        head_ref = branch.name

    repo.set_head(head_ref)

    repo_structure = RepositoryStructure(repo)
    working_copy = repo_structure.working_copy
    if working_copy:
        click.echo(f"Updating {working_copy.path} ...")
        working_copy.reset(commit, repo_structure, force=discard_changes)

    repo.reset(commit.oid, pygit2.GIT_RESET_SOFT)
