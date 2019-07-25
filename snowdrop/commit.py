import json
import os
import uuid

import click
import pygit2

from . import core, diff, gpkg


@click.command()
@click.pass_context
@click.option("--message", "-m", required=True)
def commit(ctx, message):
    """ Record changes to the repository """
    repo_dir = ctx.obj["repo_dir"]
    repo = pygit2.Repository(repo_dir)
    if not repo or not repo.is_bare:
        raise click.BadParameter("Not an existing repository", param_hint="--repo")
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

        wcdiff = diff.db_to_tree(repo, layer, db)
        if not any(wcdiff.values()):
            raise click.ClickException("No changes to commit")

        dbcur = db.cursor()

        git_index = pygit2.Index()
        git_index.read_tree(tree)

        for k, (obj_old, obj_new) in wcdiff["META"].items():
            object_path = f"{layer}/meta/{k}"
            value = json.dumps(obj_new).encode("utf8")

            blob = repo.create_blob(value)
            idx_entry = pygit2.IndexEntry(object_path, blob, pygit2.GIT_FILEMODE_BLOB)
            git_index.add(idx_entry)
            click.secho(f"Δ {object_path}", fg="yellow")

        pk_field = gpkg.pk(db, table)

        for feature_key in wcdiff["D"].keys():
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

        for obj in wcdiff["I"]:
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

        for feature_key, (obj_old, obj_new) in wcdiff["U"].items():
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
