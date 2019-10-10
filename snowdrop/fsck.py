import os
import subprocess

import click
import pygit2
from osgeo import gdal

from . import core, gpkg
from .structure import RepositoryStructure


def _fsck_reset(repo_structure, working_copy, dataset_paths):
    datasets = [repo_structure[p] for p in dataset_paths]

    for ds in datasets:
        table = ds.name
        if ds.has_geometry:
            gdal_ds = gdal.OpenEx(
                str(working_copy.full_path),
                gdal.OF_VECTOR | gdal.OF_UPDATE | gdal.OF_VERBOSE_ERROR,
                ["GPKG"],
            )
            gdal_ds.ExecuteSQL(
                f"SELECT DisableSpatialIndex({gpkg.ident(table)}, {gpkg.ident(ds.geom_column_name)});"
            )
            del gdal_ds

        with working_copy.session() as db:
            db.execute("PRAGMA defer_foreign_keys = ON;")
            try:
                working_copy._drop_triggers(db, table)

                db.execute("""DELETE FROM ".sno-meta" WHERE table_name=?;""", [table])
                db.execute("""DELETE FROM ".sno-track" WHERE table_name=?;""", [table])
                db.execute(
                    "DELETE FROM gpkg_metadata WHERE id IN (SELECT md_file_id FROM gpkg_metadata_reference WHERE table_name=?);",
                    [table],
                )
                db.execute("DELETE FROM gpkg_metadata_reference WHERE table_name=?;", [table])
                db.execute("DELETE FROM gpkg_geometry_columns WHERE table_name=?;", [table])
                db.execute("DELETE FROM gpkg_contents WHERE table_name=?;", [table])

                db.execute(f"DROP TABLE {gpkg.ident(table)};")
            finally:
                db.execute("PRAGMA defer_foreign_keys = OFF;")

    commit = repo_structure.repo.head.peel(pygit2.Commit)
    for ds in datasets:
        working_copy.write_full(commit, ds)


@click.command(context_settings=dict(ignore_unknown_options=True))
@click.pass_context
@click.option(
    "--reset-dataset",
    "reset_datasets",
    multiple=True,
    help="Reset the working copy for this dataset path",
)
@click.argument("fsck_args", nargs=-1, type=click.UNPROCESSED)
def fsck(ctx, reset_datasets, fsck_args):
    """ Verifies the connectivity and validity of the objects in the database """
    repo_dir = ctx.obj["repo_dir"] or "."
    repo = pygit2.Repository(repo_dir)
    if not repo or not repo.is_bare:
        raise click.BadParameter("Not an existing repository", param_hint="--repo")

    click.echo("Checking repository integrity...")
    r = subprocess.call(["git", "-C", repo_dir, "fsck"] + list(fsck_args))
    if r:
        click.Abort()

    # now check our stuff:
    # 1. working copy
    rs = RepositoryStructure(repo)

    if "snowdrop.workingcopy.path" not in repo.config:
        click.echo("No working-copy configured")
        return

    working_copy_path = repo.config["snowdrop.workingcopy.path"]
    if not os.path.isfile(working_copy_path):
        raise click.ClickException(
            click.style(f"Working copy missing: {working_copy_path}", fg="red")
        )
    working_copy = rs.working_copy

    click.secho(f"✔︎ Working copy: {working_copy_path}", fg="green")

    if reset_datasets:
        click.secho(f"Resetting working copy for {', '.join(reset_datasets)} ...", bold=True)
        return _fsck_reset(rs, working_copy, reset_datasets)

    with working_copy.session() as db:
        tree = repo.head.peel(pygit2.Tree)

        # compare repo tree id to what's in the DB
        try:
            oid = working_copy.assert_db_tree_match(repo.head.peel(pygit2.Tree))
            click.secho(
                f"✔︎ Working Copy tree id matches repository: {oid}", fg="green"
            )
        except core.WorkingCopyMismatch as e:
            # try and find the tree we _do_ have
            click.secho(f"✘ Repository tree is: {tree.id}", fg="red")
            click.secho(f"✘ Working Copy tree is: {e.working_copy_tree_id}", fg="red")
            click.echo("This might be fixable via `checkout --force`")
            raise click.Abort()

        has_err = False
        for dataset in rs:
            click.secho(f"\nDataset: '{dataset.path}/' (table: '{dataset.name}')", bold=True)
            table = dataset.name

            pk = gpkg.pk(db, table)
            click.echo(f'Primary key field for table: "{pk}"')
            if pk != dataset.primary_key:
                has_err = True
                click.secho(
                    f"✘ Primary Key mismatch between repo ({dataset.primary_key}) & working-copy table ({pk})",
                    fg="red",
                )

            q = db.execute(f"SELECT COUNT(*) FROM {gpkg.ident(table)};")
            wc_count = q.fetchone()[0]
            click.echo(f"{wc_count} features in {table}")
            ds_count = dataset.feature_count(fast=False)
            if wc_count != ds_count:
                has_err = True
                click.secho(
                    f"✘ Feature Count mismatch between repo ({ds_count}) & working-copy table ({wc_count})",
                    fg="red",
                )

            q = db.execute(f"SELECT COUNT(*) FROM {working_copy.TRACKING_TABLE} WHERE table_name=?;", [table])
            track_count = q.fetchone()[0]
            click.echo(f"{track_count} rows marked as changed in working-copy")

            wc_diff = working_copy.diff_db_to_tree(dataset)
            if wc_diff:
                click.secho(
                    f"! Working copy appears dirty according to the index: {len(wc_diff)} change(s)",
                    fg="yellow",
                )

                meta_diff = wc_diff[dataset.path]['META']
                if meta_diff:
                    click.secho(f"! META ({len(meta_diff)}):", fg="yellow")

                    for path in meta_diff.keys():
                        click.echo(path)

                if sum([len(wc_diff[dataset.path][i]) for i in ['I', 'U', 'D']]):
                    # has feature changes
                    for v in wc_diff[dataset.path]['I']:
                        click.echo(f" A  {v[pk]}")

                    for h, v in wc_diff[dataset.path]['D'].items():
                        click.echo(f" D  {v[pk]}")

                    for h, (v_old, v_new) in wc_diff[dataset.path]['U'].items():
                        s_old = set(v_old.items())
                        s_new = set(v_new.items())
                        diff_add = dict(s_new - s_old)
                        diff_del = dict(s_old - s_new)
                        all_keys = sorted(set(diff_del.keys()) | set(diff_add.keys()))

                        if all_keys == {pk}:
                            click.echo(f" R  {v_old[pk]} → {v_new[pk]}")
                        elif pk in all_keys:
                            click.echo(f" RM {v_old[pk]} → {v_new[pk]}")
                        else:
                            click.echo(f" M  {v_old[pk]}")

                # can't proceed with content comparison for dirty working copies
                click.echo("Can't do any further checks")
                return

            if not has_err:
                click.echo("Checking features...")
                feature_err_count = 0
                for pk_hash, feature in dataset.features(fast=False):
                    h_verify = dataset.encode_pk(feature[pk])

                    if pk_hash != h_verify:
                        has_err = True
                        click.secho(
                            f"✘ Hash mismatch for feature '{feature[pk]}': repo says {pk_hash} but should be {h_verify}",
                            fg="red",
                        )

                    row = db.execute(f"SELECT * FROM {gpkg.ident(table)} WHERE {gpkg.ident(pk)}=?;", [feature[pk]]).fetchone()
                    if dict(row) != feature:
                        s_old = set(feature.items())
                        s_new = set(dict(row).items())
                        diff_add = dict(s_new - s_old)
                        diff_del = dict(s_old - s_new)
                        all_keys = sorted(set(diff_del.keys()) | set(diff_add.keys()))

                        has_err = True
                        click.secho(
                            f"✘ Mismatch between repository and working-copy for feature {pk}={feature[pk]}: fields: {', '.join(all_keys)}",
                            fg="red",
                        )

                        feature_err_count += 1
                        if feature_err_count == 100:
                            click.secho("! More than 100 errors, stopping for now.", fg="yellow")
                            break

        if has_err:
            raise click.Abort()

    click.secho("✔︎ Everything looks good", fg="green")
