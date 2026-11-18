import os
from collections import Counter

import click

from kart.cli_util import KartCommand
from kart.exceptions import NO_WORKING_COPY, NotFound
from kart.geometry import normalise_gpkg_geom
from kart.serialise_util import msg_unpack
from kart import subprocess_util as subprocess
from kart.sqlalchemy.gpkg import Db_GPKG


# Each head is passed to `git fsck` as an argument, and Windows limits a command line
# to 32,767 characters.
MAX_HEADS_PER_GIT_FSCK = 500


def _git_fsck(repo, fsck_args):
    """
    Runs `git fsck` with the given extra arguments. Returns True if it found no problems.

    When `git fsck` isn't told which heads to start from, it starts from every ref and
    also checks the index file. It rejects the index of a Kart repository, which is
    deliberately unreadable to Git (see locked_git_index), and so fails before checking
    anything. So unless the caller named some heads, we name them: HEAD and every ref.
    Unlike Git's default, this doesn't include reflogs.
    """
    cmd = ["git", "-C", repo.path, "fsck", *fsck_args]
    if any(not arg.startswith("-") for arg in fsck_args):
        return subprocess.call(cmd) == 0

    heads = set()
    if not repo.head_is_unborn:
        heads.add(str(repo.head.target))
    for ref in repo.listall_reference_objects():
        try:
            heads.add(str(ref.resolve().target))
        except KeyError:
            # A dangling symbolic ref. Let `git fsck` report it.
            heads.add(ref.name)
    if not heads:
        # An empty repository: nothing is reachable.
        return True

    heads = sorted(heads)
    chunks = [
        heads[i : i + MAX_HEADS_PER_GIT_FSCK]
        for i in range(0, len(heads), MAX_HEADS_PER_GIT_FSCK)
    ]
    ok = True
    for i, chunk in enumerate(chunks):
        extra_args = []
        if len(chunks) > 1:
            # Each run only knows about its own heads, so it would report the objects
            # that are reachable from the other runs' heads as dangling.
            extra_args.append("--no-dangling")
        if i > 0:
            # The first run has already checked the validity of every object.
            extra_args.append("--connectivity-only")
        if subprocess.call(cmd + extra_args + chunk) != 0:
            ok = False
    return ok


def _feature_legend_hash(data):
    """
    Returns the legend hash from a feature blob, which is msgpack([legend_hash, values]).
    """
    # Fast path which doesn't decode the values: fixarray(2), then a str8 and its length.
    if len(data) > 3 and data[0] == 0x92 and data[1] == 0xD9:
        return bytes(data[3 : 3 + data[2]]).decode("ascii")
    return msg_unpack(data)[0]


def _fsck_table_dataset(dataset):
    """
    Checks that the dataset has every legend that its schema and its features name.
    Features name their legend by hash, not by a link that `git fsck` can follow,
    and can't be read without it. Returns True if no problems were found.
    """
    try:
        legends_present = {blob.name for blob in dataset.meta_tree / "legend"}
    except KeyError:
        legends_present = set()

    schema_ok = True
    schema_legend = dataset.schema.legend.hexhash()
    if schema_legend not in legends_present:
        schema_ok = False
        click.secho(
            f"✘ Legend {schema_legend} of the current schema is missing", fg="red"
        )

    feature_count = 0
    not_present_count = 0
    unreadable_count = 0
    missing_legend_counts = Counter()
    for blob in dataset.feature_blobs():
        feature_count += 1
        try:
            data = memoryview(blob)
        except KeyError:
            # Not fetched, as in a spatially filtered clone. `git fsck` accepts this too.
            not_present_count += 1
            continue
        try:
            legend_hash = _feature_legend_hash(data)
        except Exception:
            unreadable_count += 1
            continue
        if legend_hash not in legends_present:
            missing_legend_counts[legend_hash] += 1

    if not_present_count:
        click.echo(f"{not_present_count} features not present locally were skipped")
    if unreadable_count:
        click.secho(f"✘ {unreadable_count} features could not be decoded", fg="red")
    for legend_hash, count in sorted(missing_legend_counts.items()):
        click.secho(
            f"✘ {count} features refer to missing legend {legend_hash}", fg="red"
        )

    features_ok = not unreadable_count and not missing_legend_counts
    if features_ok:
        click.secho(f"✔︎ {feature_count} features checked", fg="green")
    return schema_ok and features_ok


def _fsck_reset(repo, working_copy, dataset_paths):
    commit = repo.head_commit
    datasets = [repo.datasets()[p] for p in dataset_paths]

    working_copy.drop_tables(commit, *datasets)
    working_copy.write_full(commit, *datasets)


@click.command(cls=KartCommand, context_settings=dict(ignore_unknown_options=True))
@click.pass_context
@click.option(
    "--reset-dataset",
    "reset_datasets",
    multiple=True,
    help="Reset the working copy for this dataset path",
)
@click.argument("fsck_args", nargs=-1, type=click.UNPROCESSED)
def fsck(ctx, reset_datasets, fsck_args):
    """Verifies the connectivity and validity of the objects in the database"""
    repo = ctx.obj.repo

    click.echo("Checking repository integrity...")
    if not _git_fsck(repo, fsck_args):
        raise click.Abort()

    # now check our stuff:
    # 1. datasets
    has_err = False
    if not repo.head_is_unborn:
        for dataset in repo.datasets():
            if dataset.DATASET_TYPE != "table":
                continue
            click.secho(f"\nDataset: '{dataset.path}/'", bold=True)
            if not _fsck_table_dataset(dataset):
                has_err = True
    if has_err:
        # The working copy checks need to read the features.
        raise click.Abort()

    # 2. working copy
    if not repo.workingcopy_location:
        click.echo("No working copy configured")
        return

    # TODO: this code shouldn't special-case tabular working copies
    table_wc = repo.working_copy.tabular
    if not table_wc:
        raise NotFound(
            click.style(f"Working copy missing: {table_wc}", fg="red"),
            exit_code=NO_WORKING_COPY,
        )

    click.secho(f"✔︎ Working copy: {table_wc}", fg="green")

    if reset_datasets:
        click.secho(
            f"Resetting working copy for {', '.join(reset_datasets)} ...", bold=True
        )
        return _fsck_reset(repo, table_wc, reset_datasets)

    with table_wc.session() as sess:
        tree = repo.head_tree

        # compare repo tree id to what's in the DB
        table_wc.assert_matches_tree(repo.head_tree)
        click.secho(
            f"✔︎ Working Copy tree id matches repository: {repo.head_tree}",
            fg="green",
        )

        has_err = False
        for dataset in repo.datasets():
            click.secho(
                f"\nDataset: '{dataset.path}/' (table: '{dataset.table_name}')",
                bold=True,
            )
            table = dataset.table_name

            pk = Db_GPKG.pk_name(sess, table=table)
            click.echo(f'Primary key field for table: "{pk}"')
            if pk != dataset.primary_key:
                has_err = True
                click.secho(
                    f"✘ Primary Key mismatch between repo ({dataset.primary_key}) & working-copy table ({pk})",
                    fg="red",
                )

            wc_count = sess.scalar(
                f"SELECT COUNT(*) FROM {table_wc.table_identifier(dataset)};"
            )
            click.echo(f"{wc_count} features in {table}")
            ds_count = dataset.feature_count
            if wc_count != ds_count:
                has_err = True
                click.secho(
                    f"✘ Feature Count mismatch between repo ({ds_count}) & working-copy table ({wc_count})",
                    fg="red",
                )

            track_count = sess.scalar(
                f"SELECT COUNT(*) FROM {table_wc.KART_TRACK} WHERE table_name=:table_name;",
                {"table_name": table},
            )
            click.echo(f"{track_count} rows marked as changed in working-copy")

            wc_diff = table_wc.diff_dataset_to_working_copy(dataset)
            wc_diff.prune()

            if wc_diff:
                click.secho(
                    f"! Working copy appears dirty according to the index",
                    fg="yellow",
                )

            if "meta" in wc_diff:
                meta_diff = wc_diff["meta"]
                click.secho(f"{dataset.path}:meta: ({len(meta_diff)})", fg="yellow")

                for path in meta_diff.keys():
                    click.echo(f"{dataset.path}:meta:{path}")

            if "feature" in wc_diff:
                feature_diff = wc_diff["feature"]
                click.secho(
                    f"{dataset.path}:feature: ({len(feature_diff)})", fg="yellow"
                )
                nul = "␀"

                # has feature changes
                # Note that pygit has its own names and letters for these operations - Add, Delete, Modify, Rename.
                # But, we call them insert, update and delete elsewhere in Kart - so we should be consistent here.
                for delta in feature_diff.values():
                    if delta.type == "insert":
                        click.echo(f" I   {nul:>10} → {delta.key}")
                    elif delta.type == "delete":
                        click.echo(f" D   {delta.key:>10} → {nul}")
                    else:
                        is_rename = delta.old_key != delta.new_key
                        is_update = delta.old_value != delta.new_value

                        if is_rename and is_update:
                            click.echo(f" R+U {delta.old_key:>10} → {delta.new_key}")
                        elif is_rename:
                            click.echo(f" R   {delta.old_key:>10} → {delta.new_key}")
                        elif is_update:
                            click.echo(f" U   {delta.key:>10} → {nul}")

            # can't proceed with content comparison for dirty working copies
            if wc_diff:
                click.echo("Can't do any further checks")
                return

            if not has_err:
                click.echo("Checking features...")
                feature_err_count = 0
                geom_col = dataset.geom_column_name
                for feature, blob in dataset.features_plus_blobs():
                    h_verify = os.path.basename(dataset.encode_1pk_to_path(feature[pk]))
                    if blob.name != h_verify:
                        has_err = True
                        click.secho(
                            f"✘ Hash mismatch for feature '{feature[pk]}': repo says {blob.name} but should be {h_verify}",
                            fg="red",
                        )

                    f = sess.execute(
                        f"SELECT * FROM {table_wc.table_identifier(dataset)} WHERE {table_wc.quote(pk)}=:pk;",
                        {"pk": feature[pk]},
                    )
                    db_obj = dict(f.fetchone())
                    if db_obj is not None and geom_col is not None:
                        db_obj[geom_col] = normalise_gpkg_geom(db_obj[geom_col])
                    if db_obj != feature:
                        s_old = set(feature.items())
                        s_new = set(db_obj.items())
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
                            click.secho(
                                "! More than 100 errors, stopping for now.", fg="yellow"
                            )
                            break

        if has_err:
            raise click.Abort()

    click.secho("✔︎ Everything looks good", fg="green")
