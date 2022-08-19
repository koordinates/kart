import uuid
from datetime import datetime
from pathlib import Path

import click
import pygit2

from kart import checkout, context
from kart.exceptions import InvalidOperation, NotFound
from kart.fast_import import FastImportSettings, ReplaceExisting, fast_import_tables
from kart.repo import KartConfigKeys, KartRepo
from kart.tabular.version import DEFAULT_NEW_REPO_VERSION
from kart.structure import RepoStructure
from kart.tabular.v2 import TableV2
from kart.timestamps import minutes_to_tz_offset
from kart.cli_util import KartCommand


def dataset_class_for_legacy_version(version, in_place=False):
    from .v0 import TableV0
    from .v1 import TableV1

    version = int(version)
    if version == 0:
        return TableV0
    elif version == 1:
        return TableV1
    elif version == 2:
        if in_place:
            return InPlaceUpgradeSourceTableV2
        else:
            return UpgradeSourceTableV2

    return None


class UpgradeSourceTableV2(TableV2):
    """
    Variant of TableV2 that:
    - preserves all meta_items, even non-standard ones.
    - preserves attachments
    - upgrades dataset/metadata.json to metadata.xml
    """

    def get_meta_item(self, name, missing_ok=True):
        # Remove metadata/dataset.json
        if name == "metadata/dataset.json":
            return None

        result = super().get_meta_item(name, missing_ok=missing_ok)

        # Add metadata.xml:
        if result is None and name == "metadata.xml":
            metadata_json = super().get_meta_item("metadata/dataset.json")
            if metadata_json:
                metadata_xml = [
                    m for m in metadata_json.values() if "text/xml" in m.keys()
                ]
                if metadata_xml:
                    return next(iter(metadata_xml))["text/xml"]
        return result

    def attachment_items(self):
        attachments = [obj for obj in self.tree if obj.type_str == "blob"]
        for attachment in attachments:
            yield attachment.name, attachment.data


class InPlaceUpgradeSourceTableV2(UpgradeSourceTableV2):
    @property
    def feature_blobs_already_written(self):
        return True

    def feature_iter_with_reused_blobs(self, new_dataset, feature_ids=None):
        if feature_ids is None:
            for blob in self.feature_blobs():
                pk_values = self.decode_path_to_pks(blob.name)
                new_path = new_dataset.encode_pks_to_path(pk_values, schema=self.schema)
                yield new_path, blob.id.hex
        else:
            for pk_values in feature_ids:
                old_path = self.encode_pks_to_path(pk_values, relative=True)
                try:
                    blob = self.inner_tree / old_path
                    new_path = new_dataset.encode_pks_to_path(
                        pk_values, schema=self.schema
                    )
                    yield new_path, blob.id.hex
                except KeyError:
                    continue  # Missing / deleted blobs are just skipped


class ForceLatestVersionRepo(KartRepo):
    """
    A repo that always claims to be the latest version, regardless of its contents or config.
    Used for upgrading in-place.
    """

    @property
    def table_dataset_version(self):
        return DEFAULT_NEW_REPO_VERSION


@click.command(cls=KartCommand)
@click.pass_context
@click.option(
    "--in-place",
    is_flag=True,
    default=False,
    hidden=True,
    help="Irreversibly upgrade a repo in place.",
)
@click.argument("source", type=click.Path(exists=True, file_okay=False), required=True)
@click.argument("dest", type=click.Path(writable=True), required=True)
def upgrade(ctx, source, dest, in_place):
    """
    Upgrade a repository for an earlier version of Kart to be compatible with the latest version.
    The current repository structure of Kart is known as Datasets V2, which is used from kart/Kart 0.5 onwards.

    Usage:
    kart upgrade SOURCE DEST
    """
    source = Path(source)
    dest = Path(dest)

    if in_place:
        dest = source

    if not in_place and dest.exists() and any(dest.iterdir()):
        raise InvalidOperation(f'"{dest}" isn\'t empty', param_hint="DEST")

    try:
        source_repo = KartRepo(source)
    except NotFound:
        raise click.BadParameter(
            f"'{source}': not an existing Kart repository", param_hint="SOURCE"
        )

    source_version = source_repo.table_dataset_version
    if source_version == DEFAULT_NEW_REPO_VERSION:
        raise InvalidOperation(
            f"Cannot upgrade: source repository is already at latest known version (Datasets V{source_version})"
        )

    if source_version > DEFAULT_NEW_REPO_VERSION:
        # Repo is too advanced for this version of Kart to understand, we can't upgrade it.
        # This prints a good error messsage explaining the whole situation.
        source_repo.ensure_supported_version()

    source_dataset_class = dataset_class_for_legacy_version(source_version, in_place)

    if not source_dataset_class:
        raise InvalidOperation(
            f"Unrecognised source repository version: {source_version}"
        )

    # action!
    if in_place:
        dest_repo = ForceLatestVersionRepo(dest)
    else:
        click.secho(f"Initialising {dest} ...", bold=True)
        dest.mkdir()
        dest_repo = KartRepo.init_repository(
            dest, wc_location=None, bare=source_repo.is_bare
        )

    # walk _all_ references
    source_walker = source_repo.walk(
        None, pygit2.GIT_SORT_TOPOLOGICAL | pygit2.GIT_SORT_REVERSE
    )
    for ref in source_repo.listall_reference_objects():
        source_walker.push(ref.resolve().target)

    commit_map = {}

    click.secho("\nWriting new commits ...", bold=True)
    i = -1
    for i, source_commit in enumerate(source_walker):
        dest_parents = []
        for parent_id in source_commit.parent_ids:
            try:
                dest_parents.append(commit_map[parent_id.hex])
            except KeyError:
                raise ValueError(
                    f"Commit {i} ({source_commit.id}): Haven't seen parent ({parent_id})"
                )

        _upgrade_commit(
            ctx,
            i,
            source_repo,
            source_commit,
            source_dataset_class,
            dest_parents,
            dest_repo,
            commit_map,
        )

    click.echo(f"{i+1} commits processed.")

    click.secho("\nUpdating references ...", bold=True)
    for ref in source_repo.listall_reference_objects():
        if ref.type == pygit2.GIT_REF_OID:
            # real references
            target = commit_map[ref.target.hex]
            dest_repo.references.create(ref.name, target, True)  # overwrite
            click.echo(f"  {ref.name} ({ref.target.hex[:8]} → {target[:8]})")

    for ref in source_repo.listall_reference_objects():
        if ref.type == pygit2.GIT_REF_SYMBOLIC:
            dest_repo.references.create(ref.name, ref.target)
            click.echo(f"  {ref.name} → {ref.target}")

    if i >= 0:
        if source_repo.head_is_detached:
            dest_repo.set_head(pygit2.Oid(hex=commit_map[source_repo.head.target.hex]))
        else:
            dest_repo.set_head(source_repo.head.name)

        click.secho("\nCompacting repository ...", bold=True)
        if in_place:
            # old reflogs will refer to old objects, which prevents them from getting gc'd.
            # so we clear out the old reflogs here.
            # this *does* mean you can't go back, hence the 'irreversible' in the --in-place help.
            dest_repo.invoke_git(
                "reflog", "expire", "--expire-unreachable=now", "--all"
            )

        dest_repo.gc("--prune=now")

    if source_repo.workingcopy_location:
        click.secho("\nCreating working copy ...", bold=True)
        subctx = click.Context(ctx.command, parent=ctx)
        subctx.ensure_object(context.Context)
        subctx.obj.user_repo_path = str(dest)
        subctx.invoke(checkout.create_workingcopy)

    if in_place:
        dest_repo.config[KartConfigKeys.KART_REPOSTRUCTURE_VERSION] = str(
            DEFAULT_NEW_REPO_VERSION
        )

    click.secho("\nUpgrade complete", fg="green", bold=True)


def _upgrade_commit(
    ctx,
    i,
    source_repo,
    source_commit,
    source_dataset_class,
    dest_parent_ids,
    dest_repo,
    commit_map,
):
    source_rs = RepoStructure(source_repo, source_commit)
    source_datasets = source_rs.datasets(force_dataset_class=source_dataset_class)
    dataset_count = len(source_datasets)

    s = source_commit
    author_time = f"{s.author.time} {minutes_to_tz_offset(s.author.offset)}"
    commit_time = f"{s.commit_time} {minutes_to_tz_offset(s.commit_time_offset)}"

    # We import the commit onto a temporary branch, and fix the branch heads later.
    # We choose a name that will never collide with a real branch so we can happily delete it later.
    upgrade_ref = f"refs/heads/kart-upgrade-{uuid.uuid4()}"
    header = (
        f"commit {upgrade_ref}\n"
        f"author {s.author.name} <{s.author.email}> {author_time}\n"
        f"committer {s.committer.name} <{s.committer.email}> {commit_time}\n"
        f"data {len(s.message.encode('utf8'))}\n{s.message}\n"
    )

    sole_dataset_diff = _find_sole_dataset_diff(
        source_repo, source_commit, source_datasets, source_dataset_class
    )

    if sole_dataset_diff:
        # Optimisation - we can use feature_ids if we are only importing one dataset at a time, and,
        # we have access to the parent commit.
        ds_path, ds_diff = sole_dataset_diff
        source_datasets = [source_datasets[ds_path]]
        replace_existing = ReplaceExisting.GIVEN
        from_id = commit_map[source_commit.parents[0].hex]
        from_commit = dest_repo[from_id]
        merge_ids = [p for p in dest_parent_ids if p != from_id]
        replace_ids = list(ds_diff.get("feature", {}).keys())
        feature_count = len(replace_ids)
    else:
        from_id = from_commit = None
        merge_ids = dest_parent_ids
        replace_existing = ReplaceExisting.ALL
        replace_ids = None
        feature_count = sum(s.feature_count for s in source_datasets)

    if from_id:
        header += f"from {from_id}\n"
    header += "".join(f"merge {p}\n" for p in merge_ids)

    try:
        fast_import_tables(
            dest_repo,
            source_datasets,
            replace_existing=replace_existing,
            from_commit=from_commit,
            replace_ids=replace_ids,
            verbosity=ctx.obj.verbosity,
            header=header,
            extra_cmd_args=["--force"],
        )
        dest_commit = dest_repo.references.get(upgrade_ref).target
    finally:
        # delete the extra branch ref we just created; we don't need/want it
        try:
            dest_repo.references.delete(upgrade_ref)
        except KeyError:
            pass  # Nothing to delete, probably due to some earlier failure.

    commit_map[source_commit.hex] = dest_commit.hex

    commit_time = datetime.fromtimestamp(source_commit.commit_time)
    click.echo(
        f"  {i}: {source_commit.hex[:8]} → {dest_commit.hex[:8]}"
        f" ({commit_time}; {source_commit.committer.name}; {dataset_count} datasets; {feature_count} rows)"
    )


def _find_sole_dataset_diff(
    source_repo, source_commit, source_datasets, source_dataset_class
):
    """
    Returns a tuple (dataset_path, dataset_diff) if there is only one dataset which is changed
    in source_commit (when comparing source_commit to its first parent). Otherwise returns None.
    """
    if not hasattr(source_dataset_class, "diff_feature"):
        # Earlier dataset versions are no longer full-featured so we can't diff them anymore,
        # so, we don't do this optimisation.
        return None

    parent_commit = source_commit.parents[0] if source_commit.parents else None
    if not parent_commit:
        # Initial commit - this optimisation won't help here anyway.
        return None

    parent_rs = RepoStructure(source_repo, parent_commit)
    parent_datasets = parent_rs.datasets(force_dataset_class=source_dataset_class)
    source_ds_paths = {ds.path for ds in source_datasets}
    parent_ds_paths = {ds.path for ds in parent_datasets}

    all_ds_paths = source_ds_paths | parent_ds_paths
    all_changed_ds_paths = [
        path
        for path in all_ds_paths
        if _is_path_changed(parent_commit, source_commit, path)
    ]
    if len(all_changed_ds_paths) != 1:
        return None

    from kart.diff_util import get_dataset_diff

    ds_path = all_changed_ds_paths[0]
    ds_diff = get_dataset_diff(ds_path, parent_datasets, source_datasets)
    return ds_path, ds_diff


def _is_path_changed(treeish_a, treeish_b, path):
    def lookup_path(tree, path):
        try:
            return tree / path
        except KeyError:
            return None

    tree_a = lookup_path(treeish_a.peel(pygit2.Tree), path)
    tree_b = lookup_path(treeish_b.peel(pygit2.Tree), path)
    return tree_a != tree_b
