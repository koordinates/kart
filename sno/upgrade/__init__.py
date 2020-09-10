import click
import pygit2

import subprocess
from datetime import datetime
from pathlib import Path


from . import upgrade_v0, upgrade_v1
from sno import checkout, context
from sno.exceptions import InvalidOperation
from sno.fast_import import fast_import_tables, ReplaceExisting
from sno.structure import RepositoryStructure
from sno.repository_version import get_repo_version, write_repo_version_config
from sno.timestamps import minutes_to_tz_offset


def dataset_class_for_version(version):
    from .upgrade_v0 import UpgradeDataset0
    from .upgrade_v1 import UpgradeDataset1

    version = int(version)
    if version == 0:
        return UpgradeDataset0
    elif version == 1:
        return UpgradeDataset1

    raise ValueError(
        f"No upgradeable Dataset implementation found for version={version}"
    )


@click.command()
@click.pass_context
@click.argument("source", type=click.Path(exists=True, file_okay=False), required=True)
@click.argument("dest", type=click.Path(exists=False, writable=True), required=True)
def upgrade(ctx, source, dest):
    """
    Upgrade a repository for an earlier version of Sno to be compatible with the latest version.
    The current repository structure of Sno is known as Datasets V2, which is used from Sno 0.5 onwards.
    """
    source = Path(source)
    dest = Path(dest)

    if dest.exists():
        raise click.BadParameter(f"'{dest}': already exists", param_hint="DEST")

    source_repo = pygit2.Repository(str(source))
    if not source_repo or not source_repo.is_bare:
        raise click.BadParameter(
            f"'{source}': not an existing repository", param_hint="SOURCE"
        )

    source_version = get_repo_version(source_repo)
    if source_version == 2:
        raise InvalidOperation(
            "Cannot upgrade: source repository is already at latest version (Datasets V2)"
        )

    if source_version not in (0, 1):
        raise InvalidOperation(
            "Unrecognised source repository version: {source_version}"
        )

    source_dataset_class = dataset_class_for_version(source_version)

    # action!
    click.secho(f"Initialising {dest} ...", bold=True)
    dest.mkdir()
    dest_repo = pygit2.init_repository(str(dest), bare=True)
    write_repo_version_config(dest_repo, 2)

    # walk _all_ references
    source_walker = source_repo.walk(
        source_repo.head.target, pygit2.GIT_SORT_TOPOLOGICAL | pygit2.GIT_SORT_REVERSE
    )
    for ref in source_repo.listall_reference_objects():
        source_walker.push(ref.resolve().target)

    commit_map = {}

    click.secho("\nWriting new commits ...", bold=True)
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
            i,
            source_repo,
            source_commit,
            source_version,
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

    if source_repo.head_is_detached:
        dest_repo.set_head(pygit2.Oid(hex=commit_map[source_repo.head.target.hex]))
    else:
        dest_repo.set_head(source_repo.head.name)

    click.secho("\nCompacting repository ...", bold=True)
    subprocess.check_call(["git", "-C", str(dest), "gc"])

    if "sno.workingcopy.path" in source_repo.config:
        click.secho("\nCreating working copy ...", bold=True)
        subctx = click.Context(ctx.command, parent=ctx)
        subctx.ensure_object(context.Context)
        subctx.obj.user_repo_path = str(dest)
        subctx.invoke(checkout.create_workingcopy)

    click.secho("\nUpgrade complete", fg="green", bold=True)


def _upgrade_commit(
    i,
    source_repo,
    source_commit,
    source_version,
    source_dataset_class,
    dest_parents,
    dest_repo,
    commit_map,
):

    sources = [
        ds
        for ds in RepositoryStructure(
            source_repo,
            commit=source_commit,
            version=source_version,
            dataset_class=source_dataset_class,
        )
    ]
    dataset_count = len(sources)
    feature_count = sum(s.feature_count for s in sources)

    s = source_commit
    author_time = f"{s.author.time} {minutes_to_tz_offset(s.author.offset)}"
    commit_time = f"{s.commit_time} {minutes_to_tz_offset(s.commit_time_offset)}"
    header = (
        # We import every commit onto refs/heads/master and fix the branch heads later.
        "commit refs/heads/master\n"
        f"author {s.author.name} <{s.author.email}> {author_time}\n"
        f"committer {s.committer.name} <{s.committer.email}> {commit_time}\n"
        f"data {len(s.message.encode('utf8'))}\n{s.message}\n"
    )
    header += "".join(f"merge {p}\n" for p in dest_parents)

    fast_import_tables(
        dest_repo,
        sources,
        replace_existing=ReplaceExisting.ALL,
        quiet=True,
        header=header,
        # We import every commit onto refs/heads/master, even though not all commits are related - this means
        # the master branch head will jump all over the place. git-fast-import only allows this with --force.
        extra_cmd_args=["--force"],
    )

    dest_commit = dest_repo.head.peel(pygit2.Commit)
    commit_map[source_commit.hex] = dest_commit.hex

    commit_time = datetime.fromtimestamp(source_commit.commit_time)
    click.echo(
        f"  {i}: {source_commit.hex[:8]} → {dest_commit.hex[:8]}"
        f" ({commit_time}; {source_commit.committer.name}; {dataset_count} datasets; {feature_count} rows)"
    )
