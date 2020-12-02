import click
import pygit2

from datetime import datetime
from pathlib import Path


from sno import checkout, context
from sno.exceptions import InvalidOperation, NotFound
from sno.fast_import import fast_import_tables, ReplaceExisting
from sno.sno_repo import SnoRepo
from sno.structure import RepositoryStructure
from sno.repository_version import get_repo_version
from sno.timestamps import minutes_to_tz_offset


UPGRADED_REPO_VERSION = 2


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

    Usage:
    sno upgrade SOURCE DEST
    """
    source = Path(source)
    dest = Path(dest)

    if dest.exists():
        raise click.BadParameter(f"'{dest}': already exists", param_hint="DEST")

    try:
        source_repo = SnoRepo(source)
    except NotFound:
        raise click.BadParameter(
            f"'{source}': not an existing sno repository", param_hint="SOURCE"
        )

    source_version = get_repo_version(source_repo, allow_legacy_versions=True)
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
    dest_repo = SnoRepo.init_repository(
        dest, UPGRADED_REPO_VERSION, wc_path=None, bare=True
    )

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
    dest_repo.gc()

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

    dest_commit = dest_repo.head_commit
    commit_map[source_commit.hex] = dest_commit.hex

    commit_time = datetime.fromtimestamp(source_commit.commit_time)
    click.echo(
        f"  {i}: {source_commit.hex[:8]} → {dest_commit.hex[:8]}"
        f" ({commit_time}; {source_commit.committer.name}; {dataset_count} datasets; {feature_count} rows)"
    )


@click.command()
@click.argument("source", type=click.Path(exists=True, file_okay=False), required=True)
def upgrade_to_tidy(source):
    """
    Upgrade in-place a sno repository that is bare-style to be tidy-style. See sno_repo.py
    To be used on sno-repo's that are not actually intended to be bare, but are "bare-style"
    because they were created using Sno 0.5 or less, and that was all Sno supported.
    Doesn't upgrade the repository version, or change the contents at all.

    Usage:
    sno upgrade-to-tidy SOURCE
    """
    source = Path(source).resolve()

    try:
        source_repo = SnoRepo(source)
    except NotFound:
        raise click.BadParameter(
            f"'{source}': not an existing sno repository", param_hint="SOURCE"
        )

    if source_repo.is_tidy_style_sno_repo():
        raise click.InvalidOperation(
            "Cannot upgrade in-place - source repo is already tidy-style"
        )

    repo_version = source_repo.version
    wc_path = source_repo.workingcopy_path
    is_bare = source_repo.is_bare

    source_repo.free()
    del source_repo

    dot_sno_path = source / ".sno"
    if dot_sno_path.exists() and any(dot_sno_path.iterdir()):
        raise click.InvalidOperation(".sno already exists and is not empty")
    elif not dot_sno_path.exists():
        dot_sno_path.mkdir()

    for child in source.iterdir():
        if child == dot_sno_path:
            continue
        if ".gpkg" in child.name:
            continue
        child.rename(dot_sno_path / child.name)

    tidy_repo = SnoRepo(dot_sno_path)
    tidy_repo.lock_git_index()
    tidy_repo.config["core.bare"] = False
    tidy_repo.config["sno.workingcopy.bare"] = False
    tidy_repo.write_config(repo_version, wc_path, is_bare)
    tidy_repo.activate()

    click.secho("In-place upgrade complete: repo is now tidy", fg="green", bold=True)
