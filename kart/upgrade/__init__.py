import click
import pygit2

from datetime import datetime
from pathlib import Path


from kart import checkout, context
from kart.exceptions import InvalidOperation, NotFound
from kart.fast_import import fast_import_tables, ReplaceExisting
from kart.repo import SnoRepo
from kart.structure import RepoStructure
from kart.timestamps import minutes_to_tz_offset
from kart.repo_version import SUPPORTED_REPO_VERSION


def dataset_class_for_legacy_version(version):
    from .upgrade_v0 import Dataset0
    from .upgrade_v1 import Dataset1

    version = int(version)
    if version == 0:
        return Dataset0
    elif version == 1:
        return Dataset1

    return None


@click.command()
@click.pass_context
@click.argument("source", type=click.Path(exists=True, file_okay=False), required=True)
@click.argument("dest", type=click.Path(exists=False, writable=True), required=True)
def upgrade(ctx, source, dest):
    """
    Upgrade a repository for an earlier version of Kart to be compatible with the latest version.
    The current repository structure of Kart is known as Datasets V2, which is used from kart/Kart 0.5 onwards.

    Usage:
    kart upgrade SOURCE DEST
    """
    source = Path(source)
    dest = Path(dest)

    if dest.exists():
        raise click.BadParameter(f"'{dest}': already exists", param_hint="DEST")

    try:
        source_repo = SnoRepo(source)
    except NotFound:
        raise click.BadParameter(
            f"'{source}': not an existing Kart repository", param_hint="SOURCE"
        )

    source_version = source_repo.version
    if source_version == SUPPORTED_REPO_VERSION:
        raise InvalidOperation(
            "Cannot upgrade: source repository is already at latest known version (Datasets V2)"
        )

    if source_version > SUPPORTED_REPO_VERSION:
        # Repo is too advanced for this version of Kart to understand, we can't upgrade it.
        # This prints a good error messsage explaining the whole situation.
        source_repo.ensure_supported_version()

    source_dataset_class = dataset_class_for_legacy_version(source_version)

    if not source_dataset_class:
        raise InvalidOperation(
            f"Unrecognised source repository version: {source_version}"
        )

    # action!
    click.secho(f"Initialising {dest} ...", bold=True)
    dest.mkdir()
    dest_repo = SnoRepo.init_repository(dest, wc_location=None, bare=True)

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

    if source_repo.workingcopy_location:
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
    rs = RepoStructure(
        source_repo,
        source_commit,
        dataset_class=source_dataset_class,
    )
    source_datasets = list(rs.datasets)
    dataset_count = len(source_datasets)
    feature_count = sum(s.feature_count for s in source_datasets)

    s = source_commit
    author_time = f"{s.author.time} {minutes_to_tz_offset(s.author.offset)}"
    commit_time = f"{s.commit_time} {minutes_to_tz_offset(s.commit_time_offset)}"
    header = (
        # We import every commit onto refs/heads/main and fix the branch heads later.
        "commit refs/heads/main\n"
        f"author {s.author.name} <{s.author.email}> {author_time}\n"
        f"committer {s.committer.name} <{s.committer.email}> {commit_time}\n"
        f"data {len(s.message.encode('utf8'))}\n{s.message}\n"
    )
    header += "".join(f"merge {p}\n" for p in dest_parents)

    fast_import_tables(
        dest_repo,
        source_datasets,
        replace_existing=ReplaceExisting.ALL,
        verbosity=0,
        header=header,
        # We import every commit onto refs/heads/main, even though not all commits are related - this means
        # the main branch head will jump all over the place. git-fast-import only allows this with --force.
        extra_cmd_args=["--force"],
        num_processes=1,
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
    Upgrade in-place a Kart repository that is bare-style to be tidy-style. See.repo.py
    To be used on Kart repos that are not actually intended to be bare, but are "bare-style"
    because they were created using Sno/Kart 0.5 or less, and that was all Kart supported.
    Doesn't upgrade the repository version, or change the contents at all.

    Usage:
    kart upgrade-to-tidy SOURCE
    """
    source = Path(source).resolve()

    try:
        source_repo = SnoRepo(source)
    except NotFound:
        raise click.BadParameter(
            f"'{source}': not an existing Kart repository", param_hint="SOURCE"
        )

    source_repo.ensure_supported_version()
    if source_repo.is_tidy_style:
        raise InvalidOperation(
            "Cannot upgrade in-place - source repo is already tidy-style"
        )

    wc_loc = source_repo.workingcopy_location
    is_bare = source_repo.is_bare

    source_repo.free()
    del source_repo

    dot_sno_path = source / ".sno"
    if dot_sno_path.exists() and any(dot_sno_path.iterdir()):
        raise InvalidOperation(".sno already exists and is not empty")
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
    tidy_repo.write_config(wc_loc, is_bare)
    tidy_repo.activate()

    click.secho("In-place upgrade complete: repo is now tidy", fg="green", bold=True)


@click.command()
@click.pass_context
@click.argument("source", type=click.Path(exists=True, file_okay=False), required=True)
def upgrade_to_kart(ctx, source):
    """
    Upgrade in-place a Sno repository to be a Kart repository.
    This only affects objects that are not version controlled, but are just part of the local context -
    so, local branches that are tracking remote branches will not be affected. The local changes
    that you can push before upgrading, are the same as the local changes you have after upgrading.

    Changes the following:
     - config variables:
       * sno.repository.version -> kart.repostructure.version
       * sno.workingcopy.path -> kart.workingcopy.location
     - hidden directory name from .sno to .kart (if repo is tidy-style)
     - rewrites .sno/index / .kart/index to contain "kart" extension
     - SNO_README.txt -> KART_README.txt
     - recreates the working copy, if one exists:
       * _sno_state table (or similar) -> _kart_state
       * _sno_track table (or similar) -> _kart_track
       * triggers related to _sno_track also have new names

    Usage:
    kart upgrade-to-kart SOURCE
    """
    source = Path(source).resolve()

    try:
        source_repo = SnoRepo(source)
    except NotFound:
        raise click.BadParameter(
            f"'{source}': not an existing Sno repository", param_hint="SOURCE"
        )

    source_repo.ensure_supported_version()
    if source_repo.branding == "kart":
        raise InvalidOperation(
            "Cannot upgrade in-place - source repo is already a Kart repo"
        )

    working_copy = source_repo.working_copy
    is_bare_style = source_repo.is_bare_style

    if working_copy:
        working_copy.check_not_dirty()

    if not is_bare_style:
        assert source == source_repo.workdir_path
        dot_sno_path = source / ".sno"
        assert dot_sno_path.is_dir()
        assert source_repo.gitdir_path == dot_sno_path

        dot_kart_path = source / ".kart"
        if dot_kart_path.exists():
            raise InvalidOperation(".kart already exists")

    from kart.repo import KartConfigKeys, LOCKED_GIT_INDEX_CONTENTS

    # Config variables:
    click.echo("Moving config variables")
    config = source_repo.config
    assert KartConfigKeys.SNO_REPOSITORY_VERSION in config
    config[KartConfigKeys.KART_REPOSTRUCTURE_VERSION] = config[
        KartConfigKeys.SNO_REPOSITORY_VERSION
    ]
    del config[KartConfigKeys.SNO_REPOSITORY_VERSION]

    if KartConfigKeys.SNO_WORKINGCOPY_PATH in config:
        config[KartConfigKeys.KART_WORKINGCOPY_LOCATION] = config[
            KartConfigKeys.SNO_WORKINGCOPY_PATH
        ]
        del config[KartConfigKeys.SNO_WORKINGCOPY_PATH]

    source_repo.free()
    del source_repo

    # Directory name, .git file, index file:
    if not is_bare_style:
        click.echo("Moving .sno to .kart")
        dot_sno_path.rename(dot_kart_path)
        (source / ".git").write_text("gitdir: .kart\n", encoding="utf-8")
        (dot_kart_path / "index").write_bytes(LOCKED_GIT_INDEX_CONTENTS["kart"])

    # README file:
    if (source / "SNO_README.txt").exists():
        (source / "SNO_README.txt").unlink()
    readme_text = SnoRepo.get_readme_text(is_bare_style, "kart")
    (source / "KART_README.txt").write_text(readme_text)

    # Working copy:
    if working_copy:
        subctx = click.Context(ctx.command, parent=ctx)
        subctx.ensure_object(context.Context)
        subctx.obj.user_repo_path = str(source)
        subctx.invoke(checkout.create_workingcopy, delete_existing=True)

    click.secho(
        f"\nIn-place upgrade complete: Sno repo is now Kart repo", fg="green", bold=True
    )
