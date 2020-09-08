import logging
import subprocess
import time
import uuid
from enum import Enum, auto

import click
import pygit2

from . import git_util
from .exceptions import SubprocessError, InvalidOperation, NotFound, NO_CHANGES
from .import_source import ImportSource
from .structure import DatasetStructure, RepositoryStructure
from .repository_version import get_repo_version, extra_blobs_for_version
from .timestamps import minutes_to_tz_offset


L = logging.getLogger("sno.fast_import")


class ReplaceExisting(Enum):
    # Don't replace any existing datasets.
    # Imports will start from the existing HEAD state.
    DONT_REPLACE = auto()

    # Any datasets in the import will replace existing datasets with the same name.
    # Datasets not in the import will be untouched.
    GIVEN = auto()

    # All existing datasets will be replaced by the given datasets.
    ALL = auto()


def fast_import_tables(
    repo,
    sources,
    *,
    quiet=False,
    header=None,
    message=None,
    replace_existing=ReplaceExisting.DONT_REPLACE,
    allow_empty=False,
    limit=None,
    max_pack_size="2G",
    max_delta_depth=0,
    extra_cmd_args=(),
):
    """
    Imports all of the given sources as new datasets, and commit the result.

    repo - the sno repo to import into.
    sources - an iterable of ImportSource objects. Each source is to be imported to source.dest_path.
    quiet - if True, no progress information is printed to stdout.
    header - the commit-header to supply git-fast-import. Generated if not supplied - see generate_header.
    message - the commit-message used when generating the header. Generated if not supplied - see generate_message.
    replace_existing - See ReplaceExisting enum
    limit - maximum number of features to import per source.
    max_pack_size - maximum size of pack files. Affects performance.
    max_delta_depth - maximum depth of delta-compression chains. Affects performance.
    extra_cmd_args - any extra args for the git-fast-import command.
    """

    head_tree = (
        None
        if replace_existing == ReplaceExisting.ALL
        else git_util.get_head_tree(repo)
    )

    if not head_tree:
        # Starting from an effectively empty repo. Write the blobs needed for this repo version.
        repo_version = get_repo_version(repo)
        replace_existing = ReplaceExisting.ALL
        extra_blobs = extra_blobs_for_version(repo_version)
    else:
        # Starting from a repo with commits. Make sure we have a matching version.
        repo_version = get_repo_version(repo, head_tree)
        extra_blobs = ()

    ImportSource.check_valid(sources)
    if replace_existing == ReplaceExisting.DONT_REPLACE:
        for source in sources:
            if source.dest_path in head_tree:
                raise InvalidOperation(
                    f"Cannot import to {source.dest_path}/ - already exists in repository"
                )

    cmd = [
        "git",
        "fast-import",
        "--quiet",
        "--done",
        f"--max-pack-size={max_pack_size}",
        f"--depth={max_delta_depth}",
    ] + list(extra_cmd_args)

    if header is None:
        # import onto a temp branch. then reset the head branch afterwards.
        # this allows us to check the result before updating the orig branch.
        import_branch = f'refs/heads/{uuid.uuid4()}'

        # may be None, if head is detached
        orig_branch = git_util.get_head_branch(repo)
        header = generate_header(repo, sources, message, import_branch)
    else:
        import_branch = None
    orig_commit = git_util.get_head_commit(repo)

    if not quiet:
        click.echo("Starting git-fast-import...")

    p = subprocess.Popen(cmd, cwd=repo.path, stdin=subprocess.PIPE,)
    try:
        if replace_existing != ReplaceExisting.ALL:
            header += f"from {orig_commit.oid}\n"
        p.stdin.write(header.encode("utf8"))

        # Write any extra blobs supplied by the client or needed for this version.
        for i, blob_path in write_blobs_to_stream(p.stdin, extra_blobs):
            if replace_existing != ReplaceExisting.ALL and blob_path in head_tree:
                raise ValueError(f"{blob_path} already exists")

        for source in sources:
            replacing_dataset = None
            if replace_existing == ReplaceExisting.GIVEN:
                # Delete the existing dataset, before we re-import it.
                p.stdin.write(f"D {source.dest_path}\n".encode('utf8'))

                try:
                    replacing_dataset = RepositoryStructure(repo)[source.dest_path]
                except KeyError:
                    pass
                else:
                    # We just deleted the legends, but we still need them to reimport
                    # data efficiently. Copy them from the original dataset.
                    for x in write_blobs_to_stream(
                        p.stdin, replacing_dataset.iter_legend_blob_data()
                    ):
                        pass

            dataset = DatasetStructure.for_version(repo_version)(
                tree=None, path=source.dest_path
            )

            with source:
                if limit:
                    num_rows = min(limit, source.feature_count)
                    num_rows_text = f"{num_rows:,d} of {source.feature_count:,d}"
                else:
                    num_rows = source.feature_count
                    num_rows_text = f"{num_rows:,d}"

                if not quiet:
                    click.echo(
                        f"Importing {num_rows_text} features from {source} to {source.dest_path}/ ..."
                    )

                for x in write_blobs_to_stream(
                    p.stdin, dataset.import_iter_meta_blobs(repo, source)
                ):
                    pass

                # features
                t1 = time.monotonic()
                src_iterator = source.features()

                for i, blob_path in write_blobs_to_stream(
                    p.stdin,
                    dataset.import_iter_feature_blobs(
                        src_iterator, source, replacing_dataset=replacing_dataset
                    ),
                ):
                    if i and i % 100000 == 0 and not quiet:
                        click.echo(f"  {i:,d} features... @{time.monotonic()-t1:.1f}s")

                    if limit is not None and i == (limit - 1):
                        click.secho(f"  Stopping at {limit:,d} features", fg="yellow")
                        break
                t2 = time.monotonic()
                if not quiet:
                    click.echo(f"Added {num_rows:,d} Features to index in {t2-t1:.1f}s")
                    click.echo(
                        f"Overall rate: {(num_rows/(t2-t1 or 1E-3)):.0f} features/s)"
                    )

        p.stdin.write(b"\ndone\n")
    except BrokenPipeError:
        # if git-fast-import dies early, we get an EPIPE here
        # we'll deal with it below
        pass
    else:
        p.stdin.close()
    p.wait()
    if p.returncode != 0:
        raise SubprocessError(
            f"git-fast-import error! {p.returncode}", exit_code=p.returncode
        )
    t3 = time.monotonic()
    if not quiet:
        click.echo(f"Closed in {(t3-t2):.0f}s")

    if import_branch is not None:
        # we created a temp branch for the import above.
        try:
            if head_tree and not allow_empty:
                if repo.revparse_single(import_branch).peel(pygit2.Tree) == head_tree:
                    raise NotFound("No changes to commit", exit_code=NO_CHANGES)
            latest_commit_oid = repo.references[import_branch].peel(pygit2.Commit).oid
            if orig_branch:
                # reset the original branch head to the import branch, so it gets the new commits
                if head_tree:
                    # repo was non-empty before this, and head was not detached.
                    # so orig_branch exists already.
                    # we have to delete and re-create it at the new commit.
                    repo.references.delete(orig_branch)
                repo.references.create(orig_branch, latest_commit_oid)
            else:
                # head was detached before this. just update head to the new commit,
                # so it's still detached.
                repo.set_head(latest_commit_oid)
        finally:
            # remove the import branch
            repo.references.delete(import_branch)


def write_blobs_to_stream(stream, blobs):
    for i, (blob_path, blob_data) in enumerate(blobs):
        stream.write(
            f"M 644 inline {blob_path}\ndata {len(blob_data)}\n".encode("utf8")
        )
        stream.write(blob_data)
        stream.write(b"\n")
        yield i, blob_path


def generate_header(repo, sources, message, branch):
    if message is None:
        message = generate_message(sources)

    author = git_util.author_signature(repo)
    committer = git_util.committer_signature(repo)
    return (
        f"commit {branch}\n"
        f"author {author.name} <{author.email}> {author.time} {minutes_to_tz_offset(author.offset)}\n"
        f"committer {committer.name} <{committer.email}> {committer.time} {minutes_to_tz_offset(committer.offset)}\n"
        f"data {len(message.encode('utf8'))}\n{message}\n"
    )


def generate_message(sources):
    first_source = next(iter(sources))
    return first_source.aggregate_import_source_desc(sources)

    """
    if len(sources) == 1:
        for path, source in sources.items():
            message = f"Import from {Path(source.source).name} to {path}/"
    else:
        source = next(iter(sources.values()))
        message = f"Import {len(sources)} datasets from '{Path(source.source).name}':\n"
        for path, source in sources.items():
            if path == source.table:
                message += f"\n* {path}/"
            else:
                message += f"\n* {path} (from {source.table})"
    return message
    """
