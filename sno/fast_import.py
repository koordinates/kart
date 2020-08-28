import logging
import subprocess
import time

import click
import pygit2

from .exceptions import SubprocessError, InvalidOperation
from .import_source import ImportSource
from .structure import DatasetStructure
from .repository_version import get_repo_version, extra_blobs_for_version


L = logging.getLogger("sno.fast_import")


def fast_import_tables(
    repo,
    sources,
    *,
    incremental=True,
    quiet=False,
    header=None,
    message=None,
    limit=None,
    max_pack_size="2G",
    max_delta_depth=0,
    extra_blobs=(),
    extra_cmd_args=(),
):
    """
    Imports all of the given sources as new datasets, and commit the result.

    repo - the sno repo to import into.
    sources - an iterable of ImportSource objects. Each source is to be imported to source.dest_path.
    incremental - True if the resulting commit should contain everything already at HEAD plus the new datasets,
        False if the resulting commit should only contain the new datasets.
    quiet - if True, no progress information is printed to stdout.
    header - the commit-header to supply git-fast-import. Generated if not supplied - see generate_header.
    message - the commit-message used when generating the header. Generated if not supplied - see generate_message.
    limit - maximum number of features to import per source.
    max_pack_size - maximum size of pack files. Affects performance.
    max_delta_depth - maximum depth of delta-compression chains. Affects performance.
    extra_blobs - any extra blobs that also need to be written in the same commit.
    extra_cmd_args - any extra args for the git-fast-import command.
    """

    head_tree = get_head_tree(repo) if incremental else None

    if not head_tree:
        # Starting from an effectively empty repo. Write the blobs needed for this repo version.
        repo_version = get_repo_version(repo)
        incremental = False
        extra_blobs = list(extra_blobs) + extra_blobs_for_version(repo_version)
    else:
        # Starting from a repo with commits. Make sure we have a matching version.
        repo_version = get_repo_version(repo, head_tree)

    ImportSource.check_valid(sources)
    for source in sources:
        if incremental and source.dest_path in head_tree:
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
        header = generate_header(repo, sources, message)
        cmd.append("--date-format=now")

    if not quiet:
        click.echo("Starting git-fast-import...")

    p = subprocess.Popen(cmd, cwd=repo.path, stdin=subprocess.PIPE,)
    try:
        if incremental:
            header += f"from {get_head_branch(repo)}^0\n"
        p.stdin.write(header.encode("utf8"))

        # Write an extra blobs supplied by the client or needed for this version.
        for i, blob_path in write_blobs_to_stream(p.stdin, extra_blobs):
            if incremental and blob_path in head_tree:
                raise ValueError(f"{blob_path} already exists")

        for source in sources:
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

                for i, blob_path in write_blobs_to_stream(
                    p.stdin, dataset.import_iter_meta_blobs(repo, source)
                ):
                    pass

                # features
                t1 = time.monotonic()
                src_iterator = source.features()

                for i, blob_path in write_blobs_to_stream(
                    p.stdin, dataset.import_iter_feature_blobs(src_iterator, source)
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


def get_head_tree(repo):
    """Returns the tree at the current repo HEAD."""
    if repo.is_empty:
        return None
    try:
        return repo.head.peel(pygit2.Tree)
    except pygit2.GitError:
        # This happens when the repo is not empty, but the current HEAD has no commits.
        return None


def get_head_branch(repo):
    """Returns the branch that HEAD is currently on."""
    if repo.head_is_detached:
        raise InvalidOperation(
            'Cannot fast-import when in "detached HEAD" state - ie, when not on a branch'
        )
    return repo.head.name if not repo.is_empty else "refs/heads/master"


def write_blobs_to_stream(stream, blobs):
    for i, (blob_path, blob_data) in enumerate(blobs):
        stream.write(
            f"M 644 inline {blob_path}\ndata {len(blob_data)}\n".encode("utf8")
        )
        stream.write(blob_data)
        stream.write(b"\n")
        yield i, blob_path


def generate_header(repo, sources, message):
    if message is None:
        message = generate_message(sources)

    user = repo.default_signature
    return (
        f"commit {get_head_branch(repo)}\n"
        f"committer {user.name} <{user.email}> now\n"
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
