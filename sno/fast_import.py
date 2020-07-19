import logging
import subprocess
import time
from pathlib import Path

import click
import pygit2

from .exceptions import SubprocessError
from .structure import DatasetStructure
from .structure_version import get_structure_version, extra_blobs_for_version


L = logging.getLogger("sno.fast_import")


def fast_import_tables(
    repo,
    sources,
    *,
    structure_version,
    incremental=True,
    quiet=False,
    header=None,
    message=None,
    limit=None,
    max_pack_size="2G",
    max_delta_depth=0,
    extra_blobs=(),
):
    structure_version = int(structure_version)
    head_tree = get_head_tree(repo) if incremental else None

    if not head_tree:
        # Starting from an effectively empty repo. Write the blobs needed for this repo version.
        incremental = False
        extra_blobs = list(extra_blobs) + extra_blobs_for_version(structure_version)
    else:
        # Starting from a repo with commits. Make sure we have a matching version.
        repo_version = get_structure_version(repo, head_tree)
        if repo_version != structure_version:
            raise ValueError(
                f"Version mismatch - repo is version {repo_version}, trying to import as {structure_version}"
            )

    for path, source in sources.items():
        if not source.table:
            raise ValueError("No table specified")

        if incremental and path in head_tree:
            raise ValueError(f"{path}/ already exists")

    cmd = [
        "git",
        "fast-import",
        "--quiet",
        "--done",
        f"--max-pack-size={max_pack_size}",
        f"--depth={max_delta_depth}",
    ]

    if header is None:
        header = generate_header(repo, sources, message)
        cmd.append("--date-format=now")

    if not quiet:
        click.echo("Starting git-fast-import...")

    p = subprocess.Popen(cmd, cwd=repo.path, stdin=subprocess.PIPE,)
    try:
        p.stdin.write(header.encode("utf8"))

        if incremental:
            # Start with the existing branch contents.
            # FIXME - this should probably not always be master.
            p.stdin.write(b"from refs/heads/master^0\n")

        # Write an extra blobs supplied by the client or needed for this version.
        for i, blob_path in write_blobs_to_stream(p.stdin, extra_blobs):
            if incremental and blob_path in head_tree:
                raise ValueError(f"{blob_path} already exists")

        for path, source in sources.items():
            dataset = DatasetStructure.for_version(structure_version)(
                tree=None, path=path
            )

            with source:
                if limit:
                    num_rows = min(limit, source.row_count)
                    click.echo(
                        f"Importing {num_rows:,d} of {source.row_count:,d} features from {source} to {path}/ ..."
                    )
                else:
                    num_rows = source.row_count
                    if not quiet:
                        click.echo(
                            f"Importing {num_rows:,d} features from {source} to {path}/ ..."
                        )

                for i, blob_path in write_blobs_to_stream(
                    p.stdin, dataset.import_iter_meta_blobs(repo, source)
                ):
                    pass

                # features
                t1 = time.monotonic()
                src_iterator = source.iter_features()

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
        "commit refs/heads/master\n"
        f"committer {user.name} <{user.email}> now\n"
        f"data {len(message.encode('utf8'))}\n{message}\n"
    )


def generate_message(sources):
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
