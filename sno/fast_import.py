import logging
import subprocess
import time
import uuid
from enum import Enum, auto

import click
import pygit2

from .cli_util import tool_environment
from .exceptions import SubprocessError, InvalidOperation, NotFound, NO_CHANGES
from .import_source import ImportSource
from .repo_version import (
    extra_blobs_for_version,
    SUPPORTED_REPO_VERSION,
    SUPPORTED_DATASET_CLASS,
)
from .timestamps import minutes_to_tz_offset
from .pk_generation import PkGeneratingImportSource


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
    verbosity=1,
    header=None,
    message=None,
    replace_existing=ReplaceExisting.DONT_REPLACE,
    replace_ids=None,
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
    verbosity - integer:
        0: no progress information is printed to stdout.
        1: basic status information
        2: full output of `git-fast-import --stats ...`
    header - the commit-header to supply git-fast-import. Generated if not supplied - see generate_header.
    message - the commit-message used when generating the header. Generated if not supplied - see generate_message.
    replace_existing - See ReplaceExisting enum
    replace_ids - list of PK values to replace, or None
    limit - maximum number of features to import per source.
    max_pack_size - maximum size of pack files. Affects performance.
    max_delta_depth - maximum depth of delta-compression chains. Affects performance.
    extra_cmd_args - any extra args for the git-fast-import command.
    """

    # The tree this repo was at before this function was called.
    # May be None (repo is empty)
    orig_tree = repo.head_tree

    # The tree we look at for considering what datasets already exist
    # depends what we want to replace.
    if replace_existing == ReplaceExisting.ALL:
        starting_tree = None
    else:
        starting_tree = repo.head_tree

    if not starting_tree:
        replace_existing = ReplaceExisting.ALL

    assert repo.version == SUPPORTED_REPO_VERSION
    extra_blobs = (
        extra_blobs_for_version(SUPPORTED_REPO_VERSION) if not starting_tree else []
    )
    dataset_class = SUPPORTED_DATASET_CLASS

    ImportSource.check_valid(sources)

    if replace_existing == ReplaceExisting.DONT_REPLACE:
        for source in sources:
            if source.dest_path in starting_tree:
                raise InvalidOperation(
                    f"Cannot import to {source.dest_path}/ - already exists in repository"
                )
        assert replace_ids is None

    # Add primary keys if needed.
    sources = PkGeneratingImportSource.wrap_sources_if_needed(sources, repo)

    cmd = [
        "git",
        "fast-import",
        "--done",
        f"--max-pack-size={max_pack_size}",
        f"--depth={max_delta_depth}",
    ]
    if verbosity < 2:
        cmd.append("--quiet")

    if header is None:
        # import onto a temp branch. then reset the head branch afterwards.
        # this allows us to check the result before updating the orig branch.
        import_branch = f"refs/heads/{uuid.uuid4()}"

        # may be None, if head is detached
        orig_branch = repo.head_branch
        header = generate_header(repo, sources, message, import_branch)
    else:
        import_branch = None
    orig_commit = repo.head_commit

    if verbosity >= 1:
        click.echo("Starting git-fast-import...")

    p = subprocess.Popen(
        [*cmd, *extra_cmd_args],
        cwd=repo.path,
        stdin=subprocess.PIPE,
        env=tool_environment(),
    )
    try:
        if replace_existing != ReplaceExisting.ALL:
            header += f"from {orig_commit.oid}\n"
        p.stdin.write(header.encode("utf8"))

        # Write the extra blob that records the repo's version:
        for i, blob_path in write_blobs_to_stream(p.stdin, extra_blobs):
            if replace_existing != ReplaceExisting.ALL and blob_path in starting_tree:
                raise ValueError(f"{blob_path} already exists")

        for source in sources:
            replacing_dataset = None
            if replace_existing == ReplaceExisting.GIVEN:
                try:
                    replacing_dataset = repo.datasets()[source.dest_path]
                except KeyError:
                    # no such dataset; no problem
                    replacing_dataset = None

                if replacing_dataset is not None:
                    if replace_ids is None:
                        # Delete the existing dataset, before we re-import it.
                        p.stdin.write(f"D {source.dest_path}\n".encode("utf8"))
                    else:
                        # delete and reimport meta/
                        # we also delete the specified features, but we do it further down
                        # so that we don't have to iterate the IDs more than once.
                        p.stdin.write(
                            f"D {source.dest_path}/.sno-dataset/meta\n".encode("utf8")
                        )

                    # We just deleted the legends, but we still need them to reimport
                    # data efficiently. Copy them from the original dataset.
                    for x in write_blobs_to_stream(
                        p.stdin, replacing_dataset.iter_legend_blob_data()
                    ):
                        pass

            dataset = dataset_class(tree=None, path=source.dest_path)

            with source:
                if limit:
                    num_rows = min(limit, source.feature_count)
                    num_rows_text = f"{num_rows:,d} of {source.feature_count:,d}"
                else:
                    num_rows = source.feature_count
                    num_rows_text = f"{num_rows:,d}"

                if verbosity >= 1:
                    click.echo(
                        f"Importing {num_rows_text} features from {source} to {source.dest_path}/ ..."
                    )

                # Features
                t1 = time.monotonic()
                if replace_ids is not None:

                    # As we iterate over IDs, also delete them from the dataset.
                    # This means we don't have to load the whole list into memory.
                    def _ids():
                        for pk in replace_ids:
                            pk = source.schema.sanitise_pks(pk)
                            path = dataset.encode_pks_to_path(pk)
                            p.stdin.write(f"D {path}\n".encode("utf8"))
                            yield pk

                    src_iterator = source.get_features(_ids(), ignore_missing=True)
                else:
                    src_iterator = source.features()

                progress_every = None
                if verbosity >= 1:
                    progress_every = max(100, 100_000 // (10 ** (verbosity - 1)))

                for i, blob_path in write_blobs_to_stream(
                    p.stdin,
                    dataset.import_iter_feature_blobs(
                        src_iterator, source, replacing_dataset=replacing_dataset
                    ),
                ):
                    if i and progress_every and i % progress_every == 0:
                        click.echo(f"  {i:,d} features... @{time.monotonic()-t1:.1f}s")

                    if limit is not None and i == (limit - 1):
                        click.secho(f"  Stopping at {limit:,d} features", fg="yellow")
                        break
                t2 = time.monotonic()
                if verbosity >= 1:
                    click.echo(f"Added {num_rows:,d} Features to index in {t2-t1:.1f}s")
                    click.echo(
                        f"Overall rate: {(num_rows/(t2-t1 or 1E-3)):.0f} features/s)"
                    )

                # Meta items - written second as certain importers generate extra metadata as they import features.
                for x in write_blobs_to_stream(
                    p.stdin, dataset.import_iter_meta_blobs(repo, source)
                ):
                    pass

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
    if verbosity >= 1:
        click.echo(f"Closed in {(t3-t2):.0f}s")

    if import_branch is not None:
        # we created a temp branch for the import above.
        try:
            if orig_tree and not allow_empty:
                if repo.revparse_single(import_branch).peel(pygit2.Tree) == orig_tree:
                    raise NotFound("No changes to commit", exit_code=NO_CHANGES)
            latest_commit_oid = repo.references[import_branch].peel(pygit2.Commit).oid
            if orig_branch:
                # reset the original branch head to the import branch, so it gets the new commits
                if orig_tree:
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

    author = repo.author_signature()
    committer = repo.committer_signature()
    return (
        f"commit {branch}\n"
        f"author {author.name} <{author.email}> {author.time} {minutes_to_tz_offset(author.offset)}\n"
        f"committer {committer.name} <{committer.email}> {committer.time} {minutes_to_tz_offset(committer.offset)}\n"
        f"data {len(message.encode('utf8'))}\n{message}\n"
    )


def generate_message(sources):
    first_source = next(iter(sources))
    return first_source.aggregate_import_source_desc(sources)
