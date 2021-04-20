import logging
import subprocess
import time
import uuid
from contextlib import contextmanager, ExitStack
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
from .rich_tree_builder import RichTreeBuilder
from .structure import Datasets
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


class _CommitMissing(Exception):
    pass


def _safe_walk_repo(repo):
    """
    Contextmanager. Walk the repo log, yielding each commit.
    If a commit isn't present, raises _CommitMissing.
    Avoids catching any other KeyErrors raised by pygit2 or the contextmanager body
    """
    do_raise = False
    try:
        for commit in repo.walk(repo.head.target):
            try:
                yield commit
            except KeyError:
                # we only want to catch from the `repo.walk` call,
                # not from the contextmanager body
                do_raise = True
                raise
    except KeyError:
        if do_raise:
            raise
        raise _CommitMissing


def should_compare_imported_features_against_old_features(
    repo, source, replacing_dataset
):
    """
    Returns True iff we should compare feature blobs to the previous feature blobs
    when importing.

    This prevents repo bloat after columns are added or removed from the dataset,
    by only creating new blobs when the old blob cannot be upgraded to the new
    schema.
    """
    if replacing_dataset is None:
        return False
    old_schema = replacing_dataset.schema
    if old_schema != source.schema:
        types = replacing_dataset.schema.diff_type_counts(source.schema)
        if types["pk_updates"]:
            # when the PK changes, we won't be able to match old features to new features.
            # so not much point trying.
            return False
        elif types["inserts"] or types["deletes"]:
            # however, after column adds/deletes, we want to check features against
            # old features, to avoid unnecessarily duplicating 'identical' features.
            return True

    # Walk the log until we encounter a relevant schema change
    try:
        for commit in _safe_walk_repo(repo):
            datasets = repo.datasets(commit.oid)
            try:
                old_dataset = datasets[replacing_dataset.path]
            except KeyError:
                # no schema changes since this dataset was added.
                return False
            if old_dataset.schema != source.schema:
                # this revision had a schema change
                types = old_dataset.schema.diff_type_counts(source.schema)
                if types["pk_updates"]:
                    # if the schema change was a PK update, all features were rewritten in that
                    # revision, and since no schema changes have occurred since then, we don't
                    # have to check all features against old features.
                    return False
                elif types["inserts"] or types["deletes"]:
                    return True
    except _CommitMissing:
        # probably this was because we're in a shallow clone,
        # and the commit just isn't present.
        # Just run the feature blob comparison; worst case it's a bit slow.
        return True
    return False


@contextmanager
def _git_fast_import(repo, *args):
    p = subprocess.Popen(
        ["git", "fast-import", "--done", *args],
        cwd=repo.path,
        stdin=subprocess.PIPE,
        env=tool_environment(),
        bufsize=128 * 1024,
    )
    try:
        yield p
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


def fast_import_clear_trees(*, procs, replace_ids, replacing_dataset, source):
    """
    Clears out the appropriate trees in each of the fast_import processes,
    before importing any actual data over the top.
    """
    if replacing_dataset is None:
        # nothing to do
        return
    for i, proc in enumerate(procs):
        if replace_ids is None:
            # Delete the existing dataset, before we re-import it.
            proc.stdin.write(f"D {source.dest_path}\n".encode("utf8"))
        else:
            # delete and reimport meta/
            proc.stdin.write(f"D {source.dest_path}/.sno-dataset/meta\n".encode("utf8"))
            # delete all features not pertaining to this process.
            # we also delete the features that *do*, but we do it further down
            # so that we don't have to iterate the IDs more than once.
            for subtree in range(256):
                if subtree % len(procs) != i:
                    proc.stdin.write(
                        f"D {source.dest_path}/.sno-dataset/feature/{subtree:02x}\n".encode(
                            "utf8"
                        )
                    )

        # We just deleted the legends, but we still need them to reimport
        # data efficiently. Copy them from the original dataset.
        for x in write_blobs_to_stream(
            proc.stdin, replacing_dataset.iter_legend_blob_data()
        ):
            pass


def fast_import_tables(
    repo,
    sources,
    *,
    verbosity=1,
    num_processes=4,
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
    num_processes: how many import processes to run in parallel
    header - the commit-header to supply git-fast-import. Generated if not supplied - see generate_header.
    message - the commit-message used when generating the header. Generated if not supplied - see generate_message.
    replace_existing - See ReplaceExisting enum
    replace_ids - list of PK values to replace, or None
    limit - maximum number of features to import per source.
    max_pack_size - maximum size of pack files. Affects performance.
    max_delta_depth - maximum depth of delta-compression chains. Affects performance.
    extra_cmd_args - any extra args for the git-fast-import command.
    """

    MAX_PROCESSES = 64
    if num_processes < 1:
        num_processes = 1
    elif num_processes > MAX_PROCESSES:
        # this is almost certainly a mistake, but also:
        # we want to split 256 trees roughly evenly, and if we're trying to split them across
        # too many processes it won't be very even.
        raise ValueError(f"Can't import with more than {MAX_PROCESSES} processes")

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
        "--done",
        f"--max-pack-size={max_pack_size}",
        f"--depth={max_delta_depth}",
    ]
    if verbosity < 2:
        cmd.append("--quiet")

    orig_commit = repo.head_commit
    import_refs = []

    if verbosity >= 1:
        click.echo("Starting git-fast-import...")

    try:
        with ExitStack() as stack:
            procs = []

            # PARALLEL IMPORTING
            # To do an import in parallel:
            #   * we only have one sno process, and one connection to the source.
            #   * we have multiple git-fast-import backend processes
            #   * we send all 'meta' blobs (anything that isn't a feature) to process 0
            #   * we assign feature blobs to a process based on it's first subtree.
            #     (all features in tree `datasetname/feature/01` will go to process 1, etc)
            #   * after the importing is all done, we merge the trees together.
            #   * there should never be any conflicts in this merge process.
            for i in range(num_processes):
                if header is None:
                    # import onto a temp branch. then reset the head branch afterwards.
                    import_ref = f"refs/sno-import/{uuid.uuid4()}"
                    import_refs.append(import_ref)

                    # may be None, if head is detached
                    orig_branch = repo.head_branch
                    generated_header = generate_header(
                        repo, sources, message, import_ref
                    )
                else:
                    generated_header = header
                    # this won't work if num_processes > 1 because we'll try and write to
                    # the same branch multiple times in parallel.
                    # luckily only upgrade script passes a header in, so there we just use 1 proc.
                    assert num_processes == 1
                proc = stack.enter_context(_git_fast_import(repo, *cmd))
                procs.append(proc)
                if replace_existing != ReplaceExisting.ALL:
                    generated_header += f"from {orig_commit.oid}\n"
                proc.stdin.write(generated_header.encode("utf8"))

            # Write the extra blob that records the repo's version:
            for i, blob_path in write_blobs_to_stream(procs[0].stdin, extra_blobs):
                if (
                    replace_existing != ReplaceExisting.ALL
                    and blob_path in starting_tree
                ):
                    raise ValueError(f"{blob_path} already exists")

            if num_processes == 1:

                def proc_for_feature_path(path):
                    return procs[0]

            else:

                def proc_for_feature_path(path):
                    first_subtree = int(path.rsplit("/", 3)[1], 16)
                    return procs[first_subtree % len(procs)]

            for source in sources:
                replacing_dataset = None
                if replace_existing == ReplaceExisting.GIVEN:
                    try:
                        replacing_dataset = repo.datasets()[source.dest_path]
                    except KeyError:
                        # no such dataset; no problem
                        replacing_dataset = None

                    fast_import_clear_trees(
                        procs=procs,
                        replace_ids=replace_ids,
                        replacing_dataset=replacing_dataset,
                        source=source,
                    )

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
                                proc_for_feature_path(path).stdin.write(
                                    f"D {path}\n".encode("utf8")
                                )
                                yield pk

                        src_iterator = source.get_features(_ids(), ignore_missing=True)
                    else:
                        src_iterator = source.features()

                    progress_every = None
                    if verbosity >= 1:
                        progress_every = max(100, 100_000 // (10 ** (verbosity - 1)))

                    if should_compare_imported_features_against_old_features(
                        repo, source, replacing_dataset
                    ):
                        feature_blob_iter = dataset.import_iter_feature_blobs(
                            repo,
                            src_iterator,
                            source,
                            replacing_dataset=replacing_dataset,
                        )
                    else:
                        feature_blob_iter = dataset.import_iter_feature_blobs(
                            repo, src_iterator, source
                        )

                    for i, (feature_path, blob_data) in enumerate(feature_blob_iter):
                        stream = proc_for_feature_path(feature_path).stdin
                        stream.write(
                            f"M 644 inline {feature_path}\ndata {len(blob_data)}\n".encode(
                                "utf8"
                            )
                        )
                        stream.write(blob_data)
                        stream.write(b"\n")

                        if i and progress_every and i % progress_every == 0:
                            click.echo(
                                f"  {i:,d} features... @{time.monotonic()-t1:.1f}s"
                            )

                        if limit is not None and i == (limit - 1):
                            click.secho(
                                f"  Stopping at {limit:,d} features", fg="yellow"
                            )
                            break
                    t2 = time.monotonic()
                    if verbosity >= 1:
                        click.echo(
                            f"Added {num_rows:,d} Features to index in {t2-t1:.1f}s"
                        )
                        click.echo(
                            f"Overall rate: {(num_rows/(t2-t1 or 1E-3)):.0f} features/s)"
                        )

                    # Meta items - written second as certain importers generate extra metadata as they import features.
                    for x in write_blobs_to_stream(
                        procs[0].stdin, dataset.import_iter_meta_blobs(repo, source)
                    ):
                        pass

        t3 = time.monotonic()
        if verbosity >= 1:
            click.echo(f"Closed in {(t3-t2):.0f}s")

        if import_refs:
            # we created temp branches for the import above.
            # each of the branches has _part_ of the import.
            # we have to merge the trees together to get a sensible commit.
            trees = [repo.revparse_single(b).peel(pygit2.Tree) for b in import_refs]
            if len(import_refs) > 1:
                click.echo(f"Joining {len(import_refs)} parallel-imported trees...")
                builder = RichTreeBuilder(repo, trees[0])
                for t in trees[1:]:
                    datasets = Datasets(t, SUPPORTED_DATASET_CLASS)
                    for ds in datasets:
                        try:
                            feature_tree = ds.feature_tree
                        except KeyError:
                            pass
                        else:
                            for subtree in feature_tree:
                                builder.insert(
                                    f"{ds.path}/{ds.FEATURE_PATH}{subtree.name}",
                                    subtree,
                                )
                new_tree = builder.flush()
                t4 = time.monotonic()
                click.echo(f"Joined trees in {(t4-t3):.0f}s")
            else:
                new_tree = trees[0]
                t4 = time.monotonic()
            if not allow_empty:
                if new_tree == orig_tree:
                    raise NotFound("No changes to commit", exit_code=NO_CHANGES)

            # use the existing commit details we already imported, but use the new tree
            existing_commit = repo.revparse_single(import_refs[0]).peel(pygit2.Commit)
            repo.create_commit(
                orig_branch or "HEAD",
                existing_commit.author,
                existing_commit.committer,
                existing_commit.message,
                new_tree.id,
                existing_commit.parent_ids,
            )
    finally:
        # remove the import branches
        for b in import_refs:
            if b in repo.references:
                repo.references.delete(b)


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
