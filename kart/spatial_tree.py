import functools
import logging
import math
import re
import subprocess
import sys
import time

import click
from osgeo import osr, ogr
from pysqlite3 import dbapi2 as sqlite
from sqlalchemy import Column, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import BLOB


from .cli_util import add_help_subcommand, tool_environment
from .crs_util import make_crs, normalise_wkt
from .exceptions import SubprocessError
from .geometry import Geometry
from .repo import KartRepoState, KartRepoFiles
from .serialise_util import msg_unpack
from .structs import CommitWithReference
from .sqlalchemy import TableSet
from .sqlalchemy.sqlite import sqlite_engine


L = logging.getLogger("kart.spatial_tree")

# These three parameters cannot be changed without rewriting the entire index:
S2_MIN_LEVEL = 4
S2_MAX_LEVEL = 16
S2_LEVEL_MOD = 1
# When the index is written, these parameters are stored with the index, so that we can continue to update and use
# that existing index without rewriting it even if we decide to tweak these numbers to better values for new repos.

S2_PARAMETERS = {
    "min_level": S2_MIN_LEVEL,
    "max_level": S2_MAX_LEVEL,
    "level_mod": S2_LEVEL_MOD,
}

# But this value can be changed at any time.
S2_MAX_CELLS_INDEX = 8


def _revlist_command(repo):
    return [
        "git",
        "-C",
        repo.path,
        "rev-list",
        "--objects",
        "--filter=object:type=blob",
        "--missing=allow-promisor",
    ]


DS_PATH_PATTERN = r'(.+)/\.(sno|table)-dataset/'


def _parse_revlist_output(line_iter, rel_path_pattern):
    full_path_pattern = re.compile(DS_PATH_PATTERN + rel_path_pattern)

    for line in line_iter:
        parts = line.split(" ", maxsplit=1)
        if len(parts) != 2:
            continue
        oid, path = parts

        m = full_path_pattern.match(path)
        if not m:
            continue
        ds_path = m.group(1)
        yield ds_path, oid


class CrsHelper:
    """
    Loads all CRS definitions for a particular dataset,
    and creates transforms
    """

    def __init__(self, repo):
        self.repo = repo
        self.ds_to_transforms = {}
        self.target_crs = make_crs("EPSG:4326")

    def transforms_for_dataset(self, ds_path):
        transforms = self.ds_to_transforms.get(ds_path)
        if transforms is None:
            transforms = self._load_transforms_for_dataset(ds_path)
            self.ds_to_transforms[ds_path] = transforms
        return transforms

    def _load_transforms_for_dataset(self, ds_path):
        if ds_path in self.ds_to_transforms:
            return self.ds_to_transforms[ds_path]

        crs_oids = set(self.iter_crs_oids(ds_path))
        distinct_crs_list = []
        transforms = []
        descs = []
        for crs_oid in crs_oids:
            try:
                crs = self.crs_from_oid(crs_oid)
                if crs in distinct_crs_list or any(
                    crs.IsSame(c) for c in distinct_crs_list
                ):
                    continue
                distinct_crs_list.append(crs)

                transform, desc = self.transform_from_src_crs(crs)
                transforms.append(transform)
                descs.append(desc)
            except Exception as e:
                L.warning(
                    f"Couldn't load transform for CRS {crs_oid} at {ds_path}\n{e}"
                )
        L.info(f"Loaded CRS transforms for {ds_path}: {', '.join(descs)}")
        return transforms

    def iter_crs_oids(self, ds_path):
        cmd = [
            *_revlist_command(self.repo),
            "--all",
            "--",
            *self.all_crs_paths(ds_path),
        ]
        try:
            r = subprocess.run(
                cmd,
                encoding="utf8",
                check=True,
                capture_output=True,
                env=tool_environment(),
            )
        except subprocess.CalledProcessError as e:
            raise SubprocessError(
                f"There was a problem with git rev-list: {e}", called_process_error=e
            )
        for d, crs_oid in _parse_revlist_output(
            r.stdout.splitlines(), r"meta/crs/[^/]+"
        ):
            assert d == ds_path
            yield crs_oid

    def all_crs_paths(self, ds_path):
        # Delete .sno-dataset if we drop V2 support.
        yield f"{ds_path}/.sno-dataset/meta/crs/"
        yield f"{ds_path}/.table-dataset/meta/crs/"

    @functools.lru_cache()
    def crs_from_oid(self, crs_oid):
        wkt = normalise_wkt(self.repo[crs_oid].data.decode("utf-8"))
        return make_crs(wkt)

    def transform_from_src_crs(self, src_crs):
        if src_crs.IsSame(self.target_crs):
            transform = None
            desc = f"IDENTITY({src_crs.GetAuthorityCode(None)})"
        else:
            transform = osr.CoordinateTransformation(src_crs, self.target_crs)
            desc = f"{src_crs.GetAuthorityCode(None)} -> {self.target_crs.GetAuthorityCode(None)}"
        return transform, desc


class SpatialTreeTables(TableSet):
    """Tables for associating a variable number of S2 tokens with each feature."""

    def __init__(self):
        super().__init__()

        # "commits" tracks all the commits we have indexed.
        # A commit is only considered indexed if ALL of its ancestors are also indexed - this means
        # relatively few commits need to be recorded as being indexed in this table.
        self.commits = Table(
            "commits",
            self.sqlalchemy_metadata,
            # "commit_id" is the commit ID (the SHA-1 hash), in binary (20 bytes).
            # Is equivalent to 40 chars of hex eg: d08c3dd220eea08d8dfd6d4adb84f9936c541d7a
            Column("commit_id", BLOB, nullable=False, primary_key=True),
            sqlite_with_rowid=False,
        )

        # "feature_envelopes" maps every feature to its encoded envelope.
        # If a feature has no envelope (eg no geometry), then it is not found in this table.
        self.blobs = Table(
            "feature_envelopes",
            self.sqlalchemy_metadata,
            # "blob_id" is the git object ID (the SHA-1 hash) of a feature, in binary (20 bytes).
            # Is equivalent to 40 chars of hex eg: d08c3dd220eea08d8dfd6d4adb84f9936c541d7a
            Column("blob_id", BLOB, nullable=False, primary_key=True),
            Column("envelope", BLOB, nullable=False),
            sqlite_with_rowid=False,
        )


SpatialTreeTables.copy_tables_to_class()


def drop_tables(sess):
    sess.execute("DROP TABLE IF EXISTS commits;")
    sess.execute("DROP TABLE IF EXISTS feature_envelopes;")


def iter_feature_oids(repo, start_commits, stop_commits):
    cmd = [*_revlist_command(repo), *start_commits, "--not", *stop_commits]
    try:
        p = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            encoding="utf8",
            env=tool_environment(),
        )
        yield from _parse_revlist_output(p.stdout, r"feature/.+")
    except subprocess.CalledProcessError as e:
        raise SubprocessError(
            f"There was a problem with git rev-list: {e}", called_process_error=e
        )


def _minimal_description_of_commit_set(repo, commits):
    """
    Returns the minimal set of commit IDs that have the same set of ancestors as
    the given set of commit IDs.
    Stated differently - returns the given commits except for those which are
    reachable by following ancestors of commits in the given set.
    """
    cmd = ["git", "-C", repo.path, "merge-base", "--independent"] + list(commits)
    try:
        r = subprocess.run(
            cmd,
            encoding="utf8",
            check=True,
            capture_output=True,
            env=tool_environment(),
        )
    except subprocess.CalledProcessError as e:
        raise SubprocessError(
            f"There was a problem with git merge-base: {e}", called_process_error=e
        )
    return set(r.stdout.splitlines())


def _build_on_last_index(repo, start_commits, engine, clear_existing=False):
    """
    Given a set of commits to index (including their ancestors) - the "start-commits" - returns the following:
    - the minimal description of the "start-commits"
    - the "stop-commits" - the commits that have already been indexed (including ancestors).
      The the start commits will have been indexed including their ancestors if we stop
      following ancestors once we reach these commits, since they are already indexed.
    - The minimal description of all commits that will have been indexed once this index is finished.
      (This could include commits from both "start-commits" and from "stop-commits".)

    This allows us to index the given commits (including their ancestors) by building on work we did
    last time the index was brought up to date (or completed up to a certain point).
    """

    stop_commits = set()

    if not clear_existing:
        with sessionmaker(bind=engine)() as sess:
            commits_table_exists = sess.scalar(
                "SELECT count(*) FROM sqlite_master WHERE name = 'commits';"
            )
            if commits_table_exists:
                stop_commits = {
                    row[0].hex()
                    for row in sess.execute("SELECT commit_id FROM commits;")
                }

    all_independent_commits = _minimal_description_of_commit_set(
        repo, start_commits | stop_commits
    )
    start_commits = all_independent_commits - stop_commits
    return (start_commits, stop_commits, all_independent_commits)


def _format_commits(repo, commit_ids):
    if not commit_ids:
        return None
    length = len(repo[next(iter(commit_ids))].short_id)
    return " ".join(c[:length] for c in commit_ids)


def update_spatial_tree(
    repo, commits, verbosity=1, clear_existing=False, dry_run=False
):
    """
    Index the commits given in commit_spec, and write them to the feature_envelopes.db repo file.

    repo - the Kart repo containing the commits to index, and in which to write the index file.
    commits - a set of commit IDs to index (ancestors of these are implicitly included).
    verbosity - how much non-essential information to output.
    clear_existing - when true, deletes any pre-existing data before re-indexing.
    """
    crs_helper = CrsHelper(repo)

    db_path = repo.gitdir_file(KartRepoFiles.FEATURE_ENVELOPES)
    engine = sqlite_engine(db_path)

    # Find out where we were up to last time, don't reindex anything that's already indexed.
    start_commits, stop_commits, all_independent_commits = _build_on_last_index(
        repo, commits, engine, clear_existing=clear_existing
    )

    if not start_commits:
        click.echo("Nothing to do: index already up to date.")
        return

    feature_oid_iter = iter_feature_oids(repo, start_commits, stop_commits)

    progress_every = None
    if verbosity >= 1:
        progress_every = max(100, 100_000 // (10 ** (verbosity - 1)))

    with sessionmaker(bind=engine)() as sess:
        if clear_existing:
            drop_tables(sess)

        SpatialTreeTables.create_all(sess)

    # We index from the most recent commits, and stop at the already-indexed ancestors -
    # but in terms of logging it makes more sense to say: indexing from <ANCESTORS> to <CURRENT>.
    ancestor_desc = _format_commits(repo, stop_commits)
    current_desc = _format_commits(repo, start_commits)
    if not ancestor_desc:
        click.echo(f"Indexing from the very start up to {current_desc} ...")
    else:
        click.echo(f"Indexing from {ancestor_desc} up to {current_desc} ...")

    if dry_run:
        click.echo("(Not performing the indexing due to --dry-run.")
        sys.exit(0)

    t0 = time.monotonic()
    i = 0

    # Using sqlite directly here instead of sqlalchemy is about 10x faster.
    # Possibly due to huge number of unbatched queries.
    # TODO - investigate further.
    db = sqlite.connect(f"file:{db_path}", uri=True)
    with db:
        dbcur = db.cursor()

        for i, (ds_path, feature_oid) in enumerate(feature_oid_iter):
            if i and progress_every and i % progress_every == 0:
                click.echo(f"  {i:,d} features... @{time.monotonic()-t0:.1f}s")

            transforms = crs_helper.transforms_for_dataset(ds_path)
            if not transforms:
                continue
            geom = get_geometry(repo, feature_oid)
            if geom is None:
                continue
            encoded_envelope = get_encoded_envelope(geom, transforms)

            params = (bytes.fromhex(feature_oid), encoded_envelope)
            dbcur.execute(
                "INSERT OR REPLACE INTO feature_envelopes (blob_id, envelope) VALUES (?, ?);",
                params,
            )

        # Update indexed commits.
        params = [(bytes.fromhex(commit_id),) for commit_id in all_independent_commits]
        dbcur.execute("DELETE FROM commits;")
        dbcur.executemany("INSERT INTO commits (commit_id) VALUES (?);", params)

    t1 = time.monotonic()
    click.echo(f"Indexed {i} features in {t1-t0:.1f}s")


NO_GEOMETRY_COLUMN = object()


def get_geometry(repo, feature_oid):
    legend, fields = msg_unpack(repo[feature_oid])
    col_id = get_geometry.legend_to_col_id.get(legend)
    if col_id is None:
        col_id = _find_geometry_column(fields)
        get_geometry.legend_to_col_id[legend] = col_id
    return fields[col_id] if col_id is not NO_GEOMETRY_COLUMN else None


get_geometry.legend_to_col_id = {}


def _find_geometry_column(fields):
    result = NO_GEOMETRY_COLUMN
    for i, field in enumerate(fields):
        if isinstance(field, Geometry):
            return i
        if field is None:
            result = None
    return result


def get_encoded_envelope(geom, transforms):
    """
    Returns an envelope of the geometry in EPSG:4326 (using the given transform(s) to convert it),
    and encoded as a short string of bytes, ready for writing to the sqlite database.
    """
    return encode_envelope(get_envelope_for_indexing(geom, transforms))


# Increasing this parameter means the index is more accurate, but takes up more space.
BITS_PER_VALUE = 20  # Keep this parameter even otherwise you waste half-bytes.
BITS_PER_ENVELOPE = 4 * BITS_PER_VALUE
BYTES_PER_ENVELOPE = math.ceil(BITS_PER_ENVELOPE / 8)
MAX_ENCODED_VALUE = 2 ** BITS_PER_VALUE - 1
MAX_ENCODED_ENVELOPE = 2 ** BITS_PER_ENVELOPE - 1
BYTE_ORDER = "big"


def encode_envelope(envelope):
    """
    Encodes a (min-x, max-x, min-y, max-y) envelope where max-x can be on the far side of the antimeridian.
    Encodes each value using BITS_PER_VALUE bits, as an unsigned int that exactly spans the entire valid range.
    """
    # Note: the x-values have a valid range of 720 degrees at the equator. We could get an extra bit of
    # precision if we gave them a valid range of only 360 degrees and stored max-x as an offset from min-x, but
    # it makes the code more complicated.
    result = _encode_value(envelope[0], -180, 540, math.floor)
    result <<= BITS_PER_VALUE
    result |= _encode_value(envelope[1], -180, 540, math.ceil)
    result <<= BITS_PER_VALUE
    result |= _encode_value(envelope[2], -90, 90, math.floor)
    result <<= BITS_PER_VALUE
    result |= _encode_value(envelope[3], -90, 90, math.ceil)
    assert 0 <= result <= MAX_ENCODED_ENVELOPE
    return result.to_bytes(BYTES_PER_ENVELOPE, BYTE_ORDER)


def _encode_value(value, min_value, max_value, round_fn):
    normalised = (value - min_value) / (max_value - min_value)
    encoded = round_fn(normalised * MAX_ENCODED_VALUE)
    assert 0 <= encoded <= MAX_ENCODED_VALUE
    return encoded


def decode_envelope(encoded):
    """Inverse of encode_envelope."""
    encoded = int.from_bytes(encoded, BYTE_ORDER)
    assert 0 <= encoded <= MAX_ENCODED_ENVELOPE
    max_y = _decode_value(encoded & MAX_ENCODED_VALUE, -90, 90)
    encoded >>= BITS_PER_VALUE
    min_y = _decode_value(encoded & MAX_ENCODED_VALUE, -90, 90)
    encoded >>= BITS_PER_VALUE
    max_x = _decode_value(encoded & MAX_ENCODED_VALUE, -180, 540)
    encoded >>= BITS_PER_VALUE
    min_x = _decode_value(encoded & MAX_ENCODED_VALUE, -180, 540)
    return min_x, max_x, min_y, max_y


def _decode_value(encoded, min_value, max_value):
    assert 0 <= encoded <= MAX_ENCODED_VALUE
    normalised = encoded / MAX_ENCODED_VALUE
    return normalised * (max_value - min_value) + min_value


INF = float("inf")


def get_envelope_for_indexing(geom, transforms):
    """
    Returns an envelope in EPSG:4326 that contains the entire geometry.
    Uses any and all of the given transforms to convert to EPSG:4326 (this is so that this index
    will return the right results even if the CRS of this feature has changed at some point).
    The returned envelope is ordered according to the GPKG specification: min-x, max-x, min-y, max-y.
    """

    # TODO: Make this more rigorous - ie, make sure that:
    # -90 <= S <= N <= +90
    # -180 <= W <= +180
    # 0 <= (E - W) <= 360 ; therefore -180 <= W <= +540

    # Result initialised to an infinite and negative area which disappears when unioned.
    result = [+INF, -INF, +INF, -INF]

    raw_envelope = geom.envelope(only_2d=True, calculate_if_missing=True)
    for transform in transforms:
        if transform is not None:
            envelope = _transform_envelope(raw_envelope, transform)
        else:
            envelope = raw_envelope
        result = _union_of_envelopes(result, envelope)
    click.echo(result)
    return result


def _transform_envelope(envelope, transform):
    # FIXME: This function is not 100% correct - it should segmentize each edge of the envelope and transform
    # all the segments, to account for the fact that these edges should sometimes curve when reprojected.
    # But right now we just reproject the corners and draw lines between them - in rare cases the result
    # won't cover the entire geometry.
    corners = ogr.Geometry(ogr.wkbLineString)
    # At least we transform all 4 corners, which avoids some errors caused by only transforming 2 of them.
    corners.AddPoint_2D(envelope[0], envelope[2])
    corners.AddPoint_2D(envelope[0], envelope[3])
    corners.AddPoint_2D(envelope[1], envelope[2])
    corners.AddPoint_2D(envelope[1], envelope[3])
    corners.Transform(transform)
    values = [corners.GetPoint_2D(c) for c in range(4)]
    x_values = [v[0] for v in values]
    y_values = [v[1] for v in values]
    return min(x_values), max(x_values), min(y_values), max(y_values)


def _union_of_envelopes(a, b):
    # Returns the union of two envelopes in GPKG (min-x, max-x, min-y, max-y) format.
    # Overwrites the first of the two envelopes.
    a[0] = min(a[0], b[0])
    a[1] = max(a[1], b[1])
    a[2] = min(a[2], b[2])
    a[3] = max(a[3], b[3])
    return a


def _resolve_all_commit_refs(repo):
    cmd = ["git", "-C", repo.path, "show-ref", "--hash", "--head"]
    try:
        r = subprocess.run(
            cmd,
            encoding="utf8",
            check=True,
            capture_output=True,
            env=tool_environment(),
        )
    except subprocess.CalledProcessError as e:
        raise SubprocessError(
            f"There was a problem with git show-ref: {e}", called_process_error=e
        )
    result = set()
    for c in r.stdout.splitlines():
        try:
            if repo[c].type_str == "commit":
                result.add(c)
        except KeyError:
            pass
    return result


def _resolve_commits(repo, commitish_list):
    return set(
        CommitWithReference.resolve(repo, commitish).id.hex
        for commitish in commitish_list
    )


@add_help_subcommand
@click.group()
@click.pass_context
def spatial_tree(ctx, **kwargs):
    """
    Commands for maintaining an S2-cell based spatial index.
    """


@spatial_tree.command()
@click.option(
    "--clear-existing",
    is_flag=True,
    default=False,
    help="Clear existing index before re-indexing",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Don't do any indexing, instead just output what would be indexed.",
)
@click.argument(
    "commits",
    nargs=-1,
)
@click.pass_context
def index(ctx, clear_existing, dry_run, commits):
    """
    Indexes all features added by the supplied commits and their ancestors.
    If no commits are supplied, indexes all features in all commits.
    """
    repo = ctx.obj.get_repo(allowed_states=KartRepoState.ALL_STATES)
    if not commits:
        commits = _resolve_all_commit_refs(repo)
    else:
        commits = _resolve_commits(repo, commits)

    update_spatial_tree(
        repo,
        commits,
        verbosity=ctx.obj.verbosity + 1,
        clear_existing=clear_existing,
        dry_run=dry_run,
    )
