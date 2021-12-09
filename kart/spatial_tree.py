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
        transform = osr.CoordinateTransformation(src_crs, self.target_crs)
        if src_crs.IsSame(self.target_crs):
            desc = f"IDENTITY({src_crs.GetAuthorityCode(None)})"
        else:
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
        envelope_length = sess.scalar(
            "SELECT length(envelope) FROM feature_envelopes LIMIT 1;"
        )

    bits_per_value = envelope_length * 8 // 4 if envelope_length else None
    encoder = EnvelopeEncoder(bits_per_value)

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
            envelope = get_envelope_for_indexing(geom, transforms, feature_oid)
            if envelope is None:
                continue

            params = (bytes.fromhex(feature_oid), encoder.encode(envelope))
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


class EnvelopeEncoder:
    """Encodes and decodes bounding boxes - (w, s, e, n) tuples in degrees longitude / latitude."""

    # This is the number of bits-per-value used to store envelopes when writing to a fresh database.
    # When writing to an existing database, it will look to see how envelopes have been stored previously.
    # Increasing this parameter increases the accuracy of the envelopes, but each one takes more space.
    # This number must be even, so that four values take up a whole number of bytes.
    DEFAULT_BITS_PER_VALUE = 20

    def __init__(self, bits_per_value=None):
        if bits_per_value is None:
            bits_per_value = self.DEFAULT_BITS_PER_VALUE

        assert bits_per_value % 2 == 0  # bits_per_value must be even.
        self.BITS_PER_VALUE = bits_per_value
        self.BITS_PER_ENVELOPE = 4 * self.BITS_PER_VALUE
        self.BYTES_PER_ENVELOPE = self.BITS_PER_ENVELOPE // 8
        self.VALUE_MAX_INT = 2 ** self.BITS_PER_VALUE - 1
        self.ENVELOPE_MAX_INT = 2 ** self.BITS_PER_ENVELOPE - 1

        self.BYTE_ORDER = "big"

    def encode(self, envelope):
        """
        Encodes a (w, s, e, n) envelope where -180 <= w, e <= 180 and -90 <= s, n <= 90.
        Scale each value to a unsigned integer of bitlength BITS_PER_VALUE such that 0 represents the min value (eg -180
        for longitude) and 2**BITS_PER_VALUE - 1 represents the max value (eg 180 for longitude), then concatenates
        the values together into a single unsigned integer of bitlength BITS_PER_VALUE, which is encoded to a byte array
        of length BYTES_PER_ENVELOPE using a big-endian encoding.
        """
        integer = self._encode_value(envelope[0], -180, 180, math.floor)
        integer <<= self.BITS_PER_VALUE
        integer |= self._encode_value(envelope[1], -90, 90, math.floor)
        integer <<= self.BITS_PER_VALUE
        integer |= self._encode_value(envelope[2], -180, 180, math.ceil)
        integer <<= self.BITS_PER_VALUE
        integer |= self._encode_value(envelope[3], -90, 90, math.ceil)
        assert 0 <= integer <= self.ENVELOPE_MAX_INT
        return integer.to_bytes(self.BYTES_PER_ENVELOPE, self.BYTE_ORDER)

    def _encode_value(self, value, min_value, max_value, round_fn):
        assert min_value <= value <= max_value
        normalised = (value - min_value) / (max_value - min_value)
        encoded = round_fn(normalised * self.VALUE_MAX_INT)
        assert 0 <= encoded <= self.VALUE_MAX_INT
        return encoded

    def decode(self, encoded):
        """Inverse of encode_envelope."""
        integer = int.from_bytes(encoded, self.BYTE_ORDER)
        assert 0 <= integer <= self.ENVELOPE_MAX_INT
        n = self._decode_value(integer & self.VALUE_MAX_INT, -90, 90)
        integer >>= self.BITS_PER_VALUE
        e = self._decode_value(integer & self.VALUE_MAX_INT, -180, 180)
        integer >>= self.BITS_PER_VALUE
        s = self._decode_value(integer & self.VALUE_MAX_INT, -90, 90)
        integer >>= self.BITS_PER_VALUE
        w = self._decode_value(integer & self.VALUE_MAX_INT, -180, 180)
        return w, s, e, n

    def _decode_value(self, encoded, min_value, max_value):
        assert 0 <= encoded <= self.VALUE_MAX_INT
        normalised = encoded / self.VALUE_MAX_INT
        return normalised * (max_value - min_value) + min_value


def get_envelope_for_indexing(geom, transforms, feature_oid):
    """
    Returns an envelope in EPSG:4326 that contains the entire geometry. Tries all of the given transforms to convert
    to EPSG:4326 and returns an envelope containing all of the possibilities. This is so we can find all features that
    potentially intersect a region even if their CRS has changed at some point, so they could be in more than one place.
    The returned envelope is ordered (w, s, e, n), with longitudes in the range [-180, 180] and latitudes [-90, 90].
    It is always true that s <= n. Normally w <= e unless it crosses the anti-meridian, in which case e < w.
    """

    result = None

    try:
        minmax_envelope = _transpose_gpkg_envelope(
            geom.envelope(only_2d=True, calculate_if_missing=True)
        )

        for transform in transforms:
            envelope = transform_minmax_envelope(minmax_envelope, transform)
            result = union_of_envelopes(result, envelope)
        return result
    except Exception as e:
        L.warning(f"Couldn't index feature {feature_oid}:\n{e}")
        return None


def _transpose_gpkg_envelope(envelope):
    """
    GPKG uses the envelope format (min-x, max-x, min-y, max-y). We use the envelope format (w, s, e, n).
    We transpose GPKG envelope to (min-x, min-y, max-x, max-y), so that it least it has the same axis-order as our
    format, and we handle anti-meridian issues seperately (see transform_minmax_envelope).
    """
    return envelope[0], envelope[2], envelope[1], envelope[3]


def transform_minmax_envelope(minmax_envelope, transform):
    """
    Given an envelope in (min-x, min-y, max-x, max-y) format in any CRS, transforms it to EPSG:4326 using the given
    transform, then returns an axis-aligned envelope in EPSG:4326 in (w, s, e, n) order that bounds the original
    (but which may have a slightly larger area due to the axis-aligned edges not lining up with the original).
    The returned envelope has w <= e unless it crosses the antimeridian, in which case e < w.
    """

    # FIXME: This function is not 100% correct - it should segmentize each edge of the envelope and transform
    # all the segments, to account for the fact that these edges should sometimes curve when reprojected.
    # But right now we just reproject the corners and assume straight lines between them - in rare cases the result
    # won't cover the entire geometry.

    ring = anticlockwise_ring_from_minmax_envelope(minmax_envelope)
    ring.Transform(transform)
    ring_points = [ring.GetPoint_2D(i) for i in range(5)]

    if _is_clockwise(ring_points):
        # The ring was anticlockwise, but when projected and EPSG:4326 into the range [-180, 180] it became clockwise.
        # We need to try different interprerations of the ring until we find one where it is anticlockwise (this will
        # cross the meridian). Once we've found this interpretation, we can treat the min-x and max-x as w and e.
        ring_points = _fix_ring_winding_order(ring_points)

    x_values = [p[0] for p in ring_points]
    y_values = [p[1] for p in ring_points]

    w = _wrap_lon(min(x_values))
    s = min(y_values)
    e = _wrap_lon(max(x_values))
    n = max(y_values)

    return (w, s, e, n)


def anticlockwise_ring_from_minmax_envelope(minmax_envelope):
    """Given an envelope in (min-x, min-y, max-x, max-y) format, builds an anticlockwise ring around it."""
    ring = ogr.Geometry(ogr.wkbLinearRing)
    # The envelope has the following format: min-x, min-y, max-x, max-ys.
    # We start at min-x, min-y and travel around it in an anti-clockwise direction:
    ring.AddPoint_2D(minmax_envelope[0], minmax_envelope[1])
    ring.AddPoint_2D(minmax_envelope[2], minmax_envelope[1])
    ring.AddPoint_2D(minmax_envelope[2], minmax_envelope[3])
    ring.AddPoint_2D(minmax_envelope[0], minmax_envelope[3])
    ring.AddPoint_2D(minmax_envelope[0], minmax_envelope[1])
    return ring


def _is_clockwise(ring_points):
    """
    Does a polygon area calculation to determine whether the given ring or polygon is clockwise.
    For explanation see https://en.wikipedia.org/wiki/Shoelace_formula
    Ring_points should be a list of (x, y) tuples where the first and last point are the same.
    """
    result = 0
    for i in range(len(ring_points) - 1):
        result += (
            ring_points[i][0] * ring_points[i + 1][1]
            - ring_points[i][1] * ring_points[i + 1][0]
        )
    return result < 0


def _is_anticlockwise(ring_points):
    return not _is_clockwise(ring_points)


def _fix_ring_winding_order(ring_points):
    """
    Shifts each point in turn eastwards by 360 degrees around the globe until the winding order is fixed
    to be anticlockwise. This works on any ring, but has O(n^2) efficiency, so ideally should only be
    used on geometries with few points.
    Ring_points should be a list of (x, y) tuples where the first and last point are the same.
    """
    if _is_anticlockwise(ring_points):
        return ring_points

    # Make a copy of points, and make each point a list not a tuple so it can be modified easily.
    ring_points = [list(p) for p in ring_points]

    sorted_x_values = sorted(set(p[0] for p in ring_points))
    for x in sorted_x_values:
        for p in ring_points:
            if p[0] <= x:
                p[0] += 360
        if _is_anticlockwise(ring_points):
            return ring_points
    raise AssertionError("This should never happen")


def _unwrap_lon_envelope(w, e):
    """
    Given a longitude envelope where -180 <= w, e <= 180, and w <= e unless it crosses the antimeridian, in which case e < w:
    This returns an equivalent longitude range where w remains the same, and e exceeds w by the true size of the range.
    The result will follow these three rules: -180 <= w <= 180 and 0 <= (e - w) <= 360 and -180 <= e <= 540.
    """
    return (w, e) if w <= e else (w, e + 360)


def _wrap_lon(x):
    """Puts any longitude in the range [-180, 179.99999999999] without moving its position on earth."""
    return (x + 180) % 360 - 180


def _wrap_lon_envelope(w, e):
    """
    Given a longitude envelope where w <= e, such as [0, 20] or [170, 190], where all x values w <= x <= e are inside the range:
    this wraps it so that -180 <= w, e <= 180, and w <= e unless the range crosses the antimeridian, in which case e < w.
    """
    wrapped_w = _wrap_lon(w)
    wrapped_e = _wrap_lon(e)

    min_x = min(wrapped_w, wrapped_e)
    max_x = max(wrapped_w, wrapped_e)
    if math.isclose(max_x - min_x, e - w, abs_tol=1e-3):
        return min_x, max_x
    else:
        return max_x, min_x


INF = float("inf")


def union_of_envelopes(env1, env2):
    """
    Returns the union of two envelopes where both are in (w, s, e, n) order and both are "wrapped" -
    that is, longitude values are in the range [-180, 180] and w <= e unless it crosses the antimeridian, in which case e < w.
    """
    if env1 is None:
        return env2
    if env2 is None:
        return env1

    w1, e1 = _unwrap_lon_envelope(env1[0], env1[2])
    w2, e2 = _unwrap_lon_envelope(env2[0], env2[2])
    width = INF

    for shift in (-360, 0, 360):
        shifted_w2 = w2 + shift
        shifted_e2 = e2 + shift
        potential_w = min(w1, shifted_w2)
        potential_e = max(e1, shifted_e2)
        potential_width = potential_e - potential_w

        if potential_width < width:
            width = potential_width
            result_w = potential_w
            result_e = potential_e

    result_s = min(env1[1], env2[1])
    result_n = max(env1[3], env2[3])
    if width >= 360:
        return (-180, result_s, 180, result_n)
    else:
        result_w, result_e = _wrap_lon_envelope(result_w, result_e)
        return (result_w, result_s, result_e, result_n)


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
