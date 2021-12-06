import functools
import logging
import re
import subprocess
import sys
import time

import click
from osgeo import osr, ogr
from pysqlite3 import dbapi2 as sqlite
from sqlalchemy import Column, ForeignKey, Integer, Table, Text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import BLOB


from .cli_util import add_help_subcommand, tool_environment
from .crs_util import make_crs, normalise_wkt
from .exceptions import SubprocessError
from .geometry import Geometry, GeometryType, geom_envelope, gpkg_geom_to_ogr
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

        # "parameters" records the parameters used to create this index. The same parameters must be used
        # when updating the index and when querying the index - it won't work if not kept consistent.
        self.parameters = Table(
            "parameters",
            self.sqlalchemy_metadata,
            Column("min_level", Integer, nullable=False),
            Column("max_level", Integer, nullable=False),
            Column("level_mod", Integer, nullable=False),
        )

        # "commits" tracks all the commits we have indexed.
        # A commit is only considered indexed if ALL of its ancestors are also indexed - this means
        # relatively few commits need to be recorded as being indexed in this table.
        self.commits = Table(
            "commits",
            self.sqlalchemy_metadata,
            # "commit_id" is the commit ID (the SHA-1 hash), in binary (20 bytes).
            # Is equivalent to 40 chars of hex eg: d08c3dd220eea08d8dfd6d4adb84f9936c541d7a
            Column("commit_id", BLOB, nullable=False, primary_key=True),
        )

        # "blobs" tracks all the features we have indexed (even if they have no associated S2 tokens).
        self.blobs = Table(
            "blobs",
            self.sqlalchemy_metadata,
            # From a user-perspective, "rowid" isjust an arbitrary integer primary key.
            # In more detail: This column aliases to the sqlite rowid of the table.
            # See https://www.sqlite.org/lang_createtable.html#rowid
            # Using the rowid directly as a foreign key (see "blob_tokens") means faster joins.
            # The rowid can be used without creating a column that aliases to it, but you shouldn't -
            # rowids might change if they are not aliased. See https://sqlite.org/lang_vacuum.html)
            Column("rowid", Integer, nullable=False, primary_key=True),
            # "blob_id" is the git object ID (the SHA-1 hash) of a feature, in binary (20 bytes).
            # Is equivalent to 40 chars of hex eg: d08c3dd220eea08d8dfd6d4adb84f9936c541d7a
            Column("blob_id", BLOB, nullable=False, unique=True),
            sqlite_autoincrement=True,
        )

        # "blob_tokens" associates 0 or more S2 cell tokens with each feature that we have indexed.
        # Technically these are "index terms" not S2 cell tokens since they are slightly more complicated
        # - there are two types of term, ANCESTOR and COVERING. COVERING terms start with a "$" prefix.
        # For more details on how indexing works, consult the S2RegionTermIndexer documentation.
        self.blob_tokens = Table(
            "blob_tokens",
            self.sqlalchemy_metadata,
            # Reference to blobs.rowid.
            Column(
                "blob_rowid",
                Integer,
                ForeignKey("blobs.rowid"),
                nullable=False,
                primary_key=True,
            ),
            # S2 index term eg "6d6dd90351b31cbf" or "$6e1".
            # To locate an S2 cell by token, see https://s2.sidewalklabs.com/regioncoverer/
            # (and remove any $ prefix from these index terms so that they are simply S2 cell tokens).
            Column(
                "s2_token",
                Text,
                nullable=False,
                primary_key=True,
            ),
        )


SpatialTreeTables.copy_tables_to_class()


def drop_tables(sess):
    sess.execute("DROP TABLE IF EXISTS parameters;")
    sess.execute("DROP TABLE IF EXISTS commits;")
    sess.execute("DROP TABLE IF EXISTS blob_tokens;")
    sess.execute("DROP TABLE IF EXISTS blobs;")


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


def _create_indexer_from_parameters(sess):
    """
    Return an S2RegionTermIndexer configured according to the parameters table.
    Storing these parameters in a table means we can change them if needed without breaking Kart.
    """

    import s2_py as s2

    parameters_count = sess.scalar("SELECT COUNT(*) FROM parameters;")
    assert parameters_count <= 1
    if not parameters_count:
        sess.execute(
            """
            INSERT INTO parameters (min_level, max_level, level_mod)
            VALUES (:min_level, :max_level, :level_mod);
            """,
            S2_PARAMETERS,
        )

    row = sess.execute(
        "SELECT min_level, max_level, level_mod FROM parameters;"
    ).fetchone()
    assert row is not None
    min_level, max_level, level_mod = row

    s2_indexer = s2.S2RegionTermIndexer()
    s2_indexer.set_min_level(min_level)
    s2_indexer.set_max_level(max_level)
    s2_indexer.set_level_mod(level_mod)
    s2_indexer.set_max_cells(S2_MAX_CELLS_INDEX)
    return s2_indexer


def _format_commits(repo, commit_ids):
    if not commit_ids:
        return None
    length = len(repo[next(iter(commit_ids))].short_id)
    return " ".join(c[:length] for c in commit_ids)


def update_spatial_tree(
    repo, commits, verbosity=1, clear_existing=False, dry_run=False
):
    """
    Index the commits given in commit_spec, and write them to the s2_index.db repo file.

    repo - the Kart repo containing the commits to index, and in which to write the index file.
    commits - a set of commit IDs to index (ancestors of these are implicitly included).
    verbosity - how much non-essential information to output.
    clear_existing - when true, deletes any pre-existing data before re-indexing.
    """
    crs_helper = CrsHelper(repo)

    db_path = repo.gitdir_file(KartRepoFiles.S2_INDEX)
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
        s2_indexer = _create_indexer_from_parameters(sess)

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
            try:
                s2_tokens = get_s2_tokens(s2_indexer, geom, transforms)
            except Exception as e:
                L.warning(f"Couldn't generate S2 index for {feature_oid}:\n{e}")
                continue

            params = (bytes.fromhex(feature_oid),)
            row = dbcur.execute(
                "SELECT rowid FROM blobs WHERE blob_id = ?;", params
            ).fetchone()
            if row:
                rowid = row[0]
            else:
                dbcur.execute("INSERT INTO blobs (blob_id) VALUES (?);", params)
                rowid = dbcur.lastrowid

            if not s2_tokens:
                continue

            params = [(rowid, token) for token in s2_tokens]
            dbcur.executemany(
                "INSERT OR IGNORE INTO blob_tokens (blob_rowid, s2_token) VALUES (?, ?);",
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


def get_s2_tokens(s2_indexer, geom, transforms):
    envelope = get_envelope_for_indexing(geom, transforms)
    return envelope_to_s2_tokens(s2_indexer, envelope)


INF = float("inf")


def get_envelope_for_indexing(geom, transforms):
    # TODO: Make this more rigorous - ie, make sure that:
    # -90 <= S <= N <= +90
    # -180 <= W <= +180
    # 0 <= (E - W) <= 360 ; therefore -180 <= W <= +540

    # Result initialised to an infinite and negative area which disappears when unioned with.
    result = [+INF, +INF, -INF, -INF]

    # FIXME: For correctness, this should transform the original geometries before taking their bounding box.
    # The downside of that approach is that it is maybe 10x slower.
    w, e, s, n = geom.envelope(only_2d=True, calculate_if_missing=True)
    raw_envelope = w, s, e, n
    for transform in transforms:
        if transform is not None:
            envelope = _transform_envelope(raw_envelope, transform)
        else:
            envelope = raw_envelope
        result = _union_of_envelopes(result, envelope)
    return result


def _transform_envelope(envelope, transform):
    corners = ogr.Geometry(ogr.wkbLineString)
    corners.AddPoint_2D(envelope[0], envelope[1])
    corners.AddPoint_2D(envelope[2], envelope[3])
    corners.Transform(transform)
    a, b = corners.GetPoint_2D(0), corners.GetPoint_2D(1)
    return min(a[0], b[0]), min(a[1], b[1]), max(a[0], b[0]), max(a[1], b[1])


def _union_of_envelopes(a, b):
    # Note that this overwrites envelope a.
    a[0] = min(a[0], b[0])
    a[1] = min(a[1], b[1])
    a[2] = max(a[2], b[2])
    a[3] = max(a[3], b[3])
    return a


def envelope_to_s2_tokens(s2_indexer, envelope):
    import s2_py as s2

    s2_llrect = s2.S2LatLngRect.FromPointPair(
        s2.S2LatLng.FromDegrees(envelope[1], envelope[0]),
        s2.S2LatLng.FromDegrees(envelope[3], envelope[2]),
    )
    return s2_indexer.GetIndexTerms(s2_llrect, "")


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
