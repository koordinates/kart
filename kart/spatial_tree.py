import functools
import logging
import re
import subprocess
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
from .sqlalchemy import TableSet
from .sqlalchemy.sqlite import sqlite_engine
from .serialise_util import msg_unpack

L = logging.getLogger("kart.spatial_tree")


S2_MAX_CELLS_INDEX = 8
S2_MAX_LEVEL = 15


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
        transforms = []
        descs = []
        for crs_oid in crs_oids:
            try:
                transform, desc = self.transform_from_oid(crs_oid)
                if transform not in transforms:
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
    def transform_from_oid(self, crs_oid):
        wkt = normalise_wkt(self.repo[crs_oid].data.decode("utf-8"))
        return self.transform_from_wkt(wkt)

    @functools.lru_cache()
    def transform_from_wkt(self, wkt):
        src_crs = make_crs(wkt)
        if src_crs.IsSame(self.target_crs):
            transform = None
            desc = f"IDENTITY({src_crs.GetAuthorityCode(None)})"
        else:
            transform = osr.CoordinateTransformation(src_crs, self.target_crs)
            desc = f"{src_crs.GetAuthorityCode(None)} -> {self.target_crs.GetAuthorityCode(None)}"
        return transform, desc


class SpatialTreeTables(TableSet):
    """Tables for associating a variable number of S2 cells with each feature."""

    def __init__(self):
        super().__init__()

        # "blobs" tracks all the features we have indexed (even if they do not overlap any s2 cells).
        self.blobs = Table(
            "blobs",
            self.sqlalchemy_metadata,
            # From a user-perspective, "rowid" isjust an arbitrary integer primary key.
            # In more detail: This column aliases to the sqlite rowid of the table.
            # See https://www.sqlite.org/lang_createtable.html#rowid
            # Using the rowid directly as a foreign key (see "blob_cells") means faster joins.
            # The rowid can be used without creating a column that aliases to it, but you shouldn't -
            # rowids might change if they are not aliased. See https://sqlite.org/lang_vacuum.html)
            Column("rowid", Integer, nullable=False, primary_key=True),
            # "blob_id" is the git object ID (the SHA-1 hash) of a feature, in binary (20 bytes).
            # Is equivalent to 40 chars of hex eg: d08c3dd220eea08d8dfd6d4adb84f9936c541d7a
            Column("blob_id", BLOB, nullable=False, unique=True),
            sqlite_autoincrement=True,
        )

        # "blob_cells" associates 0 or more S2 cell tokens with each feature that we have indexed.
        self.blob_cells = Table(
            "blob_cells",
            self.sqlalchemy_metadata,
            # Reference to blobs.rowid.
            Column(
                "blob_rowid",
                Integer,
                ForeignKey("blobs.rowid"),
                nullable=False,
                primary_key=True,
            ),
            # S2 cell token eg "6d6dd90351b31cbf".
            # To locate an S2 cell by token, see https://s2.sidewalklabs.com/regioncoverer/
            Column(
                "cell_token",
                Text,
                nullable=False,
                primary_key=True,
            ),
        )


SpatialTreeTables.copy_tables_to_class()


def drop_tables(sess):
    sess.execute("DROP TABLE IF EXISTS blob_cells;")
    sess.execute("DROP TABLE IF EXISTS blobs;")


def iter_feature_oids(repo, commit_spec):
    cmd = _revlist_command(repo) + commit_spec
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


def update_spatial_tree(repo, commit_spec, verbosity=1, clear_existing=False):
    """
    Index the commits given in commit_spec, and write them to the s2_index.db repo file.

    repo - the Kart repo containing the commits to index, and in which to write the index file.
    commit_spec - a list of commits to index (ancestors of these are implicitly included).
        Commits can be exluded by prefixing with '^' (ancestors of these are implicitly excluded).
        (See git rev-list for the full list of possibilities for specifying commits).
    verbosity - how much non-essential information to output.
    clear_existing - when true, deletes any pre-existing data before re-indexing.
    """
    import pywraps2 as s2

    crs_helper = CrsHelper(repo)
    feature_oid_iter = iter_feature_oids(repo, commit_spec)

    s2_coverer = s2.S2RegionCoverer()
    s2_coverer.set_max_cells(S2_MAX_CELLS_INDEX)
    s2_coverer.set_max_level(S2_MAX_LEVEL)

    progress_every = None
    if verbosity >= 1:
        progress_every = max(100, 100_000 // (10 ** (verbosity - 1)))

    db_path = repo.gitdir_file(KartRepoFiles.S2_INDEX)
    engine = sqlite_engine(db_path)
    with sessionmaker(bind=engine)() as sess:
        if clear_existing:
            drop_tables(sess)

        SpatialTreeTables.create_all(sess)

    click.echo(f"Indexing {' '.join(commit_spec)} ...")
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
                s2_cell_tokens = find_s2_cells(s2_coverer, geom, transforms)
            except Exception as e:
                L.warning(f"Couldn't locate S2 cells for {feature_oid}:\n{e}")
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

            if not s2_cell_tokens:
                continue

            params = [(rowid, token) for token in s2_cell_tokens]
            dbcur.executemany(
                "INSERT OR IGNORE INTO blob_cells (blob_rowid, cell_token) VALUES (?, ?);",
                params,
            )

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
    for i, field in enumerate(fields):
        if isinstance(field, Geometry):
            return i
    return NO_GEOMETRY_COLUMN


def find_s2_cells(s2_coverer, geom, transforms):
    is_point = geom.geometry_type == GeometryType.POINT

    return (
        _point_f2_cells(s2_coverer, geom, transforms)
        if is_point
        else _general_s2_cells(s2_coverer, geom, transforms)
    )


def _apply_transform(original, transform, overwrite_original=False):
    if transform is None:
        return original
    result = original if overwrite_original else original.Clone()
    result.Transform(transform)
    return result


def _point_f2_cells(s2_coverer, geom, transforms):
    import pywraps2 as s2

    g = gpkg_geom_to_ogr(geom)
    one_transform = len(transforms) == 1

    result = set()
    for transform in transforms:
        g_transformed = _apply_transform(g, transform, overwrite_original=one_transform)
        p = g_transformed.GetPoint()[:2]
        s2_ll = s2.S2LatLng.FromDegrees(p[1], p[0]).Normalized()
        s2_token = s2.S2CellId(s2_ll.ToPoint()).ToToken()
        result.add(s2_token)

    return result


def _general_s2_cells(s2_coverer, geom, transforms):
    import pywraps2 as s2

    e = geom_envelope(geom)
    if e is None:
        return ()  # Empty.

    sw_src = e[0], e[2]
    ne_src = e[1], e[3]

    result = set()
    for transform in transforms:
        s2_ll = []
        for p_src in (sw_src, ne_src):
            g = ogr.Geometry(ogr.wkbPoint)
            g.AddPoint(*p_src)
            _apply_transform(g, transform, overwrite_original=True)
            p_dest = g.GetPoint()[:2]
            s2_ll.append(s2.S2LatLng.FromDegrees(p_dest[1], p_dest[0]).Normalized())

        s2_llrect = s2.S2LatLngRect.FromPointPair(*s2_ll)
        for s2_cell_id in s2_coverer.GetCovering(s2_llrect):
            result.add(s2_cell_id.ToToken())

    return result


@add_help_subcommand
@click.group()
@click.pass_context
def spatial_tree(ctx, **kwargs):
    """
    Commands for maintaining an S2-cell based spatial index.
    """


@spatial_tree.command()
@click.option(
    "--all",
    "index_all_commits",
    is_flag=True,
    default=False,
    help=("Index / re-index all existing commits"),
)
@click.option(
    "--clear-existing",
    is_flag=True,
    default=False,
    help=("Clear existing index before re-indexing"),
)
@click.argument(
    "commits",
    nargs=-1,
)
@click.pass_context
def index(ctx, index_all_commits, clear_existing, commits):
    """
    Indexes all features added by the supplied commits and their ancestors.
    The commits can be specified in any format accepted by git rev-list, including
    --all, COMMIT-TO-INCLUDE, ^COMMIT-TO-EXCLUDE, and RANGE-START..RANGE-END
    """
    if index_all_commits and commits:
        raise click.UsageError("Can't supply both --all and commits to be indexed")
    elif not index_all_commits and not commits:
        raise click.UsageError("No commits to be indexed were supplied")

    commit_spec = ["--all"] if index_all_commits else list(commits)

    repo = ctx.obj.get_repo(allowed_states=KartRepoState.ALL_STATES)
    update_spatial_tree(
        repo,
        commit_spec,
        verbosity=ctx.obj.verbosity + 1,
        clear_existing=clear_existing,
    )
