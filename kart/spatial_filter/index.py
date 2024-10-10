import functools
import logging
import math
import sys
import time

import click
import pygit2
from osgeo import ogr, osr
from pysqlite3 import dbapi2 as sqlite
from sqlalchemy import Column, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import BLOB

from kart.crs_util import make_crs, normalise_wkt
from kart.exceptions import InvalidOperation, SubprocessError
from kart.geometry import Geometry
from kart.repo import KartRepoFiles
from kart.rev_list_objects import rev_list_feature_blobs
from kart.serialise_util import msg_unpack
from kart.sqlalchemy import TableSet
from kart.sqlalchemy.sqlite import sqlite_engine
from kart.structs import CommitWithReference
from kart import subprocess_util as subprocess


L = logging.getLogger("kart.spatial_filter.index")


def buffered_bulk_warn(self, message, sample):
    """For logging lots of identical warnings, but only actually outputs once per time flush_bulk_warns is called."""
    self.bulk_warns.setdefault(message, 0)
    self.bulk_warns[message] = abs(self.bulk_warns[message]) + 1
    self.bulk_warn_samples[message] = sample


def flush_bulk_warns(self):
    """Output the number of occurrences of each type of buffered_bulk_warn message."""
    for message, occurrences in self.bulk_warns.items():
        if occurrences > 0:
            sample = self.bulk_warn_samples[message]
            self.warn(f"{message} ({occurrences} total occurences)\nSample: {sample}")
            self.bulk_warns[message] = -occurrences


L.buffered_bulk_warn = buffered_bulk_warn.__get__(L)
L.flush_bulk_warns = flush_bulk_warns.__get__(L)
L.bulk_warns = {}
L.bulk_warn_samples = {}


class CrsHelper:
    """
    Loads all CRS definitions for a particular dataset, and creates a set of transforms for each commit.
    The transforms for a dataset at a particular commit include the transform for the CRS at that commit,
    and the transform for each future commit. This is because a feature added at a particular commit has
    the current CRS applied to it, but may also later have a future CRS applied to it if that feature
    still exists when that CRS becomes current (and we do not check when individual features are deleted).
    The feature will not have a previous CRS applied to it, since that CRS was already removed before
    the feature was added (ie, we can be sure there is no overlap).
    """

    def __init__(self, repo, start_commits=None, stop_commits=None):
        self.repo = repo
        self.ds_to_transforms = {}
        self.target_crs = make_crs("EPSG:4326")
        self._distinct_crs_list = []
        if start_commits is not None:
            self.start_stop_spec = [*start_commits, "--not", *stop_commits]
        else:
            self.start_stop_spec = ["--all"]

    def transforms_for_dataset_at_commit(self, ds_path, commit_id, verbose=False):
        transforms = self.ds_to_transforms.get(ds_path)
        if transforms is None:
            transforms = self._load_transforms_for_dataset(ds_path, verbose=verbose)
        result = transforms[commit_id]
        if verbose:
            descs = [t.desc for t in result]
            click.echo(
                f"Applying the following CRS transforms for {ds_path} at commit {commit_id}: {', '.join(descs)}"
            )
        return result

    def _load_transforms_for_dataset(self, ds_path, verbose=False):
        if ds_path in self.ds_to_transforms:
            return self.ds_to_transforms[ds_path]

        seen_crs_oid_set = set()
        transform_set = set()
        transform_list = []
        commit_id_to_transform_list = {}

        for commit_id in self._all_commits():
            crs_tree = self._get_crs_tree_for_ds_at_commit(ds_path, commit_id)
            if crs_tree is not None and crs_tree.id.hex not in seen_crs_oid_set:
                seen_crs_oid_set.add(crs_tree.id.hex)
                for crs_blob in crs_tree:
                    crs_blob_oid = crs_blob.id.hex
                    if crs_blob.type_str != "blob" or crs_blob_oid in seen_crs_oid_set:
                        continue
                    seen_crs_oid_set.add(crs_blob_oid)
                    try:
                        crs = self.crs_from_oid(crs_blob.id.hex)
                        transform = self.transform_from_src_crs(crs)
                        if transform not in transform_set:
                            transform_set.add(transform)
                            transform_list = transform_list + [transform]
                    except Exception:
                        L.warning(
                            f"Couldn't load transform for CRS {crs_blob_oid} ({crs_blob.name} at {ds_path})",
                            exc_info=True,
                        )
            commit_id_to_transform_list[commit_id] = transform_list
            if verbose:
                descs = [t.desc for t in transform_list]
                trunc = _truncate_oid(self.repo)
                click.echo(
                    f"Transforms for {ds_path} at {commit_id[:trunc]}: {', '.join(descs)}"
                )

        descs = [t.desc for t in transform_list]
        info = click.echo if verbose else L.info
        info(f"Loaded CRS transforms for {ds_path}: {', '.join(descs)}")

        self.ds_to_transforms[ds_path] = commit_id_to_transform_list
        return commit_id_to_transform_list

    def _get_crs_tree_for_ds_at_commit(self, ds_path, commit_id):
        root_tree = self.repo[commit_id].peel(pygit2.Tree)
        result = self._safe_get_obj(root_tree, f"{ds_path}/.table-dataset/meta/crs/")
        if result is None:
            # Delete this fall-back if we drop Datasets V2 support.
            result = self._safe_get_obj(root_tree, f"{ds_path}/.sno-dataset/meta/crs/")
        return result

    def _safe_get_obj(self, root_tree, path):
        try:
            return root_tree / path
        except KeyError:
            return None

    @functools.lru_cache(maxsize=1)
    def _all_commits(self):
        cmd = [
            "git",
            "-C",
            self.repo.path,
            "rev-list",
            *self.start_stop_spec,
        ]
        try:
            commits = subprocess.check_output(cmd, encoding="utf8")
        except subprocess.CalledProcessError as e:
            raise SubprocessError(
                f"There was a problem with git rev-list: {e}", called_process_error=e
            )
        return commits.splitlines()

    @functools.lru_cache()
    def crs_from_oid(self, crs_oid):
        wkt = normalise_wkt(self.repo[crs_oid].data.decode("utf-8"))
        result = make_crs(wkt)
        for prior_result in self._distinct_crs_list:
            if result.IsSame(prior_result):
                return prior_result
        self._distinct_crs_list.append(result)
        return result

    @functools.lru_cache()
    def transform_from_src_crs(self, src_crs):
        transform = osr.CoordinateTransformation(src_crs, self.target_crs)
        if src_crs.IsSame(self.target_crs):
            desc = f"IDENTITY({src_crs.GetAuthorityCode(None)})"
        else:
            desc = f"{src_crs.GetAuthorityCode(None)} -> {self.target_crs.GetAuthorityCode(None)}"
        transform.desc = desc
        return transform


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


def _minimal_description_of_commit_set(repo, commits):
    """
    Returns the minimal set of commit IDs that have the same set of ancestors as
    the given set of commit IDs.
    Stated differently - returns the given commits except for those which are
    reachable by following ancestors of commits in the given set.
    """
    cmd = ["git", "-C", repo.path, "merge-base", "--independent"] + list(commits)
    try:
        r = subprocess.run(cmd, encoding="utf8", check=True, capture_output=True)
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


def update_spatial_filter_index(
    repo, commits, verbosity=1, clear_existing=False, dry_run=False
):
    """
    Index the commits given in commit_spec, and write them to the feature_envelopes.db repo file.

    repo - the Kart repo containing the commits to index, and in which to write the index file.
    commits - a set of commit IDs to index (ancestors of these are implicitly included).
    verbosity - how much non-essential information to output.
    clear_existing - when true, deletes any pre-existing data before re-indexing.
    """

    # This is needed to allow just-in-time fetching features that are outside the spatial filter,
    # but are needed by the client for some specific operation:
    if "uploadpack.allowAnySHA1InWant" not in repo.config:
        repo.config["uploadpack.allowAnySHA1InWant"] = True

    db_path = repo.gitdir_file(KartRepoFiles.FEATURE_ENVELOPES)
    engine = sqlite_engine(db_path)

    # Find out where we were up to last time, don't reindex anything that's already indexed.
    start_commits, stop_commits, all_independent_commits = _build_on_last_index(
        repo, commits, engine, clear_existing=clear_existing
    )

    crs_helper = CrsHelper(repo, start_commits, stop_commits)

    if not start_commits:
        click.echo("Nothing to do: index already up to date.")
        return

    feature_blob_iter = rev_list_feature_blobs(repo, start_commits, stop_commits)

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
    trunc = _truncate_oid(repo)

    # Using sqlite directly here instead of sqlalchemy is about 10x faster.
    # Possibly due to huge number of unbatched queries.
    # TODO - investigate further.
    db = sqlite.connect(f"file:{db_path}", uri=True)
    with db:
        dbcur = db.cursor()

        for i, (commit_id, path_match_result, feature_blob) in enumerate(
            feature_blob_iter
        ):
            if i and progress_every and i % progress_every == 0:
                click.echo(f"  {i:,d} features... @{time.monotonic()-t0:.1f}s")
                L.flush_bulk_warns()

            ds_path = path_match_result.group(1)
            transforms = crs_helper.transforms_for_dataset_at_commit(
                ds_path,
                commit_id,
            )
            if not transforms:
                continue
            geom = get_geometry(repo, feature_blob)
            if geom is None or geom.is_empty():
                continue
            feature_oid = feature_blob.id.hex
            feature_desc = f"{commit_id[:trunc]}:{ds_path}:{feature_oid[:trunc]}"
            envelope = get_envelope_for_indexing(geom, transforms, feature_desc)
            if envelope is None:
                continue

            params = (bytes.fromhex(feature_oid), encoder.encode(envelope))
            dbcur.execute(
                "INSERT OR REPLACE INTO feature_envelopes (blob_id, envelope) VALUES (?, ?);",
                params,
            )

        click.echo(f"  {i:,d} features... @{time.monotonic()-t0:.1f}s")
        L.flush_bulk_warns()

        # Update indexed commits.
        params = [(bytes.fromhex(commit_id),) for commit_id in all_independent_commits]
        dbcur.execute("DELETE FROM commits;")
        dbcur.executemany("INSERT INTO commits (commit_id) VALUES (?);", params)

    t1 = time.monotonic()
    click.echo(f"Indexed {i} features in {t1-t0:.1f}s")


def debug_index(repo, arg):
    """
    Use kart spatial-filter index --debug=OBJECT to learn more about how a particular object is being indexed.
    Usage:
        --debug=COMMIT:DATASET_PATH:FEATURE_OID
        --debug=COMMIT:DATASET_PATH:FEATURE_PRIMARY_KEY
        --debug=HEX_ENCODED_BINARY_ENVELOPE
        --debug=W,S,E,N  (4 floats)
    """
    from kart.promisor_utils import object_is_promised

    if ":" in arg:
        _debug_feature(repo, arg)
    elif "," in arg:
        _debug_envelope(arg)
    elif all(c in "0123456789abcdefABCDEF" for c in arg):
        try:
            _ = repo[arg]
        except KeyError as e:
            if object_is_promised(e):
                raise InvalidOperation("Can't index promised object")
            _debug_encoded_envelope(arg)
        else:
            _debug_feature(repo, arg)
    elif arg.startswith('b"') or arg.startswith("b'"):
        _debug_encoded_envelope(arg)
    else:
        raise click.UsageError(debug_index.__doc__)


def _debug_feature(repo, arg):
    from kart.promisor_utils import object_is_promised

    parts = arg.split(":", maxsplit=2)
    if len(parts) < 3:
        raise click.UsageError(
            "--debug=FEATURE_OID is not supported - try --debug=COMMIT:DATASET_PATH:FEATURE_OID"
        )

    commit_id, ds_path, pk = parts
    commit_id = repo[commit_id].peel(pygit2.Commit).id.hex
    ds = repo.datasets(commit_id)[ds_path]

    try:
        _ = repo[pk]
    except KeyError as e:
        if object_is_promised(e):
            raise InvalidOperation("Can't index promised object")
        path = ds.encode_pks_to_path(ds.schema.sanitise_pks(pk), relative=True)
        feature_oid = ds.get_blob_at(path).id.hex
    else:
        # Actually this is a feature_oid
        feature_oid = pk

    trunc = _truncate_oid(repo)
    feature_desc = f"{commit_id[:trunc]}:{ds_path}:{feature_oid[:trunc]}"
    click.echo(f"Feature {feature_desc}")

    crs_helper = CrsHelper(repo)
    transforms = crs_helper.transforms_for_dataset_at_commit(
        ds_path, commit_id, verbose=True
    )

    geometry = get_geometry(repo, repo[feature_oid])
    envelope = _get_envelope_for_indexing_verbose(geometry, transforms, feature_oid)

    if envelope is not None:
        click.echo()
        click.echo(f"Final envelope: {envelope}")
        _debug_envelope(envelope)


def _debug_envelope(arg):
    import binascii

    if isinstance(arg, str):
        envelope = [float(s) for s in arg.split(",")]
    else:
        envelope = arg
    assert len(envelope) == 4
    assert all(isinstance(p, float) for p in envelope)

    encoder = EnvelopeEncoder()
    encoded = encoder.encode(envelope)
    encoded_hex = binascii.hexlify(encoded).decode()
    roundtripped = encoder.decode(encoded)
    click.echo(f"Encoded as {encoded_hex}\t\t({encoded})")
    click.echo(f"(which decodes as {roundtripped})")


def _debug_encoded_envelope(arg):
    import ast
    import binascii

    if arg.startswith("b'") or arg.startswith('b"'):
        encoded = ast.literal_eval(arg)
    else:
        encoded = binascii.unhexlify(arg.encode())

    encoder = EnvelopeEncoder(len(encoded) * 8 // 4)
    encoded_hex = binascii.hexlify(encoded).decode()
    decoded = encoder.decode(encoded)

    click.echo(f"Encoded as {encoded_hex}\t\t({encoded})")
    click.echo(f"Which decodes as: {decoded}")


NO_GEOMETRY_COLUMN = object()


def get_geometry(repo, feature_blob):
    legend, fields = msg_unpack(memoryview(feature_blob))
    col_id = get_geometry.legend_to_col_id.get(legend)
    if col_id is None:
        col_id = _find_geometry_column(fields)
    if col_id is None:
        return None
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


def _truncate_oid(repo):
    try:
        return len(repo.head.peel(pygit2.Tree).short_id)
    except Exception:
        return None


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
        self.VALUE_MAX_INT = 2**self.BITS_PER_VALUE - 1
        self.ENVELOPE_MAX_INT = 2**self.BITS_PER_ENVELOPE - 1

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


def get_envelope_for_indexing(geom, transforms, feature_desc):
    """
    Returns an envelope in EPSG:4326 that contains the entire geometry. Tries all of the given transforms to convert
    to EPSG:4326 and returns an envelope containing all of the possibilities. This is so we can find all features that
    potentially intersect a region even if their CRS has changed at some point, so they could be in more than one place.
    The returned envelope is ordered (w, s, e, n), with longitudes in the range [-180, 180] and latitudes [-90, 90].
    It is always true that s <= n. Normally w <= e unless it crosses the anti-meridian, in which case e < w.
    If the envelope cannot be calculated efficiently or at all, None is returned - a None result can be treated as
    equivalent to [-180, -90, 90, 180].
    """

    result = None

    try:
        minmax_envelope = _transpose_gpkg_or_ogr_envelope(
            geom.envelope(only_2d=True, calculate_if_missing=True)
        )

        for transform in transforms:
            try:
                envelope = transform_minmax_envelope(minmax_envelope, transform)
            except CannotIndex as e:
                if isinstance(e, CannotIndexDueToWrongCrs) and len(transforms) > 1:
                    L.buffered_bulk_warn(
                        f"Skipped obviously bad transform {transform.desc}",
                        feature_desc,
                    )
                    continue
                L.buffered_bulk_warn("Skipped indexing feature", feature_desc)
                return None

            result = union_of_envelopes(result, envelope)

        if result is None:
            L.buffered_bulk_warn("Skipped indexing feature", feature_desc)
            return None

        if not _is_valid_envelope(result):
            L.buffered_bulk_warn(
                "Couldn't index feature - resulting envelope not valid", feature_desc
            )
            return None
        return result
    except Exception:
        L.warning("Couldn't index feature %s", feature_desc, exc_info=True)
        return None


def _is_valid_envelope(env):
    return (
        (-180 <= env[0] <= 180)
        and (-90 <= env[1] <= 90)
        and (-180 <= env[2] <= 180)
        and (-90 < env[3] <= 90)
        and (env[1] <= env[3])
    )


def _get_envelope_for_indexing_verbose(geom, transforms, feature_desc):
    # Keep in sync with get_envelope_for_indexing above. Lots of debug output added.
    result = None

    try:
        minmax_envelope = _transpose_gpkg_or_ogr_envelope(
            geom.envelope(only_2d=True, calculate_if_missing=True)
        )
        click.echo()
        click.echo(f"Geometry envelope: {minmax_envelope}")

        for transform in transforms:
            desc = getattr(transform, "desc") or str(transform)
            click.echo()
            click.echo(f"Applying transform {desc}...")

            try:
                first_envelope = transform_minmax_envelope(
                    minmax_envelope, transform, buffer_for_curvature=False
                )
                envelope = transform_minmax_envelope(
                    minmax_envelope, transform, buffer_for_curvature=True
                )

                if first_envelope and first_envelope != envelope:
                    click.echo(f"First attempt: {first_envelope}")
                    click.echo(f"With buffer-for-curvature: {envelope}")
                else:
                    click.echo(f"Result: {envelope}")
            except CannotIndex as e:
                click.echo(f"Transform resulted in bad envelope: {e.bad_envelope}")
                if isinstance(e, CannotIndexDueToWrongCrs) and len(transforms) > 1:
                    click.echo(
                        f"Skipped obviously wrong transform {transform.desc} for feature {feature_desc}"
                    )
                    continue
                click.echo(f"Skipped indexing feature {feature_desc}")
                return None

            result = union_of_envelopes(result, envelope)
            if result != envelope:
                click.echo(f"Total envelope so far: {result}")
        if not _is_valid_envelope(result):
            L.warning(
                "Couldn't index feature %s - resulting envelope not valid", feature_desc
            )
            return None
        return result
    except Exception:
        L.warning("Couldn't index feature %s", feature_desc, exc_info=True)
        return None


def _transpose_gpkg_or_ogr_envelope(envelope):
    """
    GPKG uses the envelope format (min-x, max-x, min-y, max-y). We use the envelope format (w, s, e, n).
    We transpose GPKG envelope to (min-x, min-y, max-x, max-y), so that it least it has the same axis-order as our
    format, and we handle anti-meridian issues seperately (see transform_minmax_envelope).
    """
    return envelope[0], envelope[2], envelope[1], envelope[3]


def get_ogr_envelope(ogr_geometry):
    """Returns the envelope of the given OGR geometry in (min-x, max-x, min-y, max-y) format."""
    return _transpose_gpkg_or_ogr_envelope(ogr_geometry.GetEnvelope())


class CannotIndex(Exception):
    """
    Raised if the transformed envelope fails the sanity check by being larger than the planet or not on the planet,
    but also if it is smaller than the planet but apparently wider than a hemisphere, which we can't interpret
    unambiguously - it may or may not cross the antimeridian.
    """

    def __init__(self, bad_envelope):
        self.bad_envelope = bad_envelope


class CannotIndexDueToWrongCrs(CannotIndex):
    """Raised if the transformed envelope fails the sanity check so spectacularly that it is clear the wrong CRS was used."""


def transform_minmax_envelope(envelope, transform, buffer_for_curvature=True):
    """
    Given an envelope in (min-x, min-y, max-x, max-y) format in any CRS, transforms it to EPSG:4326 using the given
    transform, then returns an axis-aligned envelope in EPSG:4326 in (w, s, e, n) order that bounds the original
    (but which may have a slightly larger area due to the axis-aligned edges not lining up with the original).
    The returned envelope has w <= e unless it crosses the antimeridian, in which case e < w.
    If buffer_for_curvature is True, the resulting envelope has a buffer-area added to all sides to ensure that
    not only the vertices, but also the curved edges of the original envelope are contained in the projected envelope.
    """
    # Handle points / envelopes with 0 area:
    if envelope[0] == envelope[2] and envelope[1] == envelope[3]:
        x, y, _ = transform.TransformPoint(envelope[0], envelope[1])
        x = _wrap_lon(x)
        result = (x, y, x, y)
        polarmost_y = abs(y)
        # See comments below in the general case:
        if polarmost_y > 1000:
            raise CannotIndexDueToWrongCrs(result)
        elif polarmost_y > 90:
            raise CannotIndex(result)
        return result

    ring = anticlockwise_ring_from_minmax_envelope(envelope)
    ring.Transform(transform)
    # At this point, depending on the transform used, the geometry could be in one piece, or it could be split into
    # two by the antimeridian - transforms almost always result in all longitude values being in the range [-180, 180].
    # We try to fix it up so that it's contiguous, which will mean that it has a useful min-max envelope.

    transformed_envelope = get_ogr_envelope(ring)
    width, height = _minmax_envelope_dimensions(transformed_envelope)
    split_x = None
    if width >= 180 and _is_clockwise(ring):
        # The ring was anticlockwise, but when projected and EPSG:4326 into the range [-180, 180] it became clockwise.
        # We need to try different interprerations of the ring until we find one where it is anticlockwise (this will
        # cross the meridian). Once we've found this interpretation, we can treat the min-x and max-x as w and e.
        split_x = _fix_ring_winding_order(ring)
        transformed_envelope = get_ogr_envelope(ring)
        width, height = _minmax_envelope_dimensions(transformed_envelope)

    polarmost_y = _max_abs_y(transformed_envelope)

    if width > 1000 or height > 1000 or polarmost_y > 1000:
        # If this happens its pretty certain that the wrong CRS has been used - this envelope is a lot larger than the
        # planet and/or a long way too far north or south to be on the planet. A threshold of 1000 is used since if
        # the envelope was only slightly larger than Earth or slightly too far north or south, then we can't be sure its
        # the wrong CRS - the data itself might just be slightly wrong.
        raise CannotIndexDueToWrongCrs(transformed_envelope)

    if width >= 180:
        # When this happens, it's likely because the original geometry crossed the antimeridian AND it was stored
        # in a non-contiguous way (ie in two halves, one near -180 and one near 180). If that happens, it means
        # the min-x and max-x values we got aren't useful for calculating the western- and eastern-most points -
        # they'll just be roughly -180 and 180. Rather than inspecting the original geometry to try and find
        # the true envelope, we just give up - raising CannotIndex is allowed if we can't easily calculate the envelope.
        # (It could also genuinely be a geometry wider than 180 degrees, but we can't easily tell the difference.
        # Or, it could be CRS issues again.)
        raise CannotIndex(transformed_envelope)

    if polarmost_y > 90:
        # Envelope extends too far north or south. This could be due to the wrong CRS or it could be bad data or
        # rounding errors.
        raise CannotIndex(transformed_envelope)

    if buffer_for_curvature:
        biggest_dimension = max(width, height)
        if biggest_dimension < 1.0:
            # Geometry is less than one degree by one degree - line curvature is minimal.
            # Add an extra 1/10th of envelope size to all edges.
            transformed_envelope = _buffer_minmax_envelope(
                transformed_envelope, 0.1 * biggest_dimension
            )
        else:
            # Redo some (but not all) of our calculations with a segmented envelope.
            # Envelope is segmented to ensure line segments don't span more than a degree.
            segments_per_side = max(10, math.ceil(biggest_dimension))
            ring = anticlockwise_ring_from_minmax_envelope(
                envelope, segments_per_side=segments_per_side
            )
            ring.Transform(transform)
            if split_x is not None:
                _reinterpret_to_be_east_of(split_x, ring)
            transformed_envelope = get_ogr_envelope(ring)
            # Add an extra 1/10th of a degree to all edges.
            transformed_envelope = _buffer_minmax_envelope(transformed_envelope, 0.1)

    w = _wrap_lon(transformed_envelope[0])
    s = _clamp_lat(transformed_envelope[1])
    e = _wrap_lon(transformed_envelope[2])
    n = _clamp_lat(transformed_envelope[3])
    return (w, s, e, n)


def anticlockwise_ring_from_minmax_envelope(envelope, segments_per_side=None):
    """Given an envelope in (min-x, min-y, max-x, max-y) format, builds an anticlockwise ring around it."""
    ring = ogr.Geometry(ogr.wkbLinearRing)
    # The envelope has the following format: min-x, min-y, max-x, max-ys.
    # We start at min-x, min-y and travel around it in an anti-clockwise direction:
    ring.AddPoint_2D(envelope[0], envelope[1])
    ring.AddPoint_2D(envelope[2], envelope[1])
    ring.AddPoint_2D(envelope[2], envelope[3])
    ring.AddPoint_2D(envelope[0], envelope[3])
    ring.AddPoint_2D(envelope[0], envelope[1])

    if segments_per_side is not None:
        width, height = _minmax_envelope_dimensions(envelope)
        larger_side = max(width, height)
        smaller_side = min(width, height)
        if smaller_side < larger_side / 4:
            segment_length = larger_side / segments_per_side
        else:
            segment_length = smaller_side / segments_per_side
        ring.Segmentize(segment_length)

    return ring


def _is_clockwise(ring):
    """
    Given a simple OGR ring, does a polygon area calculation to determine whether it is clockwise.
    The first and last point of the ring must be the same.
    For explanation see https://en.wikipedia.org/wiki/Shoelace_formula
    """
    result = 0
    for i in range(ring.GetPointCount() - 1):
        result += ring.GetX(i) * ring.GetY(i + 1) - ring.GetX(i + 1) * ring.GetY(i)
    return result < 0


def _is_anticlockwise(ring):
    return not _is_clockwise(ring)


def _fix_ring_winding_order(ring):
    """
    Given an OGR ring, shifts each point in turn eastwards by 360 degrees around the globe until the winding order
    is anticlockwise. This works on rings with any number of points, but has O(n^2) efficiency, so is best used on
    rectangles or other rings with few points. The first and last point of the ring must be the same.
    Returns an x point that all points were shifted to be east of, or None if no shifting was needed.
    """
    if _is_anticlockwise(ring):
        return None

    sorted_x_values = sorted(set(ring.GetX(i) for i in range(ring.GetPointCount())))
    split_x_options = (
        (sorted_x_values[i] + sorted_x_values[i + 1]) / 2
        for i in range(len(sorted_x_values) - 1)
    )
    for split_x in split_x_options:
        _reinterpret_to_be_east_of(split_x, ring)
        if _is_anticlockwise(ring):
            return split_x
    raise AssertionError("This should never happen")


def _reinterpret_to_be_east_of(split_x, ring):
    """
    Adds 360 degrees to all points that are east of the given X value. The resulting points will be in the same
    place on Earth, but this can change the winding order of the resulting polygon, and it can change which
    edges appear to cross the antimeridian.
    """
    for i in range(ring.GetPointCount()):
        if ring.GetX(i) < split_x:
            ring.SetPoint_2D(i, ring.GetX(i) + 360, ring.GetY(i))


def _buffer_minmax_envelope(envelope, buffer):
    """
    Adds a buffer onto all sides of an lat-lon envelope in the format (min-x, min-y, max-x, max-y).
    The buffer is in degrees latitude / longitude.
    """
    return (
        envelope[0] - buffer,
        max(envelope[1] - buffer, -90),
        envelope[2] + buffer,
        min(envelope[3] + buffer, 90),
    )


def _minmax_envelope_dimensions(envelope):
    """Returns (width, height) for an envelope in the format (min-x, min-y, max-x, max-y)."""
    return envelope[2] - envelope[0], envelope[3] - envelope[1]


def _max_abs_y(envelope):
    """Returns the greatest magnitude y value of the envelope (how far it extends away from the equator)."""
    return max(abs(envelope[1]), abs(envelope[3]))


def _unwrap_lon_envelope(w, e):
    """
    Given a longitude envelope in the format (w, e) where -180 <= w, e <= 180, and w <= e unless it crosses the
    antimeridian, in which case e < w:
    This returns an equivalent longitude range where w remains the same, and e exceeds w by the true size of the range.
    The result will follow these three rules: -180 <= w <= 180 and 0 <= (e - w) <= 360 and -180 <= e <= 540.
    """
    return (w, e) if w <= e else (w, e + 360)


def _wrap_lon(x):
    """Puts any longitude in the range -180 <= x < 180 without moving its position on earth."""
    return (x + 180) % 360 - 180


def _clamp_lat(y):
    """Clamps any latitude to the range -90 <= y <= 90. Use with care as this could hide problems eg CRS issues."""
    return max(-90, min(90, y))


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


def resolve_all_commit_refs(repo):
    """Returns the set of all branch heads, refs, HEAD, as commit SHAs."""
    cmd = ["git", "-C", repo.path, "show-ref", "--hash", "--head"]
    try:
        r = subprocess.run(cmd, encoding="utf8", check=True, capture_output=True)
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


def resolve_commits(repo, commitish_list):
    """Resolves the given strings into a set of commit SHAs."""
    return set(
        CommitWithReference.resolve(repo, commitish).id.hex
        for commitish in commitish_list
    )
