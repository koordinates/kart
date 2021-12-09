from dataclasses import dataclass
import pytest

from osgeo import osr

from kart.crs_util import make_crs
from kart.sqlalchemy.sqlite import sqlite_engine
from kart.spatial_tree import (
    EnvelopeEncoder,
    anticlockwise_ring_from_minmax_envelope,
    transform_minmax_envelope,
    union_of_envelopes,
)
from sqlalchemy.orm import sessionmaker

H = pytest.helpers.helpers()


@dataclass
class IndexSummary:
    features: int
    first_blob_id: str
    first_envelope: tuple
    last_blob_id: str
    last_envelope: tuple


EXPECTED_POINTS_INDEX = IndexSummary(
    2148,
    "0075ca2608a7ea5a8883123d4767eb0056dc9fbe",
    (174.37455884775878, -35.81883419068709, 174.37455884775878, -35.81883419068709),
    "ffefdaa2170c33397e147d9c521dbd0e83362cfc",
    (174.5172939396111, -38.899534524681165, 174.5172939396111, -38.899534524681165),
)


EXPECTED_POLYGONS_INDEX = IndexSummary(
    228,
    "0299357eda50165abaec3c59b34334a02d4edbc6",
    (175.3581076167, -37.8013959833, 175.38388545, -37.7817686333),
    "ff7dacd17bc855fdb29873dc25f5c3853bdfcf7f",
    (172.5777264167, -43.3007339833, 172.5845793667, -43.2968427333),
)


@pytest.mark.parametrize(
    "envelope,expected_encoded",
    [
        ((0, 0, 0, 0), b"\x7f\xff\xf7\xff\xff\x80\x00\x08\x00\x00"),
        ((1e-10, 1e-10, 1e-10, 1e-10), b"\x7f\xff\xf7\xff\xff\x80\x00\x08\x00\x00"),
        ((-1e-10, -1e-10, -1e-10, -1e-10), b"\x7f\xff\xf7\xff\xff\x80\x00\x08\x00\x00"),
        ((-180, -90, 180, 90), b"\x00\x00\x00\x00\x00\xff\xff\xff\xff\xff"),
        ((-90, -10, 90, 10), b"\x3f\xff\xf7\x1c\x71\xc0\x00\x08\xe3\x8e"),
        ((90, -20, -90, 20), b"\xbf\xff\xf6\x38\xe3\x40\x00\t\xc7\x1c"),
        (
            (-45.830, 65.173, -43.232, 65.745),
            b"\x5f\x68\xed\xcb\x0b\x61\x41\xed\xd8\x10",
        ),
        (
            (174.958, -37.198, 174.992, -37.190),
            b"\xfc\x6a\x14\xb1\x89\xfc\x70\x54\xb1\xb9",
        ),
        (
            (178.723, 0.148, -175.234, 2.538),
            b"\xff\x17\x78\x03\x5d\x03\x63\xa8\x39\xc1",
        ),
    ],
)
def test_roundtrip_envelope(envelope, expected_encoded):
    encoder = EnvelopeEncoder()
    actual_encoded = encoder.encode(envelope)
    assert actual_encoded == expected_encoded

    roundtripped = encoder.decode(actual_encoded)
    _check_envelope(roundtripped, envelope)


def _check_envelope(roundtripped, original):
    assert roundtripped == pytest.approx(original, abs=1e-3)
    assert roundtripped[0] <= original[0]
    assert roundtripped[1] <= original[1]
    assert roundtripped[2] >= original[2]
    assert roundtripped[3] >= original[3]


def test_index_points_all(data_archive, cli_runner):
    # Indexing --all should give the same results every time.
    # For points, every point should have only one long S2 cell token.
    with data_archive("points.tgz") as repo_path:
        r = cli_runner.invoke(["spatial-tree", "index"])
        assert r.exit_code == 0, r.stderr
        s = _get_index_summary(repo_path)
        assert s.features == 2148
        _check_index(s, EXPECTED_POINTS_INDEX)


def test_index_points_commit_by_commit(data_archive, cli_runner):
    # Indexing one commit at a time should get the same results as indexing --all.
    with data_archive("points.tgz") as repo_path:
        r = cli_runner.invoke(["spatial-tree", "index", H.POINTS.HEAD1_SHA])
        assert r.exit_code == 0, r.stderr
        s = _get_index_summary(repo_path)
        assert s.features == 2143

        r = cli_runner.invoke(["spatial-tree", "index", H.POINTS.HEAD_SHA])
        assert r.exit_code == 0, r.stderr
        s = _get_index_summary(repo_path)
        assert s.features == 2148
        _check_index(s, EXPECTED_POINTS_INDEX)


def test_index_points_idempotent(data_archive, cli_runner):
    # Indexing the commits one at a time and then indexing all commits again will also give the same result.
    # (We force everything to be indexed twice by deleting the record of whats been indexed).
    with data_archive("points.tgz") as repo_path:
        r = cli_runner.invoke(["spatial-tree", "index", H.POINTS.HEAD1_SHA])
        assert r.exit_code == 0, r.stderr
        s = _get_index_summary(repo_path)
        assert s.features == 2143

        r = cli_runner.invoke(["spatial-tree", "index", H.POINTS.HEAD_SHA])
        assert r.exit_code == 0, r.stderr
        s = _get_index_summary(repo_path)
        assert s.features == 2148

        # Trying to reindex shouldn't do anything since we remember where we are up to.
        r = cli_runner.invoke(["spatial-tree", "index"])
        assert r.exit_code == 0, r.stderr
        assert "Nothing to do" in r.stdout
        s = _get_index_summary(repo_path)
        assert s.features == 2148

        # Force reindex by deleting record of what's been indexed.
        # Even so, this should just rewrite the same index over the top of the old one.
        db_path = repo_path / ".kart" / "feature_envelopes.db"
        engine = sqlite_engine(db_path)
        with sessionmaker(bind=engine)() as sess:
            sess.execute("DELETE FROM commits;")

        r = cli_runner.invoke(["spatial-tree", "index"])
        assert r.exit_code == 0, r.stderr
        assert "Nothing to do" not in r.stdout
        s = _get_index_summary(repo_path)
        assert s.features == 2148
        _check_index(s, EXPECTED_POINTS_INDEX)


def _check_index(actual, expected):
    assert actual.first_blob_id == expected.first_blob_id
    _check_envelope(actual.first_envelope, expected.first_envelope)
    assert actual.last_blob_id == expected.last_blob_id
    _check_envelope(actual.last_envelope, expected.last_envelope)


def test_index_polygons_all(data_archive, cli_runner):
    with data_archive("polygons.tgz") as repo_path:
        r = cli_runner.invoke(["spatial-tree", "index"])
        assert r.exit_code == 0, r.stderr

        s = _get_index_summary(repo_path)
        assert s.features == 228
        _check_index(s, EXPECTED_POLYGONS_INDEX)


def test_index_table_all(data_archive, cli_runner):
    with data_archive("table.tgz") as repo_path:
        r = cli_runner.invoke(["spatial-tree", "index"])
        assert r.exit_code == 0, r.stderr

        s = _get_index_summary(repo_path)
        assert s.features == 0


def _get_index_summary(repo_path):
    db_path = repo_path / ".kart" / "feature_envelopes.db"
    engine = sqlite_engine(db_path)
    with sessionmaker(bind=engine)() as sess:
        features = sess.scalar("SELECT COUNT(*) FROM feature_envelopes;")

        if not features:
            return IndexSummary(features, None, None, None, None)

        encoder = EnvelopeEncoder()
        row = sess.execute(
            "SELECT blob_id, envelope FROM feature_envelopes ORDER BY blob_id LIMIT 1;"
        ).fetchone()
        first_blob_id = row[0].hex()
        first_envelope = encoder.decode(row[1])

        row = sess.execute(
            "SELECT blob_id, envelope FROM feature_envelopes ORDER BY blob_id DESC LIMIT 1;"
        ).fetchone()
        last_blob_id = row[0].hex()
        last_envelope = encoder.decode(row[1])

        return IndexSummary(
            features, first_blob_id, first_envelope, last_blob_id, last_envelope
        )


# This transform leaves every point exactly where it is, even points past the antimeridian eg (185, 0)
# We need to test this since its a special case - no other transform will result in longitudes outside
# the range [-180, 180].
EPSG_4326 = make_crs("EPSG:4326")
IDENTITY_TRANSFORM = osr.CoordinateTransformation(EPSG_4326, EPSG_4326)

# This transform leaves points more-or-less where they are, but it does ensure the output longitude
# values are -180 <= x <= 180, which is what most transforms will do, so this is helps test that
# we handle anti-meridians properly in the general case.
NZGD2000 = make_crs("EPSG:2193")
NZGD2000_TRANSFORM = osr.CoordinateTransformation(NZGD2000, EPSG_4326)


@pytest.mark.parametrize(
    "minmax_envelope,transform,expected_result",
    [
        ((1, 2, 3, 4), IDENTITY_TRANSFORM, (1, 2, 3, 4)),
        ((177, -10, 184, 10), IDENTITY_TRANSFORM, (177, -10, -176, 10)),
        ((185, 10, 190, 20), IDENTITY_TRANSFORM, (-175, 10, -170, 20)),
        ((-190, -20, -185, -10), IDENTITY_TRANSFORM, (170, -20, 175, -10)),
        (
            (1347679, 5456907, 2021025, 6117225),
            NZGD2000_TRANSFORM,
            (170, -41, 178, -35),
        ),
        (
            (1347679, 5456907, 2532792, 5740667),
            NZGD2000_TRANSFORM,
            (170, -41, -176, -38),
        ),
        (
            (2367133, 5308558, 2513805, 5517072),
            NZGD2000_TRANSFORM,
            (-178, -42, -176, -40),
        ),
    ],
)
def test_transform_raw_envelope(transform, minmax_envelope, expected_result):
    # This first part of the test just tests our assumptions about transforms:
    # we need to be sure that the IDENTITY_TRANSFORM works differently to the other
    # transforms, so that we can be sure the code handles both possibilities properly.
    ring = anticlockwise_ring_from_minmax_envelope(minmax_envelope)
    ring.Transform(transform)
    x_values = [ring.GetPoint_2D(i)[0] for i in range(5)]

    if transform == IDENTITY_TRANSFORM:
        # IDENTITY_TRANSFORM leaves x-values as they are.
        # This means output values can be > 180 or < -180.
        assert set(x_values) == set([minmax_envelope[0], minmax_envelope[2]])
    else:
        # Any other transform will output x-values within this range,
        # which means detecting anti-meridian crossings works a little differently.
        assert all(-180 <= x <= 180 for x in x_values)

    # This is the actual test that the transform works.
    actual_result = transform_minmax_envelope(minmax_envelope, transform)
    assert actual_result == pytest.approx(expected_result, abs=1e-5)


@pytest.mark.parametrize(
    "env1,env2,expected_result",
    [
        (None, None, None),
        ((1, 2, 3, 4), None, (1, 2, 3, 4)),
        (None, (1, 2, 3, 4), (1, 2, 3, 4)),
        ((1, 2, 3, 4), (5, 6, 7, 8), (1, 2, 7, 8)),
        ((3, 2, 7, 8), (1, 4, 5, 6), (1, 2, 7, 8)),
        ((-10, -1, 10, 1), (-1, -10, 1, 10), (-10, -10, 10, 10)),
        ((170, 2, 175, 4), (-165, 6, -160, 8), (170, 2, -160, 8)),
        ((0, 2, 10, 6), (170, 4, -150, 8), (170, 2, 10, 8)),
        ((0, 2, 10, 6), (160, 4, -160, 8), (0, 2, -160, 8)),
    ],
)
def test_union_of_envelopes(env1, env2, expected_result):
    actual_result = union_of_envelopes(env1, env2)
    assert actual_result == expected_result
