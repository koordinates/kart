from dataclasses import dataclass
import pytest

from osgeo import osr

from kart.crs_util import make_crs
from kart.sqlalchemy.sqlite import sqlite_engine
from kart.spatial_filter.index import (
    EnvelopeEncoder,
    anticlockwise_ring_from_minmax_envelope,
    transform_minmax_envelope,
    union_of_envelopes,
    get_ogr_envelope,
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


def _check_envelope(roundtripped, original, abs=1e-3):
    assert roundtripped == pytest.approx(original, abs=abs)
    if original is not None:
        assert roundtripped[0] <= original[0]
        assert roundtripped[1] <= original[1]
        assert roundtripped[2] >= original[2]
        assert roundtripped[3] >= original[3]


def test_index_points_all(data_archive, cli_runner):
    # Indexing --all should give the same results every time.
    # For points, every point should have only one long S2 cell token.
    with data_archive("points.tgz") as repo_path:
        r = cli_runner.invoke(["spatial-filter", "index"])
        assert r.exit_code == 0, r.stderr
        s = _get_index_summary(repo_path)
        assert s.features == 2148
        _check_index(s, EXPECTED_POINTS_INDEX)


def test_index_points_commit_by_commit(data_archive, cli_runner):
    # Indexing one commit at a time should get the same results as indexing --all.
    with data_archive("points.tgz") as repo_path:
        r = cli_runner.invoke(["spatial-filter", "index", H.POINTS.HEAD1_SHA])
        assert r.exit_code == 0, r.stderr
        s = _get_index_summary(repo_path)
        assert s.features == 2143

        r = cli_runner.invoke(["spatial-filter", "index", H.POINTS.HEAD_SHA])
        assert r.exit_code == 0, r.stderr
        s = _get_index_summary(repo_path)
        assert s.features == 2148
        _check_index(s, EXPECTED_POINTS_INDEX)


def test_index_points_idempotent(data_archive, cli_runner):
    # Indexing the commits one at a time and then indexing all commits again will also give the same result.
    # (We force everything to be indexed twice by deleting the record of whats been indexed).
    with data_archive("points.tgz") as repo_path:
        r = cli_runner.invoke(["spatial-filter", "index", H.POINTS.HEAD1_SHA])
        assert r.exit_code == 0, r.stderr
        s = _get_index_summary(repo_path)
        assert s.features == 2143

        r = cli_runner.invoke(["spatial-filter", "index", H.POINTS.HEAD_SHA])
        assert r.exit_code == 0, r.stderr
        s = _get_index_summary(repo_path)
        assert s.features == 2148

        # Trying to reindex shouldn't do anything since we remember where we are up to.
        r = cli_runner.invoke(["spatial-filter", "index"])
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

        r = cli_runner.invoke(["spatial-filter", "index"])
        assert r.exit_code == 0, r.stderr
        assert "Nothing to do" not in r.stdout
        s = _get_index_summary(repo_path)
        assert s.features == 2148
        _check_index(s, EXPECTED_POINTS_INDEX)


def _check_index(actual, expected, abs=1e-3):
    assert actual.first_blob_id == expected.first_blob_id
    _check_envelope(actual.first_envelope, expected.first_envelope, abs=abs)
    assert actual.last_blob_id == expected.last_blob_id
    _check_envelope(actual.last_envelope, expected.last_envelope, abs=abs)


def test_index_polygons_all(data_archive, cli_runner):
    with data_archive("polygons.tgz") as repo_path:
        r = cli_runner.invoke(["spatial-filter", "index"])
        assert r.exit_code == 0, r.stderr

        s = _get_index_summary(repo_path)
        assert s.features == 228
        # The buffer-for-curvature (buffer added to compensate for possible curvature of line segments)
        # means the polygon index is not as accurate.
        _check_index(s, EXPECTED_POLYGONS_INDEX, 1e-2)


def test_index_table_all(data_archive, cli_runner):
    with data_archive("table.tgz") as repo_path:
        r = cli_runner.invoke(["spatial-filter", "index"])
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

# This transform tests the general case - like most transforms, the end result will be points with
# longitudes wrapped into the range [-180, 180]. It also is valid over an area that crosses the
# anti-meridian, so we can test that too.
NZTM = make_crs("EPSG:2193")
NZTM_TRANSFORM = osr.CoordinateTransformation(NZTM, EPSG_4326)


@pytest.mark.parametrize(
    "input,transform,expected_result",
    [
        ((1, 2, 3, 4), IDENTITY_TRANSFORM, (1, 2, 3, 4)),
        ((177, -10, 184, 10), IDENTITY_TRANSFORM, (177, -10, -176, 10)),
        ((185, 10, 190, 20), IDENTITY_TRANSFORM, (-175, 10, -170, 20)),
        ((-190, -20, -185, -10), IDENTITY_TRANSFORM, (170, -20, 175, -10)),
        ((1347679, 5456907, 2021026, 6117225), NZTM_TRANSFORM, (170, -41, 178, -35)),
        ((1347679, 5456907, 2532792, 5740668), NZTM_TRANSFORM, (170, -41, -176, -38)),
        ((2367133, 5308557, 2513805, 5517073), NZTM_TRANSFORM, (-178, -42, -176, -40)),
        # If the original geometry representation is split into two pieces by the anti-meridian,
        # we can't calculate a useful envelope from the original geometry's minmax-envelope:
        ((-179, -10, 179, 10), IDENTITY_TRANSFORM, None),
        # If the geometry's envelope is really wide (>180 degrees) then we can't necessarily
        # distinguish it from the above case except by inspecting geometry itself, instead of its
        # envelope, and even then we run into some ambiguities. But we've chosen to keep it simple
        # and just not index these geometries:
        ((-95, -10, 95, 10), IDENTITY_TRANSFORM, None),
        ((160, -10, 350, 10), IDENTITY_TRANSFORM, None),
        ((0, 1_000_000, 15_000_000, 1_100_000), NZTM_TRANSFORM, None),
    ],
)
def test_transform_minmax_envelope_area(input, transform, expected_result):
    # This first part of the test just tests our assumptions about transforms:
    # we need to be sure that the IDENTITY_TRANSFORM works differently to the other
    # transforms, so that we can be sure the code handles both possibilities properly.
    ring = anticlockwise_ring_from_minmax_envelope(input)
    ring.Transform(transform)
    x_values = [ring.GetPoint_2D(i)[0] for i in range(5)]

    if transform == IDENTITY_TRANSFORM:
        # IDENTITY_TRANSFORM leaves x-values as they are.
        # This means output values can be > 180 or < -180.
        assert set(x_values) == set([input[0], input[2]])
    else:
        # Any other transform will output x-values within this range,
        # which means detecting anti-meridian crossings works a little differently.
        assert all(-180 <= x <= 180 for x in x_values)

    # This is the actual test that the transform works.
    # We test it without buffer-for-curvature first since that has only one right answer, so we can check
    # that the logic is working accurately without giving a large amount of arbitrary leeway for buffering.
    actual_result = transform_minmax_envelope(
        input, transform, buffer_for_curvature=False
    )
    _check_envelope(actual_result, expected_result, abs=1e-5)

    # Now we test it with a buffer added. We just make sure it's roughly the same or larger - there's
    # a separate test where we specifically check that the buffering works when its needed.
    buffered_result = transform_minmax_envelope(
        input, transform, buffer_for_curvature=True
    )
    _check_envelope(buffered_result, expected_result, abs=0.2)


@pytest.mark.parametrize(
    "input,transform,expected_result",
    [
        ((1, 2, 1, 2), IDENTITY_TRANSFORM, (1, 2, 1, 2)),
        ((185, 85, 185, 85), IDENTITY_TRANSFORM, (-175, 85, -175, 85)),
        ((1347679, 5456907, 1347679, 5456907), NZTM_TRANSFORM, (170, -41, 170, -41)),
        ((2567196, 5736624, 2567196, 5736624), NZTM_TRANSFORM, (-176, -38, -176, -38)),
    ],
)
def test_transform_minmax_envelope_point(input, transform, expected_result):
    actual_result = transform_minmax_envelope(
        input, transform, buffer_for_curvature=False
    )
    assert actual_result == pytest.approx(expected_result, abs=1e-5)

    # For points, no buffer is added, since there can be no curved lines.
    buffered_result = transform_minmax_envelope(
        input, transform, buffer_for_curvature=True
    )
    assert buffered_result == pytest.approx(expected_result, abs=1e-5)
    assert buffered_result == actual_result


def test_transform_minmax_envelope_buffer_for_curvature():
    # This envelope is defined in NZTM. When coverted to EPSG:4326, this envelope's straight-line edges should be
    # curved, in theory. In practise, the way transforms work is by converting vertices only, and then assuming
    # straight lines between them all - hence the need to segmentise long straight lines so they have lots of
    # vertices which when transformed will approximate the curve.
    # But back to theory: when transformed to EPSG:4326, this rectangle will become 4 corners connected by 4 curves.
    # The corners, in anticlockwise order, are at (91.7, -76.7) -> (-105.0, -75.7) -> (-171.1, -26.2) -> (158.3, -26.3)
    # The first curve - from (91.7, -76.7) -> (-105.0, -75.7) is convex - it passes through (180, -87.9),
    # which is a long way south of any of the corner vertices. The other three curves are concave, and stay well within
    # the convex-hull described by the 4 conrner vertices - so it is the southernmost edge of the envelope where we
    # need to make sure we are segmenting and buffering conservatively enough.
    input = (120_000, 230_000, 3_200_000, 7_000_000)
    transform = NZTM_TRANSFORM

    transformed_no_buffer = transform_minmax_envelope(
        input, transform, buffer_for_curvature=False
    )

    # This mostly works but it has the wrong southern boundary due to not taking edge curvature into account:
    assert transformed_no_buffer == pytest.approx(
        (91.678236, -76.703577, -105.024329, -26.221904), abs=1e-5
    )

    # Performing the transform manually with a highly segmented rectangle that follows the envelope:
    ring = anticlockwise_ring_from_minmax_envelope(input)
    ring.Segmentize(100_000)  # Sements no more than 100km long.
    ring.Transform(NZTM_TRANSFORM)
    transformed_manually = get_ogr_envelope(ring)
    # This envelope's x-values are useless due to antimeridian issues, but it's y values are accurate.
    # Note that it's south border is outside the south border calculated above with buffer_for_curvature=False.
    assert transformed_manually == pytest.approx(
        (-179.900476, -87.956105, 179.105089, -26.221904), abs=1e-5
    )

    # This is the correct answer: it has the x-values from transformed_no_buffer, and the y_values
    # from the envelope we just transformed manually with highly segmented edges:
    correct_envelope = (91.678236, -87.956105, -105.024329, -26.221904)

    transformed_with_buffer = transform_minmax_envelope(
        input, transform, buffer_for_curvature=True
    )

    # Check that transformed_with_buffer is pretty close to correct, and it is no smaller than the correct envelope.
    _check_envelope(transformed_with_buffer, correct_envelope, abs=0.2)


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
