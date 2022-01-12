import binascii
from dataclasses import dataclass
import pytest

from osgeo import osr

from kart.crs_util import make_crs
from kart.sqlalchemy.sqlite import sqlite_engine
from kart.spatial_filter.index import (
    CannotIndex,
    EnvelopeEncoder,
    anticlockwise_ring_from_minmax_envelope,
    transform_minmax_envelope,
    union_of_envelopes,
    get_ogr_envelope,
)
from sqlalchemy.orm import sessionmaker

H = pytest.helpers.helpers()


@dataclass
class Entry:
    blob_id: str
    envelope: tuple


@dataclass
class IndexSummary:
    features: int
    first_blob_id: Entry
    last_blob_id: Entry
    westernmost: Entry
    southernmost: Entry
    easternmost: Entry
    northernmost: Entry
    widest: Entry


EXPECTED_POINTS_INDEX = IndexSummary(
    features=2148,
    first_blob_id=Entry(
        blob_id='0075ca2608a7ea5a8883123d4767eb0056dc9fbe',
        envelope=(174.37455885, -35.81883419, 174.37455885, -35.81883419),
    ),
    last_blob_id=Entry(
        blob_id='ffefdaa2170c33397e147d9c521dbd0e83362cfc',
        envelope=(174.51729394, -38.89953452, 174.51729394, -38.89953452),
    ),
    westernmost=Entry(
        blob_id='ea098c7b7bbbb57d5069bbfefe332300bc5af316',
        envelope=(170.61676942, -45.73477461, 170.61676942, -45.73477461),
    ),
    southernmost=Entry(
        blob_id='ea098c7b7bbbb57d5069bbfefe332300bc5af316',
        envelope=(170.61676942, -45.73477461, 170.61676942, -45.73477461),
    ),
    easternmost=Entry(
        blob_id='6523dde7f3b2172c6090563d9e99b32918703017',
        envelope=(178.43023198, -37.64119695, 178.43023198, -37.64119695),
    ),
    northernmost=Entry(
        blob_id='81e591a2e7c4985e2b82b6ef3e74a3a1b298e472',
        envelope=(172.99773191, -34.40609417, 172.99773191, -34.40609417),
    ),
    widest=None,
)

EXPECTED_POLYGONS_INDEX = IndexSummary(
    features=228,
    first_blob_id=Entry(
        blob_id='0299357eda50165abaec3c59b34334a02d4edbc6',
        envelope=(175.35810762, -37.80139598, 175.38388545, -37.78176863),
    ),
    last_blob_id=Entry(
        blob_id='ff7dacd17bc855fdb29873dc25f5c3853bdfcf7f',
        envelope=(172.57772642, -43.30073398, 172.58457937, -43.29684273),
    ),
    westernmost=Entry(
        blob_id='091eb2f16039471a6cc15adb8ae1fd4218ec751d',
        envelope=(172.31957762, -43.59000687, 172.38531723, -43.55415243),
    ),
    southernmost=Entry(
        blob_id='03f318186b6d7eef401c11465c443e7054e52123',
        envelope=(172.36927828, -43.63570298, 172.39867563, -43.61338377),
    ),
    easternmost=Entry(
        blob_id='ad9512b11f524e36237128fedd58f9ef71d07063',
        envelope=(176.95962540, -37.95628630, 176.98604958, -37.93726660),
    ),
    northernmost=Entry(
        blob_id='b8a2aed7d91fa91ad5aea47e3d0dea38027a4266',
        envelope=(174.27367710, -35.70618512, 174.29895293, -35.68906918),
    ),
    widest=Entry(
        blob_id='c150c29a1606f9b3d6ad572d7f4bdf5352cc0d70',
        envelope=(175.17343175, -37.93850625, 175.30657743, -37.89814838),
    ),
)

EXPECTED_ANTIMERIDIAN_3994_INDEX = IndexSummary(
    features=616,
    first_blob_id=Entry(
        blob_id='0008f607b7bb404c9d2d73e7377e7d10c5d04a6a',
        envelope=(-164.12204700, -14.00730160, -164.01131600, -13.89563250),
    ),
    last_blob_id=Entry(
        blob_id='fe5af2ef96140331795504608fa7e22c4c18432b',
        envelope=(-156.83539010, -17.26248740, -155.63135470, -17.08319900),
    ),
    westernmost=Entry(
        blob_id='8b7f0d336356de4418274fc6c4e22235cca6a481',
        envelope=(161.73579840, -41.96151070, 161.95505280, -41.62362810),
    ),
    southernmost=Entry(
        blob_id='88a069204c1c0f763aef1ec283a12404f927a6c6',
        envelope=(-175.33739650, -67.52500030, -175.12571240, -67.44136820),
    ),
    easternmost=Entry(
        blob_id='38939239f7995decd10d082e8cc27e2e3c3f2b25',
        envelope=(-148.19130080, -37.97096460, -143.83333330, -34.37675740),
    ),
    northernmost=Entry(
        blob_id='13718b1889a591319c1f72e93b6c2412890b5026',
        envelope=(-160.57510810, -17.34120320, -153.50000000, -7.50000000),
    ),
    widest=Entry(
        blob_id='3d7f1d09b02d3e3df1dc22777193535ebc38d0c1',
        envelope=(-162.10558360, -42.50000000, -148.61019660, -36.61721010),
    ),
)


EXPECTED_ANTIMERIDIAN_3832_INDEX = IndexSummary(
    features=616,
    first_blob_id=Entry(
        blob_id='00178a825ca904cd73fc144719205d523ee8bcbe',
        envelope=(-153.75143160, -9.77738980, -153.50000000, -9.47359470),
    ),
    last_blob_id=Entry(
        blob_id='fe9ccd848e82e73eddfc765a7dee93e2f533e719',
        envelope=(-171.45524940, -27.56882660, -170.74683640, -26.80289790),
    ),
    westernmost=Entry(
        blob_id='9162f81b135232560874e4a095ce7718d9293f5b',
        envelope=EXPECTED_ANTIMERIDIAN_3994_INDEX.westernmost.envelope,
    ),
    southernmost=Entry(
        blob_id='d0d91483159910ac42f791d0cf73d1e799c81144',
        envelope=EXPECTED_ANTIMERIDIAN_3994_INDEX.southernmost.envelope,
    ),
    easternmost=Entry(
        blob_id='8b6dfa16dbcd51639e417e756a324c9ce865a845',
        envelope=EXPECTED_ANTIMERIDIAN_3994_INDEX.easternmost.envelope,
    ),
    northernmost=Entry(
        blob_id='590f40c2aeeec46832ea23cf94cbf563b774245c',
        envelope=EXPECTED_ANTIMERIDIAN_3994_INDEX.northernmost.envelope,
    ),
    widest=Entry(
        blob_id='1347b4ea194e641c5ea745ca84e38ade9a82de0b',
        envelope=EXPECTED_ANTIMERIDIAN_3994_INDEX.widest.envelope,
    ),
)


def _check_index(actual, expected, abs=1e-3, widest_abs=None):
    assert actual.features == expected.features
    _check_entry(actual.first_blob_id, expected.first_blob_id, abs=abs)
    _check_entry(actual.last_blob_id, expected.last_blob_id, abs=abs)
    _check_entry(actual.westernmost, expected.westernmost, abs=abs)
    _check_entry(actual.southernmost, expected.southernmost, abs=abs)
    _check_entry(actual.easternmost, expected.easternmost, abs=abs)
    _check_entry(actual.northernmost, expected.northernmost, abs=abs)
    if expected.widest is not None:
        _check_entry(actual.widest, expected.widest, abs=(widest_abs or abs))


def _check_entry(actual, expected, abs=abs):
    assert actual.blob_id == expected.blob_id
    _check_envelope(actual.envelope, expected.envelope, abs=abs)


def _check_envelope(roundtripped, original, abs=1e-3):
    assert roundtripped == pytest.approx(original, abs=abs)
    if original is not None:
        assert roundtripped[0] <= original[0]
        assert roundtripped[1] <= original[1]
        assert roundtripped[2] >= original[2]
        assert roundtripped[3] >= original[3]


@pytest.mark.parametrize(
    "envelope,expected_encoded_hex",
    [
        ((0, 0, 0, 0), b"7ffff7ffff8000080000"),
        ((1e-10, 1e-10, 1e-10, 1e-10), b"7ffff7ffff8000080000"),
        ((-1e-10, -1e-10, -1e-10, -1e-10), b"7ffff7ffff8000080000"),
        ((-180, -90, 180, 90), b"0000000000ffffffffff"),
        ((-90, -10, 90, 10), b"3ffff71c71c00008e38e"),
        ((90, -20, -90, 20), b"bffff638e3400009c71c"),
        (
            (-45.830, 65.173, -43.232, 65.745),
            b"5f68edcb0b6141edd810",
        ),
        (
            (174.958, -37.198, 174.992, -37.190),
            b"fc6a14b189fc7054b1b9",
        ),
        (
            (178.723, 0.148, -175.234, 2.538),
            b"ff1778035d0363a839c1",
        ),
    ],
)
def test_roundtrip_envelope(envelope, expected_encoded_hex):
    expected_encoded = binascii.unhexlify(expected_encoded_hex)
    encoder = EnvelopeEncoder()
    actual_encoded = encoder.encode(envelope)
    assert actual_encoded == expected_encoded

    roundtripped = encoder.decode(actual_encoded)
    _check_envelope(roundtripped, envelope)


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


def test_index_polygons_all(data_archive, cli_runner):
    with data_archive("polygons.tgz") as repo_path:
        r = cli_runner.invoke(["spatial-filter", "index"])
        assert r.exit_code == 0, r.stderr

        s = _get_index_summary(repo_path)
        assert s.features == 228
        # The buffer-for-curvature (buffer added to compensate for possible curvature of line segments)
        # means the polygon index is not quite as accurate - and a lot less accurate for the widest polygon.
        _check_index(s, EXPECTED_POLYGONS_INDEX, 1e-2, widest_abs=0.2)


def test_index_table_all(data_archive, cli_runner):
    with data_archive("table.tgz") as repo_path:
        r = cli_runner.invoke(["spatial-filter", "index"])
        assert r.exit_code == 0, r.stderr

        s = _get_index_summary(repo_path)
        assert s.features == 0


def test_index_antimeridian_3994(data_archive, cli_runner):
    with data_archive("antimeridian-3994.tgz") as repo_path:
        r = cli_runner.invoke(["spatial-filter", "index"])
        assert r.exit_code == 0, r.stderr

        s = _get_index_summary(repo_path, unwrap_lon=0.0)
        assert s.features == 616
        # The buffer-for-curvature (buffer added to compensate for possible curvature of line segments)
        # means the antimeridian index is not as accurate (since each features has a large envelope).
        _check_index(s, EXPECTED_ANTIMERIDIAN_3994_INDEX, 0.2)


def test_index_antimeridian_3882(data_archive, cli_runner):
    with data_archive("antimeridian-3832.tgz") as repo_path:
        r = cli_runner.invoke(["spatial-filter", "index"])
        assert r.exit_code == 0, r.stderr

        s = _get_index_summary(repo_path, unwrap_lon=0.0)
        assert s.features == 616
        # The buffer-for-curvature (buffer added to compensate for possible curvature of line segments)
        # means the antimeridian index is not as accurate (since each features has a large envelope).
        _check_index(s, EXPECTED_ANTIMERIDIAN_3832_INDEX, 0.2)


def _get_index_summary(repo_path, unwrap_lon=-180):
    db_path = repo_path / ".kart" / "feature_envelopes.db"
    engine = sqlite_engine(db_path)
    with sessionmaker(bind=engine)() as sess:
        features = sess.scalar("SELECT COUNT(*) FROM feature_envelopes;")

        if not features:
            return IndexSummary(features, *([None] * 7))

        first_blob_id = lambda blob_id, envelope: -int(blob_id, 16)
        last_blob_id = lambda blob_id, envelope: int(blob_id, 16)

        def westernmost(blob_id, envelope):
            return -(envelope[0] + 360 if envelope[0] < unwrap_lon else envelope[0])

        def southernmost(blob_id, envelope):
            return -envelope[1]

        def easternmost(blob_id, envelope):
            return envelope[2] + 360 if envelope[2] < unwrap_lon else envelope[2]

        def northernmost(blob_id, envelope):
            return envelope[3]

        def widest(blob_id, envelope):
            return (
                envelope[2] - envelope[0]
                if envelope[2] >= envelope[0]
                else envelope[2] + 360 - envelope[0]
            )

        score_funcs = [
            first_blob_id,
            last_blob_id,
            westernmost,
            southernmost,
            easternmost,
            northernmost,
            widest,
        ]

        winners = [None] * len(score_funcs)
        winning_scores = [-float('INF')] * len(score_funcs)

        encoder = EnvelopeEncoder()
        r = sess.execute("SELECT blob_id, envelope FROM feature_envelopes;")
        for row in r:
            blob_id = row[0].hex()
            envelope = encoder.decode(row[1])
            for i in range(len(score_funcs)):
                score = score_funcs[i](blob_id, envelope)
                if score > winning_scores[i]:
                    winning_scores[i] = score
                    winners[i] = Entry(blob_id, envelope)

        return IndexSummary(features, *winners)


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

    if expected_result is None:
        with pytest.raises(CannotIndex):
            transform_minmax_envelope(input, transform)
        return

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
