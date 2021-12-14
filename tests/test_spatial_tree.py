import pytest

from kart.sqlalchemy.sqlite import sqlite_engine
from kart.spatial_tree import encode_envelope, decode_envelope
from sqlalchemy.orm import sessionmaker

H = pytest.helpers.helpers()


@pytest.mark.parametrize(
    "envelope",
    [
        [0, 0, 0, 0],
        [1e-10, 1e-10, 1e-10, 1e-10],
        [-1e-10, -1e-10, -1e-10, -1e-10],
        [-180, 180, -90, 90],
        [0, 360, -10, 10],
        [90, 450, -20, 20],
        [180, 540, -30, 30],
        [-45.830, -43.232, 65.173, 65.745],
        [174.958, 174.992, -37.198, -37.190],
        [178.723, 185.234, 0.148, 2.538],
    ],
)
def test_roundtrip_envelope(envelope):
    roundtripped = decode_envelope(encode_envelope(envelope))
    assert roundtripped == pytest.approx(envelope, abs=1e-3)
    assert roundtripped[0] <= envelope[0]
    assert roundtripped[1] >= envelope[1]
    assert roundtripped[2] <= envelope[2]
    assert roundtripped[3] >= envelope[3]


def test_index_points_all(data_archive, cli_runner):
    # Indexing --all should give the same results every time.
    # For points, every point should have only one long S2 cell token.
    with data_archive("points.tgz") as repo_path:
        r = cli_runner.invoke(["spatial-tree", "index"])
        assert r.exit_code == 0, r.stderr

        s = _get_spatial_tree_summary(repo_path)
        assert s.features == 2148
        assert s.first_blob_id == "0075ca2608a7ea5a8883123d4767eb0056dc9fbe"
        assert s.first_envelope == pytest.approx(
            (174.3740, 174.3747, -35.8189, -35.8188), abs=1e-3
        )
        assert s.last_blob_id == "ffefdaa2170c33397e147d9c521dbd0e83362cfc"
        assert s.last_envelope == pytest.approx(
            (174.5168, 174.5174, -38.8996, -38.8994), abs=1e-3
        )


def test_index_points_commit_by_commit(data_archive, cli_runner):
    # Indexing one commit at a time should get the same results as indexing --all.
    with data_archive("points.tgz") as repo_path:
        r = cli_runner.invoke(["spatial-tree", "index", H.POINTS.HEAD1_SHA])
        assert r.exit_code == 0, r.stderr
        s = _get_spatial_tree_summary(repo_path)
        assert s.features == 2143

        r = cli_runner.invoke(["spatial-tree", "index", H.POINTS.HEAD_SHA])
        assert r.exit_code == 0, r.stderr

        s = _get_spatial_tree_summary(repo_path)
        assert s.features == 2148
        assert s.first_blob_id == "0075ca2608a7ea5a8883123d4767eb0056dc9fbe"
        assert s.first_envelope == pytest.approx(
            (174.3740, 174.3747, -35.8189, -35.8188), abs=1e-3
        )
        assert s.last_blob_id == "ffefdaa2170c33397e147d9c521dbd0e83362cfc"
        assert s.last_envelope == pytest.approx(
            (174.5168, 174.5174, -38.8996, -38.8994), abs=1e-3
        )


def test_index_points_idempotent(data_archive, cli_runner):
    # Indexing the commits one at a time and then indexing all commits again will also give the same result.
    # (We force everything to be indexed twice by deleting the record of whats been indexed).
    with data_archive("points.tgz") as repo_path:
        r = cli_runner.invoke(["spatial-tree", "index", H.POINTS.HEAD1_SHA])
        assert r.exit_code == 0, r.stderr
        s = _get_spatial_tree_summary(repo_path)
        assert s.features == 2143

        r = cli_runner.invoke(["spatial-tree", "index", H.POINTS.HEAD_SHA])
        assert r.exit_code == 0, r.stderr
        s = _get_spatial_tree_summary(repo_path)
        assert s.features == 2148

        # Trying to reindex shouldn't do anything since we remember where we are up to.
        r = cli_runner.invoke(["spatial-tree", "index"])
        assert r.exit_code == 0, r.stderr
        assert "Nothing to do" in r.stdout
        s = _get_spatial_tree_summary(repo_path)
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
        s = _get_spatial_tree_summary(repo_path)
        assert s.features == 2148
        assert s.first_blob_id == "0075ca2608a7ea5a8883123d4767eb0056dc9fbe"
        assert s.first_envelope == pytest.approx(
            (174.3740, 174.3747, -35.8189, -35.8188), abs=1e-3
        )
        assert s.last_blob_id == "ffefdaa2170c33397e147d9c521dbd0e83362cfc"
        assert s.last_envelope == pytest.approx(
            (174.5168, 174.5174, -38.8996, -38.8994), abs=1e-3
        )


def test_index_polygons_all(data_archive, cli_runner):
    with data_archive("polygons.tgz") as repo_path:
        r = cli_runner.invoke(["spatial-tree", "index"])
        assert r.exit_code == 0, r.stderr

        s = _get_spatial_tree_summary(repo_path)
        assert s.features == 228
        assert s.first_blob_id == "0299357eda50165abaec3c59b34334a02d4edbc6"
        assert s.first_envelope == pytest.approx(
            (175.3579, 175.3840, -37.8015, -37.7818), abs=1e-3
        )
        assert s.last_blob_id == "ff7dacd17bc855fdb29873dc25f5c3853bdfcf7f"
        assert s.last_envelope == pytest.approx(
            (172.5777, 172.5853, -43.3009, -43.2967), abs=1e-3
        )


def test_index_table_all(data_archive, cli_runner):
    with data_archive("table.tgz") as repo_path:
        r = cli_runner.invoke(["spatial-tree", "index"])
        assert r.exit_code == 0, r.stderr

        s = _get_spatial_tree_summary(repo_path)
        assert s.features == 0


def _get_spatial_tree_summary(repo_path):
    class Summary:
        pass

    s = Summary()

    db_path = repo_path / ".kart" / "feature_envelopes.db"
    engine = sqlite_engine(db_path)
    with sessionmaker(bind=engine)() as sess:
        s.features = sess.scalar("SELECT COUNT(*) FROM feature_envelopes;")

        if s.features:
            row = sess.execute(
                "SELECT blob_id, envelope FROM feature_envelopes ORDER BY blob_id LIMIT 1;"
            ).fetchone()
            s.first_blob_id = row[0].hex()
            s.first_envelope = decode_envelope(row[1])

            row = sess.execute(
                "SELECT blob_id, envelope FROM feature_envelopes ORDER BY blob_id DESC LIMIT 1;"
            ).fetchone()
            s.last_blob_id = row[0].hex()
            s.last_envelope = decode_envelope(row[1])

    return s
