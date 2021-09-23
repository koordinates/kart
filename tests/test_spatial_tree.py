import pytest

from kart import is_windows, is_linux
from kart.sqlalchemy.sqlite import sqlite_engine
from sqlalchemy.orm import sessionmaker

H = pytest.helpers.helpers()

SKIP_REASON = "s2_py is not yet included in the kart windows build"


@pytest.mark.skipif(is_windows, reason=SKIP_REASON)
def test_index_points_all(data_archive, cli_runner):
    # Indexing --all should give the same results every time.
    # For points, every point should have only one long S2 cell token.
    with data_archive("points.tgz") as repo_path:
        r = cli_runner.invoke(["spatial-tree", "index", "--all"])
        assert r.exit_code == 0, r.stderr

        stats = _get_spatial_tree_stats(repo_path)
        assert stats.features == 2148
        assert stats.avg_cell_tokens_per_feature == 1
        assert stats.avg_cell_token_length == 16
        assert stats.distinct_cell_tokens == 2143


@pytest.mark.skipif(is_windows, reason=SKIP_REASON)
def test_index_points_commit_by_commit(data_archive, cli_runner):
    # Indexing one commit at a time should get the same results as indexing --all.
    with data_archive("points.tgz") as repo_path:
        r = cli_runner.invoke(["spatial-tree", "index", H.POINTS.HEAD1_SHA])
        assert r.exit_code == 0, r.stderr
        stats = _get_spatial_tree_stats(repo_path)
        assert stats.features == 2143

        r = cli_runner.invoke(
            ["spatial-tree", "index", H.POINTS.HEAD_SHA, "^" + H.POINTS.HEAD1_SHA]
        )
        assert r.exit_code == 0, r.stderr

        stats = _get_spatial_tree_stats(repo_path)
        assert stats.features == 2148
        assert stats.avg_cell_tokens_per_feature == 1
        assert stats.avg_cell_token_length == 16
        assert stats.distinct_cell_tokens == 2143


@pytest.mark.skipif(is_windows, reason=SKIP_REASON)
def test_index_points_idempotent(data_archive, cli_runner):
    # Indexing the commits one at a time (and backwards) and then indexing --all should
    # also give the same result, even though everything will have been indexed twice.
    with data_archive("points.tgz") as repo_path:
        r = cli_runner.invoke(
            ["spatial-tree", "index", H.POINTS.HEAD_SHA, "^" + H.POINTS.HEAD1_SHA]
        )
        assert r.exit_code == 0, r.stderr
        stats = _get_spatial_tree_stats(repo_path)
        assert stats.features == 5

        r = cli_runner.invoke(["spatial-tree", "index", H.POINTS.HEAD1_SHA])
        assert r.exit_code == 0, r.stderr
        stats = _get_spatial_tree_stats(repo_path)
        assert stats.features == 2148

        r = cli_runner.invoke(["spatial-tree", "index", "--all"])
        assert r.exit_code == 0, r.stderr
        stats = _get_spatial_tree_stats(repo_path)
        assert stats.features == 2148
        assert stats.avg_cell_tokens_per_feature == 1
        assert stats.avg_cell_token_length == 16
        assert stats.distinct_cell_tokens == 2143


@pytest.mark.skipif(is_windows, reason=SKIP_REASON)
def test_index_polygons_all(data_archive, cli_runner):
    # FIXME: These results shouldn't be different on macos and linux.
    # Dig into why they are different.
    with data_archive("polygons.tgz") as repo_path:
        r = cli_runner.invoke(["spatial-tree", "index", "--all"])
        assert r.exit_code == 0, r.stderr

        stats = _get_spatial_tree_stats(repo_path)
        assert stats.features == 228
        assert stats.avg_cell_tokens_per_feature == pytest.approx(
            7.276 if is_linux else 7.232, abs=0.1
        )
        assert stats.avg_cell_token_length == pytest.approx(8.066, abs=0.1)
        assert stats.distinct_cell_tokens == 1370 if is_linux else 1360


@pytest.mark.skipif(is_windows, reason=SKIP_REASON)
def test_index_table_all(data_archive, cli_runner):
    with data_archive("table.tgz") as repo_path:
        r = cli_runner.invoke(["spatial-tree", "index", "--all"])
        assert r.exit_code == 0, r.stderr

        stats = _get_spatial_tree_stats(repo_path)
        assert stats.features == 0
        assert stats.cell_tokens == 0


def _get_spatial_tree_stats(repo_path):
    class Stats:
        pass

    stats = Stats()

    db_path = repo_path / ".kart" / "s2_index.db"
    engine = sqlite_engine(db_path)
    with sessionmaker(bind=engine)() as sess:
        orphans = sess.execute(
            """
            SELECT blob_rowid FROM blob_cells
            EXCEPT SELECT rowid FROM blobs;
            """
        )
        assert orphans.first() is None

        stats.features = sess.scalar("SELECT COUNT(*) FROM blobs;")
        stats.cell_tokens = sess.scalar("SELECT COUNT(*) FROM blob_cells;")

        if stats.features:
            stats.avg_cell_tokens_per_feature = stats.cell_tokens / stats.features

        if stats.cell_tokens:
            stats.avg_cell_token_length = sess.scalar(
                "SELECT AVG(LENGTH(cell_token)) FROM blob_cells;"
            )
            stats.distinct_cell_tokens = sess.scalar(
                "SELECT COUNT (DISTINCT cell_token) FROM blob_cells;"
            )

    return stats
