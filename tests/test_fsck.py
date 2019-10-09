import pytest


H = pytest.helpers.helpers()


@pytest.mark.xfail(reason="Needs rewritten to deal with WorkingCopy abstraction")
def test_fsck(data_working_copy, geopackage, cli_runner):
    with data_working_copy("points") as (repo, wc):
        db = geopackage(wc)

        r = cli_runner.invoke(["fsck"])
        assert r.exit_code == 0, r

        # introduce a feature mismatch
        assert H.row_count(db, H.POINTS_LAYER) == H.POINTS_ROWCOUNT
        assert H.row_count(db, '__kxg_map') == H.POINTS_ROWCOUNT

        with db:
            db.execute(f"UPDATE {H.POINTS_LAYER} SET name='fred' WHERE fid=1;")
            db.execute("UPDATE __kxg_map SET state=0 WHERE feature_id=1;")

        assert H.row_count(db, H.POINTS_LAYER) == H.POINTS_ROWCOUNT
        assert H.row_count(db, '__kxg_map') == H.POINTS_ROWCOUNT

        r = cli_runner.invoke(["fsck"])
        assert r.exit_code == 1, r

        r = cli_runner.invoke(["fsck", "--reset-layer"])
        assert r.exit_code == 0, r

        assert H.row_count(db, H.POINTS_LAYER) == H.POINTS_ROWCOUNT
        assert H.row_count(db, '__kxg_map') == H.POINTS_ROWCOUNT

        r = cli_runner.invoke(["fsck"])
        assert r.exit_code == 0, r
