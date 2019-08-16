import pytest


H = pytest.helpers.helpers()


@pytest.xfail("Needs rewritten to deal with WorkingCopy abstraction")
def test_fsck(data_working_copy, geopackage, cli_runner):
    with data_working_copy("points.snow") as (repo, wc):
        db = geopackage(wc)

        r = cli_runner.invoke(["fsck"])
        assert r.exit_code == 0, r

        # introduce a feature mismatch
        assert (
            db.execute(f"SELECT COUNT(*) FROM {H.POINTS_LAYER};").fetchone()[0] == 2143
        )
        assert db.execute(f"SELECT COUNT(*) FROM __kxg_map;").fetchone()[0] == 2143

        with db:
            db.execute(f"UPDATE {H.POINTS_LAYER} SET name='fred' WHERE fid=1;")
            db.execute("UPDATE __kxg_map SET state=0 WHERE feature_id=1;")

        assert (
            db.execute(f"SELECT COUNT(*) FROM {H.POINTS_LAYER};").fetchone()[0] == 2143
        )
        assert db.execute(f"SELECT COUNT(*) FROM __kxg_map;").fetchone()[0] == 2143

        r = cli_runner.invoke(["fsck"])
        assert r.exit_code == 1, r

        r = cli_runner.invoke(["fsck", "--reset-layer"])
        assert r.exit_code == 0, r

        assert (
            db.execute(f"SELECT COUNT(*) FROM {H.POINTS_LAYER};").fetchone()[0] == 2143
        )
        assert db.execute(f"SELECT COUNT(*) FROM __kxg_map;").fetchone()[0] == 2143

        r = cli_runner.invoke(["fsck"])
        assert r.exit_code == 0, r
