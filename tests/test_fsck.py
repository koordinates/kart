import pytest
from sno.sqlalchemy import gpkg_engine


H = pytest.helpers.helpers()


def test_fsck(data_working_copy, cli_runner):
    with data_working_copy("points") as (repo, wc):
        engine = gpkg_engine(wc)

        r = cli_runner.invoke(["fsck"])
        assert r.exit_code == 0, r.stdout

        with engine.connect() as conn:
            assert H.row_count(conn, H.POINTS.LAYER) == H.POINTS.ROWCOUNT
            assert H.row_count(conn, "gpkg_sno_track") == 0

            # introduce a feature mismatch
            conn.execute(f"UPDATE {H.POINTS.LAYER} SET name='fred' WHERE fid=1;")
            conn.execute("""DELETE FROM "gpkg_sno_track" WHERE pk='1';""")

        r = cli_runner.invoke(["fsck"])
        assert r.exit_code == 1, r

        r = cli_runner.invoke(["fsck", "--reset-dataset=nz_pa_points_topo_150k"])
        assert r.exit_code == 0, r

        with engine.connect() as conn:
            assert H.row_count(conn, H.POINTS.LAYER) == H.POINTS.ROWCOUNT
            assert H.row_count(conn, "gpkg_sno_track") == 0

        r = cli_runner.invoke(["fsck"])
        assert r.exit_code == 0, r
