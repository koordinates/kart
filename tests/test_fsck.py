import pytest
from kart.repo import KartRepo


H = pytest.helpers.helpers()


def test_fsck(data_working_copy, cli_runner):
    with data_working_copy("points") as (repo_path, wc):
        repo = KartRepo(repo_path)

        r = cli_runner.invoke(["fsck"])
        assert r.exit_code == 0, r.stdout

        with repo.working_copy.tabular.session() as sess:
            assert H.row_count(sess, H.POINTS.LAYER) == H.POINTS.ROWCOUNT
            assert H.row_count(sess, "gpkg_kart_track") == 0

            # introduce a feature mismatch
            sess.execute(f"UPDATE {H.POINTS.LAYER} SET name='fred' WHERE fid=1;")
            sess.execute("""DELETE FROM "gpkg_kart_track" WHERE pk='1';""")

        r = cli_runner.invoke(["fsck"])
        assert r.exit_code == 1, r

        r = cli_runner.invoke(["fsck", "--reset-dataset=nz_pa_points_topo_150k"])
        assert r.exit_code == 0, r

        with repo.working_copy.tabular.session() as sess:
            assert H.row_count(sess, H.POINTS.LAYER) == H.POINTS.ROWCOUNT
            assert H.row_count(sess, "gpkg_kart_track") == 0

        r = cli_runner.invoke(["fsck"])
        assert r.exit_code == 0, r
