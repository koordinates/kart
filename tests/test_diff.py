import pytest


H = pytest.helpers.helpers()


def test_diff_points(data_working_copy, geopackage, cli_runner):
    """ diff the working copy against the repository (no index!) """
    with data_working_copy("points") as (repo, wc):
        # empty
        r = cli_runner.invoke(["diff"])
        assert r.exit_code == 0, r
        # assert r.stdout.splitlines() == []

        # make some changes
        db = geopackage(wc)
        with db:
            cur = db.cursor()

            cur.execute(H.POINTS_INSERT, H.POINTS_RECORD)
            assert cur.rowcount == 1
            cur.execute(f"UPDATE {H.POINTS_LAYER} SET fid=9998 WHERE fid=1;")
            assert cur.rowcount == 1
            cur.execute(
                f"UPDATE {H.POINTS_LAYER} SET name='test', t50_fid=NULL WHERE fid=2;"
            )
            assert cur.rowcount == 1
            cur.execute(f"DELETE FROM {H.POINTS_LAYER} WHERE fid=3;")
            assert cur.rowcount == 1

        r = cli_runner.invoke(["diff"])
        assert r.exit_code == 0, r
        print("STDOUT", repr(r.stdout))
        assert r.stdout.splitlines() == [
            "--- nz_pa_points_topo_150k:fid=3",
            "-                                     geom = POINT(...)",
            "-                               macronated = N",
            "-                                     name = Tauwhare Pa",
            "-                               name_ascii = Tauwhare Pa",
            "-                                  t50_fid = 2426273",
            "+++ nz_pa_points_topo_150k:fid=9999",
            "+                                     geom = POINT(...)",
            "+                               macronated = 0",
            "+                                     name = Te Motu-a-kore",
            "+                               name_ascii = Te Motu-a-kore",
            "+                                  t50_fid = 9999999",
            "--- nz_pa_points_topo_150k:fid=2",
            "+++ nz_pa_points_topo_150k:fid=2",
            "-                                     name = ␀",
            "+                                     name = test",
            "-                                  t50_fid = 2426272",
            "+                                  t50_fid = ␀",
            "--- nz_pa_points_topo_150k:fid=1",
            "+++ nz_pa_points_topo_150k:fid=9998",
        ]


def test_diff_polygons(data_working_copy, geopackage, cli_runner):
    """ diff the working copy against the repository (no index!) """
    with data_working_copy("polygons") as (repo, wc):
        # empty
        r = cli_runner.invoke(["diff"])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines() == []

        # make some changes
        db = geopackage(wc)
        with db:
            cur = db.cursor()

            cur.execute(H.POLYGONS_INSERT, H.POLYGONS_RECORD)
            assert cur.rowcount == 1
            cur.execute(f"UPDATE {H.POLYGONS_LAYER} SET id=9998 WHERE id=1424927;")
            assert cur.rowcount == 1
            cur.execute(
                f"UPDATE {H.POLYGONS_LAYER} SET survey_reference='test', date_adjusted='2019-01-01T00:00:00Z' WHERE id=1443053;"
            )
            assert cur.rowcount == 1
            cur.execute(f"DELETE FROM {H.POLYGONS_LAYER} WHERE id=1452332;")
            assert cur.rowcount == 1

        r = cli_runner.invoke(["diff"])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines() == [
            "--- nz_waca_adjustments:id=1452332",
            "-                           adjusted_nodes = 558",
            "-                            date_adjusted = 2011-06-07T15:22:58Z",
            "-                                     geom = MULTIPOLYGON(...)",
            "-                         survey_reference = ␀",
            "+++ nz_waca_adjustments:id=9999999",
            "+                           adjusted_nodes = 123",
            "+                            date_adjusted = 2019-07-05T13:04:00+01:00",
            "+                                     geom = POLYGON(...)",
            "+                         survey_reference = Null Island™ 🗺",
            "--- nz_waca_adjustments:id=1443053",
            "+++ nz_waca_adjustments:id=1443053",
            "-                            date_adjusted = 2011-05-10T12:09:10Z",
            "+                            date_adjusted = 2019-01-01T00:00:00Z",
            "-                         survey_reference = ␀",
            "+                         survey_reference = test",
            "--- nz_waca_adjustments:id=1424927",
            "+++ nz_waca_adjustments:id=9998",
        ]


def test_diff_table(data_working_copy, geopackage, cli_runner):
    """ diff the working copy against the repository (no index!) """
    with data_working_copy("table") as (repo, wc):
        # empty
        r = cli_runner.invoke(["diff"])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines() == []

        # make some changes
        db = geopackage(wc)
        with db:
            cur = db.cursor()

            cur.execute(H.TABLE_INSERT, H.TABLE_RECORD)
            assert cur.rowcount == 1
            cur.execute(f"UPDATE {H.TABLE_LAYER} SET \"OBJECTID\"=9998 WHERE OBJECTID=1;")
            assert cur.rowcount == 1
            cur.execute(
                f"UPDATE {H.TABLE_LAYER} SET name='test', POP2000=9867 WHERE OBJECTID=2;"
            )
            assert cur.rowcount == 1
            cur.execute(f'DELETE FROM {H.TABLE_LAYER} WHERE "OBJECTID"=3;')
            assert cur.rowcount == 1

        r = cli_runner.invoke(["diff"])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines() == [
            "--- countiestbl:OBJECTID=3",
            "-                                     AREA = 2529.9794",
            "-                                CNTY_FIPS = 065",
            "-                                     FIPS = 53065",
            "-                                     NAME = Stevens",
            "-                                  POP1990 = 30948.0",
            "-                                  POP2000 = 40652.0",
            "-                               POP90_SQMI = 12",
            "-                               STATE_FIPS = 53",
            "-                               STATE_NAME = Washington",
            "-                               Shape_Area = 0.7954858988987561",
            "-                               Shape_Leng = 4.876296245235406",
            "+++ countiestbl:OBJECTID=9999",
            "+                                     AREA = 1784.0634",
            "+                                CNTY_FIPS = 077",
            "+                                     FIPS = 27077",
            "+                                     NAME = Lake of the Gruffalo",
            "+                                  POP1990 = 4076.0",
            "+                                  POP2000 = 4651.0",
            "+                               POP90_SQMI = 2",
            "+                               STATE_FIPS = 27",
            "+                               STATE_NAME = Minnesota",
            "+                               Shape_Area = 0.565449933741451",
            "+                               Shape_Leng = 4.05545998243992",
            "--- countiestbl:OBJECTID=2",
            "+++ countiestbl:OBJECTID=2",
            "-                                     NAME = Ferry",
            "+                                     NAME = test",
            "-                                  POP2000 = 7199.0",
            "+                                  POP2000 = 9867.0",
            "--- countiestbl:OBJECTID=1",
            "+++ countiestbl:OBJECTID=9998",
        ]
