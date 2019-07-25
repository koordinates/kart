import pytest


H = pytest.helpers.helpers()


@pytest.mark.parametrize("archive,table", [
    pytest.param('points.snow', H.POINTS_LAYER, id='points'),
    pytest.param('polygons.snow', H.POLYGONS_LAYER, id='polygons-pk'),
    pytest.param('table.snow', H.TABLE_LAYER, id='table'),
])
def test_diff(archive, table, data_working_copy, geopackage, cli_runner):
    """ diff the working copy against the repository (no index!) """
    with data_working_copy(archive) as (repo, wc):
        # empty
        r = cli_runner.invoke(["diff"])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines() == []

        # make some changes
        db = geopackage(wc)
        with db:
            cur = db.cursor()
            if table == H.POINTS_LAYER:
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

            elif table == H.POLYGONS_LAYER:
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

            elif table == H.TABLE_LAYER:
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

            else:
                raise NotImplementedError(f"table={table}")

        r = cli_runner.invoke(["diff"])
        assert r.exit_code == 0, r
        if table == H.POINTS_LAYER:
            assert r.stdout.splitlines() == [
                "--- 2bad8ad5-97aa-4910-9e6c-c6e8e692700d",
                "-                                      fid = 3",
                "-                                     geom = POINT(...)",
                "-                               macronated = N",
                "-                                     name = Tauwhare Pa",
                "-                               name_ascii = Tauwhare Pa",
                "-                                  t50_fid = 2426273",
                "+++ {new feature}",
                "+                                      fid = 9999",
                "+                                     geom = POINT(...)",
                "+                                  t50_fid = 9999999",
                "+                               name_ascii = Te Motu-a-kore",
                "+                               macronated = 0",
                "+                                     name = Te Motu-a-kore",
                "--- 7416b7ab-992d-4595-ab96-39a186f5968a",
                "+++ 7416b7ab-992d-4595-ab96-39a186f5968a",
                "-                                      fid = 1",
                "+                                      fid = 9998",
                "--- 905a8ae1-8346-4b42-8646-42385beac87f",
                "+++ 905a8ae1-8346-4b42-8646-42385beac87f",
                "                                       fid = 2",
                "-                                     name = ␀",
                "+                                     name = test",
                "-                                  t50_fid = 2426272",
                "+                                  t50_fid = ␀",
            ]
        elif table == H.POLYGONS_LAYER:
            assert r.stdout.splitlines() == [
                "--- a015ce37-666c-4f90-889b-6c035300dc59",
                "-                           adjusted_nodes = 558",
                "-                            date_adjusted = 2011-06-07T15:22:58Z",
                "-                                     geom = MULTIPOLYGON(...)",
                "-                                       id = 1452332",
                "-                         survey_reference = ␀",
                "+++ {new feature}",
                "+                                       id = 9999999",
                "+                                     geom = POLYGON(...)",
                "+                            date_adjusted = 2019-07-05T13:04:00+01:00",
                "+                         survey_reference = Null Island™ 🗺",
                "+                           adjusted_nodes = 123",
                "--- a7bdd03b-2aa9-41f8-91a7-abb5d5ec621b",
                "+++ a7bdd03b-2aa9-41f8-91a7-abb5d5ec621b",
                "-                                       id = 1424927",
                "+                                       id = 9998",
                "--- ec360eb3-f57e-47d0-bda5-a0a02231ff6c",
                "+++ ec360eb3-f57e-47d0-bda5-a0a02231ff6c",
                "                                        id = 1443053",
                "-                            date_adjusted = 2011-05-10T12:09:10Z",
                "+                            date_adjusted = 2019-01-01T00:00:00Z",
                "-                         survey_reference = ␀",
                "+                         survey_reference = test",
            ]
        elif table == H.TABLE_LAYER:
            assert r.stdout.splitlines() == [
                "--- b8e60439-89e2-4432-8fa3-189e2f7813e4",
                "-                                     AREA = 2529.9794",
                "-                                CNTY_FIPS = 065",
                "-                                     FIPS = 53065",
                "-                                     NAME = Stevens",
                "-                                 OBJECTID = 3",
                "-                                  POP1990 = 30948.0",
                "-                                  POP2000 = 40652.0",
                "-                               POP90_SQMI = 12",
                "-                               STATE_FIPS = 53",
                "-                               STATE_NAME = Washington",
                "-                               Shape_Area = 0.7954858988987561",
                "-                               Shape_Leng = 4.876296245235406",
                "+++ {new feature}",
                "+                                 OBJECTID = 9999",
                "+                                     NAME = Lake of the Gruffalo",
                "+                               STATE_NAME = Minnesota",
                "+                               STATE_FIPS = 27",
                "+                                CNTY_FIPS = 077",
                "+                                     FIPS = 27077",
                "+                                     AREA = 1784.0634",
                "+                                  POP1990 = 4076.0",
                "+                                  POP2000 = 4651.0",
                "+                               POP90_SQMI = 2",
                "+                               Shape_Leng = 4.05545998243992",
                "+                               Shape_Area = 0.565449933741451",
                "--- 326581a9-766f-48ad-a30b-0fd8dddd5f83",
                "+++ 326581a9-766f-48ad-a30b-0fd8dddd5f83",
                "                                  OBJECTID = 2",
                "-                                     NAME = Ferry",
                "+                                     NAME = test",
                "-                                  POP2000 = 7199.0",
                "+                                  POP2000 = 9867.0",
                "--- bd2d37e1-1183-433b-98d0-076b5cb1b5be",
                "+++ bd2d37e1-1183-433b-98d0-076b5cb1b5be",
                "-                                 OBJECTID = 1",
                "+                                 OBJECTID = 9998",
            ]
