import json

import pytest


H = pytest.helpers.helpers()


@pytest.mark.parametrize("output_format", ["text", "geojson", "json"])
def test_diff_points(output_format, data_working_copy, geopackage, cli_runner):
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

        r = cli_runner.invoke(["diff", f"--{output_format}", "--output=-"])
        assert r.exit_code == 0, r
        print("STDOUT", repr(r.stdout))
        if output_format == "text":
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
        elif output_format == "geojson":
            odata = json.loads(r.stdout)
            assert len(odata["features"]) == 6
            assert odata == {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                177.071_252_196_287_02,
                                -37.979_475_484_627_57,
                            ],
                        },
                        "properties": {
                            "fid": 3,
                            "macronated": "N",
                            "name": "Tauwhare Pa",
                            "name_ascii": "Tauwhare Pa",
                            "t50_fid": 2_426_273,
                        },
                        "id": "D::3",
                    },
                    {
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
                        "properties": {
                            "fid": 9999,
                            "t50_fid": 9_999_999,
                            "name_ascii": "Te Motu-a-kore",
                            "macronated": "0",
                            "name": "Te Motu-a-kore",
                        },
                        "id": "I::9999",
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                177.078_662_844_395_9,
                                -37.988_184_857_601_8,
                            ],
                        },
                        "properties": {
                            "fid": 2,
                            "macronated": "N",
                            "name": None,
                            "name_ascii": None,
                            "t50_fid": 2_426_272,
                        },
                        "id": "U-::2",
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                177.078_662_844_395_9,
                                -37.988_184_857_601_8,
                            ],
                        },
                        "properties": {
                            "fid": 2,
                            "t50_fid": None,
                            "name_ascii": None,
                            "macronated": "N",
                            "name": "test",
                        },
                        "id": "U+::2",
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                177.095_962_971_358_6,
                                -38.004_338_036_217_68,
                            ],
                        },
                        "properties": {
                            "fid": 1,
                            "macronated": "N",
                            "name": None,
                            "name_ascii": None,
                            "t50_fid": 2_426_271,
                        },
                        "id": "U-::1",
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                177.095_962_971_358_6,
                                -38.004_338_036_217_68,
                            ],
                        },
                        "properties": {
                            "fid": 9998,
                            "t50_fid": 2_426_271,
                            "name_ascii": None,
                            "macronated": "N",
                            "name": None,
                        },
                        "id": "U+::9998",
                    },
                ],
            }
        elif output_format == "json":
            odata = json.loads(r.stdout)
            assert (
                len(odata["sno.diff/v1"]["nz_pa_points_topo_150k"]["featureChanges"])
                == 4
            )
            assert odata == {
                "sno.diff/v1": {
                    "nz_pa_points_topo_150k": {
                        "metaChanges": {},
                        "featureChanges": [
                            [
                                None,
                                {
                                    "type": "Feature",
                                    "geometry": {
                                        "type": "Point",
                                        "coordinates": [0.0, 0.0],
                                    },
                                    "properties": {
                                        "fid": 9999,
                                        "t50_fid": 9_999_999,
                                        "name_ascii": "Te Motu-a-kore",
                                        "macronated": "0",
                                        "name": "Te Motu-a-kore",
                                    },
                                    "id": "I::9999",
                                },
                            ],
                            [
                                {
                                    "type": "Feature",
                                    "geometry": {
                                        "type": "Point",
                                        "coordinates": [
                                            177.071_252_196_287_02,
                                            -37.979_475_484_627_57,
                                        ],
                                    },
                                    "properties": {
                                        "fid": 3,
                                        "macronated": "N",
                                        "name": "Tauwhare Pa",
                                        "name_ascii": "Tauwhare Pa",
                                        "t50_fid": 2_426_273,
                                    },
                                    "id": "D::3",
                                },
                                None,
                            ],
                            [
                                {
                                    "type": "Feature",
                                    "geometry": {
                                        "type": "Point",
                                        "coordinates": [
                                            177.095_962_971_358_6,
                                            -38.004_338_036_217_68,
                                        ],
                                    },
                                    "properties": {
                                        "fid": 1,
                                        "macronated": "N",
                                        "name": None,
                                        "name_ascii": None,
                                        "t50_fid": 2_426_271,
                                    },
                                    "id": "U-::1",
                                },
                                {
                                    "type": "Feature",
                                    "geometry": {
                                        "type": "Point",
                                        "coordinates": [
                                            177.095_962_971_358_6,
                                            -38.004_338_036_217_68,
                                        ],
                                    },
                                    "properties": {
                                        "fid": 9998,
                                        "t50_fid": 2_426_271,
                                        "name_ascii": None,
                                        "macronated": "N",
                                        "name": None,
                                    },
                                    "id": "U+::9998",
                                },
                            ],
                            [
                                {
                                    "type": "Feature",
                                    "geometry": {
                                        "type": "Point",
                                        "coordinates": [
                                            177.078_662_844_395_9,
                                            -37.988_184_857_601_8,
                                        ],
                                    },
                                    "properties": {
                                        "fid": 2,
                                        "macronated": "N",
                                        "name": None,
                                        "name_ascii": None,
                                        "t50_fid": 2_426_272,
                                    },
                                    "id": "U-::2",
                                },
                                {
                                    "type": "Feature",
                                    "geometry": {
                                        "type": "Point",
                                        "coordinates": [
                                            177.078_662_844_395_9,
                                            -37.988_184_857_601_8,
                                        ],
                                    },
                                    "properties": {
                                        "fid": 2,
                                        "t50_fid": None,
                                        "name_ascii": None,
                                        "macronated": "N",
                                        "name": "test",
                                    },
                                    "id": "U+::2",
                                },
                            ],
                        ],
                    }
                }
            }


@pytest.mark.parametrize("output_format", ["text", "geojson", "json"])
def test_diff_polygons(output_format, data_working_copy, geopackage, cli_runner):
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

        r = cli_runner.invoke(["diff", f"--{output_format}", "--output=-"])
        assert r.exit_code == 0, r
        if output_format == "text":
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
        elif output_format == "geojson":
            odata = json.loads(r.stdout)
            assert len(odata["features"]) == 6
            assert odata == {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "MultiPolygon",
                            "coordinates": [
                                [
                                    [
                                        [174.731_157_683_3, -36.799_283_85],
                                        [174.730_470_716_7, -36.796_400_95],
                                        [174.730_472_2, -36.796_323_283_3],
                                        [174.731_246_833_3, -36.795_535_566_7],
                                        [174.731_796_216_7, -36.795_137_983_3],
                                        [174.731_870_233_3, -36.795_087_966_7],
                                        [174.731_899_816_7, -36.795_070_716_7],
                                        [174.732_051_85, -36.794_982_083_3],
                                        [174.732_203_9, -36.794_893_45],
                                        [174.732_812_133_3, -36.794_538_9],
                                        [174.733_139_883_3, -36.794_347_85],
                                        [174.733_307_983_3, -36.794_249_866_7],
                                        [174.733_341_716_7, -36.794_231_666_7],
                                        [174.733_702_1, -36.794_166_5],
                                        [174.733_990_683_3, -36.794_262_933_3],
                                        [174.734_288_05, -36.794_433_116_7],
                                        [174.736_541_133_3, -36.796_472_616_7],
                                        [174.736_568_65, -36.796_552_566_7],
                                        [174.736_553_833_3, -36.796_667],
                                        [174.736_335_3, -36.796_878_85],
                                        [174.736_180_016_7, -36.797_001_816_7],
                                        [174.732_969_516_7, -36.799_071_45],
                                        [174.732_654_483_3, -36.799_214_2],
                                        [174.731_157_683_3, -36.799_283_85],
                                    ]
                                ]
                            ],
                        },
                        "properties": {
                            "id": 1_452_332,
                            "adjusted_nodes": 558,
                            "date_adjusted": "2011-06-07T15:22:58Z",
                            "survey_reference": None,
                        },
                        "id": "D::1452332",
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [
                                [
                                    [0.0, 0.0],
                                    [0.0, 0.001],
                                    [0.001, 0.001],
                                    [0.001, 0.0],
                                    [0.0, 0.0],
                                ]
                            ],
                        },
                        "properties": {
                            "id": 9_999_999,
                            "date_adjusted": "2019-07-05T13:04:00+01:00",
                            "survey_reference": "Null Island™ 🗺",
                            "adjusted_nodes": 123,
                        },
                        "id": "I::9999999",
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "MultiPolygon",
                            "coordinates": [
                                [
                                    [
                                        [174.216_618_083_3, -39.116_006_916_7],
                                        [174.210_532_433_3, -39.062_889_633_3],
                                        [174.218_767_133_3, -39.044_481_366_7],
                                        [174.233_628_6, -39.043_576_183_3],
                                        [174.248_983_433_3, -39.067_347_716_7],
                                        [174.237_115_083_3, -39.104_299_8],
                                        [174.237_047_966_7, -39.104_386_5],
                                        [174.223_032_466_7, -39.114_993_95],
                                        [174.222_116_8, -39.115_347_05],
                                        [174.219_978_466_7, -39.115_833_983_3],
                                        [174.216_618_083_3, -39.116_006_916_7],
                                    ]
                                ]
                            ],
                        },
                        "properties": {
                            "id": 1_443_053,
                            "adjusted_nodes": 1238,
                            "date_adjusted": "2011-05-10T12:09:10Z",
                            "survey_reference": None,
                        },
                        "id": "U-::1443053",
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "MultiPolygon",
                            "coordinates": [
                                [
                                    [
                                        [174.216_618_083_3, -39.116_006_916_7],
                                        [174.210_532_433_3, -39.062_889_633_3],
                                        [174.218_767_133_3, -39.044_481_366_7],
                                        [174.233_628_6, -39.043_576_183_3],
                                        [174.248_983_433_3, -39.067_347_716_7],
                                        [174.237_115_083_3, -39.104_299_8],
                                        [174.237_047_966_7, -39.104_386_5],
                                        [174.223_032_466_7, -39.114_993_95],
                                        [174.222_116_8, -39.115_347_05],
                                        [174.219_978_466_7, -39.115_833_983_3],
                                        [174.216_618_083_3, -39.116_006_916_7],
                                    ]
                                ]
                            ],
                        },
                        "properties": {
                            "id": 1_443_053,
                            "date_adjusted": "2019-01-01T00:00:00Z",
                            "survey_reference": "test",
                            "adjusted_nodes": 1238,
                        },
                        "id": "U+::1443053",
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "MultiPolygon",
                            "coordinates": [
                                [
                                    [
                                        [175.365_019_55, -37.867_737_133_3],
                                        [175.359_424_816_7, -37.859_677_466_7],
                                        [175.358_776_6, -37.858_739_4],
                                        [175.357_528_166_7, -37.856_829_966_7],
                                        [175.357_346_8, -37.856_404_883_3],
                                        [175.350_319_216_7, -37.838_409_016_7],
                                        [175.351_635_8, -37.834_856_583_3],
                                        [175.357_739_316_7, -37.827_765_216_7],
                                        [175.358_196_366_7, -37.827_312_183_3],
                                        [175.361_308_266_7, -37.827_064_966_7],
                                        [175.384_347_033_3, -37.849_134_05],
                                        [175.384_300_45, -37.849_304_583_3],
                                        [175.377_467_833_3, -37.860_278_266_7],
                                        [175.375_013_566_7, -37.864_152_2],
                                        [175.373_939_666_7, -37.865_846_683_3],
                                        [175.372_695_366_7, -37.867_499_533_3],
                                        [175.372_516_333_3, -37.867_591_25],
                                        [175.365_019_55, -37.867_737_133_3],
                                    ]
                                ]
                            ],
                        },
                        "properties": {
                            "id": 1_424_927,
                            "adjusted_nodes": 1122,
                            "date_adjusted": "2011-03-25T07:30:45Z",
                            "survey_reference": None,
                        },
                        "id": "U-::1424927",
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "MultiPolygon",
                            "coordinates": [
                                [
                                    [
                                        [175.365_019_55, -37.867_737_133_3],
                                        [175.359_424_816_7, -37.859_677_466_7],
                                        [175.358_776_6, -37.858_739_4],
                                        [175.357_528_166_7, -37.856_829_966_7],
                                        [175.357_346_8, -37.856_404_883_3],
                                        [175.350_319_216_7, -37.838_409_016_7],
                                        [175.351_635_8, -37.834_856_583_3],
                                        [175.357_739_316_7, -37.827_765_216_7],
                                        [175.358_196_366_7, -37.827_312_183_3],
                                        [175.361_308_266_7, -37.827_064_966_7],
                                        [175.384_347_033_3, -37.849_134_05],
                                        [175.384_300_45, -37.849_304_583_3],
                                        [175.377_467_833_3, -37.860_278_266_7],
                                        [175.375_013_566_7, -37.864_152_2],
                                        [175.373_939_666_7, -37.865_846_683_3],
                                        [175.372_695_366_7, -37.867_499_533_3],
                                        [175.372_516_333_3, -37.867_591_25],
                                        [175.365_019_55, -37.867_737_133_3],
                                    ]
                                ]
                            ],
                        },
                        "properties": {
                            "id": 9998,
                            "date_adjusted": "2011-03-25T07:30:45Z",
                            "survey_reference": None,
                            "adjusted_nodes": 1122,
                        },
                        "id": "U+::9998",
                    },
                ],
            }
        elif output_format == "json":
            odata = json.loads(r.stdout)
            assert (
                len(odata["sno.diff/v1"]["nz_waca_adjustments"]["featureChanges"]) == 4
            )
            assert odata == {
                "sno.diff/v1": {
                    "nz_waca_adjustments": {
                        "metaChanges": {},
                        "featureChanges": [
                            [
                                None,
                                {
                                    "type": "Feature",
                                    "geometry": {
                                        "type": "Polygon",
                                        "coordinates": [
                                            [
                                                [0.0, 0.0],
                                                [0.0, 0.001],
                                                [0.001, 0.001],
                                                [0.001, 0.0],
                                                [0.0, 0.0],
                                            ]
                                        ],
                                    },
                                    "properties": {
                                        "id": 9_999_999,
                                        "date_adjusted": "2019-07-05T13:04:00+01:00",
                                        "survey_reference": "Null Island™ 🗺",
                                        "adjusted_nodes": 123,
                                    },
                                    "id": "I::9999999",
                                },
                            ],
                            [
                                {
                                    "type": "Feature",
                                    "geometry": {
                                        "type": "MultiPolygon",
                                        "coordinates": [
                                            [
                                                [
                                                    [174.731_157_683_3, -36.799_283_85],
                                                    [174.730_470_716_7, -36.796_400_95],
                                                    [174.730_472_2, -36.796_323_283_3],
                                                    [
                                                        174.731_246_833_3,
                                                        -36.795_535_566_7,
                                                    ],
                                                    [
                                                        174.731_796_216_7,
                                                        -36.795_137_983_3,
                                                    ],
                                                    [
                                                        174.731_870_233_3,
                                                        -36.795_087_966_7,
                                                    ],
                                                    [
                                                        174.731_899_816_7,
                                                        -36.795_070_716_7,
                                                    ],
                                                    [174.732_051_85, -36.794_982_083_3],
                                                    [174.732_203_9, -36.794_893_45],
                                                    [174.732_812_133_3, -36.794_538_9],
                                                    [174.733_139_883_3, -36.794_347_85],
                                                    [
                                                        174.733_307_983_3,
                                                        -36.794_249_866_7,
                                                    ],
                                                    [
                                                        174.733_341_716_7,
                                                        -36.794_231_666_7,
                                                    ],
                                                    [174.733_702_1, -36.794_166_5],
                                                    [
                                                        174.733_990_683_3,
                                                        -36.794_262_933_3,
                                                    ],
                                                    [174.734_288_05, -36.794_433_116_7],
                                                    [
                                                        174.736_541_133_3,
                                                        -36.796_472_616_7,
                                                    ],
                                                    [174.736_568_65, -36.796_552_566_7],
                                                    [174.736_553_833_3, -36.796_667],
                                                    [174.736_335_3, -36.796_878_85],
                                                    [
                                                        174.736_180_016_7,
                                                        -36.797_001_816_7,
                                                    ],
                                                    [174.732_969_516_7, -36.799_071_45],
                                                    [174.732_654_483_3, -36.799_214_2],
                                                    [174.731_157_683_3, -36.799_283_85],
                                                ]
                                            ]
                                        ],
                                    },
                                    "properties": {
                                        "id": 1_452_332,
                                        "adjusted_nodes": 558,
                                        "date_adjusted": "2011-06-07T15:22:58Z",
                                        "survey_reference": None,
                                    },
                                    "id": "D::1452332",
                                },
                                None,
                            ],
                            [
                                {
                                    "type": "Feature",
                                    "geometry": {
                                        "type": "MultiPolygon",
                                        "coordinates": [
                                            [
                                                [
                                                    [175.365_019_55, -37.867_737_133_3],
                                                    [
                                                        175.359_424_816_7,
                                                        -37.859_677_466_7,
                                                    ],
                                                    [175.358_776_6, -37.858_739_4],
                                                    [
                                                        175.357_528_166_7,
                                                        -37.856_829_966_7,
                                                    ],
                                                    [175.357_346_8, -37.856_404_883_3],
                                                    [
                                                        175.350_319_216_7,
                                                        -37.838_409_016_7,
                                                    ],
                                                    [175.351_635_8, -37.834_856_583_3],
                                                    [
                                                        175.357_739_316_7,
                                                        -37.827_765_216_7,
                                                    ],
                                                    [
                                                        175.358_196_366_7,
                                                        -37.827_312_183_3,
                                                    ],
                                                    [
                                                        175.361_308_266_7,
                                                        -37.827_064_966_7,
                                                    ],
                                                    [175.384_347_033_3, -37.849_134_05],
                                                    [175.384_300_45, -37.849_304_583_3],
                                                    [
                                                        175.377_467_833_3,
                                                        -37.860_278_266_7,
                                                    ],
                                                    [175.375_013_566_7, -37.864_152_2],
                                                    [
                                                        175.373_939_666_7,
                                                        -37.865_846_683_3,
                                                    ],
                                                    [
                                                        175.372_695_366_7,
                                                        -37.867_499_533_3,
                                                    ],
                                                    [175.372_516_333_3, -37.867_591_25],
                                                    [175.365_019_55, -37.867_737_133_3],
                                                ]
                                            ]
                                        ],
                                    },
                                    "properties": {
                                        "id": 1_424_927,
                                        "adjusted_nodes": 1122,
                                        "date_adjusted": "2011-03-25T07:30:45Z",
                                        "survey_reference": None,
                                    },
                                    "id": "U-::1424927",
                                },
                                {
                                    "type": "Feature",
                                    "geometry": {
                                        "type": "MultiPolygon",
                                        "coordinates": [
                                            [
                                                [
                                                    [175.365_019_55, -37.867_737_133_3],
                                                    [
                                                        175.359_424_816_7,
                                                        -37.859_677_466_7,
                                                    ],
                                                    [175.358_776_6, -37.858_739_4],
                                                    [
                                                        175.357_528_166_7,
                                                        -37.856_829_966_7,
                                                    ],
                                                    [175.357_346_8, -37.856_404_883_3],
                                                    [
                                                        175.350_319_216_7,
                                                        -37.838_409_016_7,
                                                    ],
                                                    [175.351_635_8, -37.834_856_583_3],
                                                    [
                                                        175.357_739_316_7,
                                                        -37.827_765_216_7,
                                                    ],
                                                    [
                                                        175.358_196_366_7,
                                                        -37.827_312_183_3,
                                                    ],
                                                    [
                                                        175.361_308_266_7,
                                                        -37.827_064_966_7,
                                                    ],
                                                    [175.384_347_033_3, -37.849_134_05],
                                                    [175.384_300_45, -37.849_304_583_3],
                                                    [
                                                        175.377_467_833_3,
                                                        -37.860_278_266_7,
                                                    ],
                                                    [175.375_013_566_7, -37.864_152_2],
                                                    [
                                                        175.373_939_666_7,
                                                        -37.865_846_683_3,
                                                    ],
                                                    [
                                                        175.372_695_366_7,
                                                        -37.867_499_533_3,
                                                    ],
                                                    [175.372_516_333_3, -37.867_591_25],
                                                    [175.365_019_55, -37.867_737_133_3],
                                                ]
                                            ]
                                        ],
                                    },
                                    "properties": {
                                        "id": 9998,
                                        "date_adjusted": "2011-03-25T07:30:45Z",
                                        "survey_reference": None,
                                        "adjusted_nodes": 1122,
                                    },
                                    "id": "U+::9998",
                                },
                            ],
                            [
                                {
                                    "type": "Feature",
                                    "geometry": {
                                        "type": "MultiPolygon",
                                        "coordinates": [
                                            [
                                                [
                                                    [
                                                        174.216_618_083_3,
                                                        -39.116_006_916_7,
                                                    ],
                                                    [
                                                        174.210_532_433_3,
                                                        -39.062_889_633_3,
                                                    ],
                                                    [
                                                        174.218_767_133_3,
                                                        -39.044_481_366_7,
                                                    ],
                                                    [174.233_628_6, -39.043_576_183_3],
                                                    [
                                                        174.248_983_433_3,
                                                        -39.067_347_716_7,
                                                    ],
                                                    [174.237_115_083_3, -39.104_299_8],
                                                    [174.237_047_966_7, -39.104_386_5],
                                                    [174.223_032_466_7, -39.114_993_95],
                                                    [174.222_116_8, -39.115_347_05],
                                                    [
                                                        174.219_978_466_7,
                                                        -39.115_833_983_3,
                                                    ],
                                                    [
                                                        174.216_618_083_3,
                                                        -39.116_006_916_7,
                                                    ],
                                                ]
                                            ]
                                        ],
                                    },
                                    "properties": {
                                        "id": 1_443_053,
                                        "adjusted_nodes": 1238,
                                        "date_adjusted": "2011-05-10T12:09:10Z",
                                        "survey_reference": None,
                                    },
                                    "id": "U-::1443053",
                                },
                                {
                                    "type": "Feature",
                                    "geometry": {
                                        "type": "MultiPolygon",
                                        "coordinates": [
                                            [
                                                [
                                                    [
                                                        174.216_618_083_3,
                                                        -39.116_006_916_7,
                                                    ],
                                                    [
                                                        174.210_532_433_3,
                                                        -39.062_889_633_3,
                                                    ],
                                                    [
                                                        174.218_767_133_3,
                                                        -39.044_481_366_7,
                                                    ],
                                                    [174.233_628_6, -39.043_576_183_3],
                                                    [
                                                        174.248_983_433_3,
                                                        -39.067_347_716_7,
                                                    ],
                                                    [174.237_115_083_3, -39.104_299_8],
                                                    [174.237_047_966_7, -39.104_386_5],
                                                    [174.223_032_466_7, -39.114_993_95],
                                                    [174.222_116_8, -39.115_347_05],
                                                    [
                                                        174.219_978_466_7,
                                                        -39.115_833_983_3,
                                                    ],
                                                    [
                                                        174.216_618_083_3,
                                                        -39.116_006_916_7,
                                                    ],
                                                ]
                                            ]
                                        ],
                                    },
                                    "properties": {
                                        "id": 1_443_053,
                                        "date_adjusted": "2019-01-01T00:00:00Z",
                                        "survey_reference": "test",
                                        "adjusted_nodes": 1238,
                                    },
                                    "id": "U+::1443053",
                                },
                            ],
                        ],
                    }
                }
            }


@pytest.mark.parametrize("output_format", ["text", "geojson", "json"])
def test_diff_table(output_format, data_working_copy, geopackage, cli_runner):
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
            cur.execute(f'UPDATE {H.TABLE_LAYER} SET "OBJECTID"=9998 WHERE OBJECTID=1;')
            assert cur.rowcount == 1
            cur.execute(
                f"UPDATE {H.TABLE_LAYER} SET name='test', POP2000=9867 WHERE OBJECTID=2;"
            )
            assert cur.rowcount == 1
            cur.execute(f'DELETE FROM {H.TABLE_LAYER} WHERE "OBJECTID"=3;')
            assert cur.rowcount == 1

        r = cli_runner.invoke(["diff", f"--{output_format}", "--output=-"])
        assert r.exit_code == 0, r
        if output_format == "text":
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
        elif output_format == "geojson":
            odata = json.loads(r.stdout)
            assert len(odata["features"]) == 6
            assert odata == {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": None,
                        "properties": {
                            "OBJECTID": 3,
                            "AREA": 2529.9794,
                            "CNTY_FIPS": "065",
                            "FIPS": "53065",
                            "NAME": "Stevens",
                            "POP1990": 30948.0,
                            "POP2000": 40652.0,
                            "POP90_SQMI": 12,
                            "STATE_FIPS": "53",
                            "STATE_NAME": "Washington",
                            "Shape_Area": 0.795_485_898_898_756_1,
                            "Shape_Leng": 4.876_296_245_235_406,
                        },
                        "id": "D::3",
                    },
                    {
                        "type": "Feature",
                        "geometry": None,
                        "properties": {
                            "OBJECTID": 9999,
                            "NAME": "Lake of the Gruffalo",
                            "STATE_NAME": "Minnesota",
                            "STATE_FIPS": "27",
                            "CNTY_FIPS": "077",
                            "FIPS": "27077",
                            "AREA": 1784.0634,
                            "POP1990": 4076.0,
                            "POP2000": 4651.0,
                            "POP90_SQMI": 2,
                            "Shape_Leng": 4.055_459_982_439_92,
                            "Shape_Area": 0.565_449_933_741_451,
                        },
                        "id": "I::9999",
                    },
                    {
                        "type": "Feature",
                        "geometry": None,
                        "properties": {
                            "OBJECTID": 2,
                            "AREA": 2280.2319,
                            "CNTY_FIPS": "019",
                            "FIPS": "53019",
                            "NAME": "Ferry",
                            "POP1990": 6295.0,
                            "POP2000": 7199.0,
                            "POP90_SQMI": 3,
                            "STATE_FIPS": "53",
                            "STATE_NAME": "Washington",
                            "Shape_Area": 0.718_059_302_645_116_1,
                            "Shape_Leng": 3.786_160_993_863_997,
                        },
                        "id": "U-::2",
                    },
                    {
                        "type": "Feature",
                        "geometry": None,
                        "properties": {
                            "OBJECTID": 2,
                            "NAME": "test",
                            "STATE_NAME": "Washington",
                            "STATE_FIPS": "53",
                            "CNTY_FIPS": "019",
                            "FIPS": "53019",
                            "AREA": 2280.2319,
                            "POP1990": 6295.0,
                            "POP2000": 9867.0,
                            "POP90_SQMI": 3,
                            "Shape_Leng": 3.786_160_993_863_997,
                            "Shape_Area": 0.718_059_302_645_116_1,
                        },
                        "id": "U+::2",
                    },
                    {
                        "type": "Feature",
                        "geometry": None,
                        "properties": {
                            "OBJECTID": 1,
                            "AREA": 1784.0634,
                            "CNTY_FIPS": "077",
                            "FIPS": "27077",
                            "NAME": "Lake of the Woods",
                            "POP1990": 4076.0,
                            "POP2000": 4651.0,
                            "POP90_SQMI": 2,
                            "STATE_FIPS": "27",
                            "STATE_NAME": "Minnesota",
                            "Shape_Area": 0.565_449_933_741_450_9,
                            "Shape_Leng": 4.055_459_982_439_919,
                        },
                        "id": "U-::1",
                    },
                    {
                        "type": "Feature",
                        "geometry": None,
                        "properties": {
                            "OBJECTID": 9998,
                            "NAME": "Lake of the Woods",
                            "STATE_NAME": "Minnesota",
                            "STATE_FIPS": "27",
                            "CNTY_FIPS": "077",
                            "FIPS": "27077",
                            "AREA": 1784.0634,
                            "POP1990": 4076.0,
                            "POP2000": 4651.0,
                            "POP90_SQMI": 2,
                            "Shape_Leng": 4.055_459_982_439_919,
                            "Shape_Area": 0.565_449_933_741_450_9,
                        },
                        "id": "U+::9998",
                    },
                ],
            }
        elif output_format == "json":
            odata = json.loads(r.stdout)
            assert len(odata["sno.diff/v1"]["countiestbl"]["featureChanges"]) == 4
            assert odata == {
                "sno.diff/v1": {
                    "countiestbl": {
                        "metaChanges": {},
                        "featureChanges": [
                            [
                                None,
                                {
                                    "type": "Feature",
                                    "geometry": None,
                                    "properties": {
                                        "OBJECTID": 9999,
                                        "NAME": "Lake of the Gruffalo",
                                        "STATE_NAME": "Minnesota",
                                        "STATE_FIPS": "27",
                                        "CNTY_FIPS": "077",
                                        "FIPS": "27077",
                                        "AREA": 1784.0634,
                                        "POP1990": 4076.0,
                                        "POP2000": 4651.0,
                                        "POP90_SQMI": 2,
                                        "Shape_Leng": 4.055_459_982_439_92,
                                        "Shape_Area": 0.565_449_933_741_451,
                                    },
                                    "id": "I::9999",
                                },
                            ],
                            [
                                {
                                    "type": "Feature",
                                    "geometry": None,
                                    "properties": {
                                        "OBJECTID": 3,
                                        "AREA": 2529.9794,
                                        "CNTY_FIPS": "065",
                                        "FIPS": "53065",
                                        "NAME": "Stevens",
                                        "POP1990": 30948.0,
                                        "POP2000": 40652.0,
                                        "POP90_SQMI": 12,
                                        "STATE_FIPS": "53",
                                        "STATE_NAME": "Washington",
                                        "Shape_Area": 0.795_485_898_898_756_1,
                                        "Shape_Leng": 4.876_296_245_235_406,
                                    },
                                    "id": "D::3",
                                },
                                None,
                            ],
                            [
                                {
                                    "type": "Feature",
                                    "geometry": None,
                                    "properties": {
                                        "OBJECTID": 1,
                                        "AREA": 1784.0634,
                                        "CNTY_FIPS": "077",
                                        "FIPS": "27077",
                                        "NAME": "Lake of the Woods",
                                        "POP1990": 4076.0,
                                        "POP2000": 4651.0,
                                        "POP90_SQMI": 2,
                                        "STATE_FIPS": "27",
                                        "STATE_NAME": "Minnesota",
                                        "Shape_Area": 0.565_449_933_741_450_9,
                                        "Shape_Leng": 4.055_459_982_439_919,
                                    },
                                    "id": "U-::1",
                                },
                                {
                                    "type": "Feature",
                                    "geometry": None,
                                    "properties": {
                                        "OBJECTID": 9998,
                                        "NAME": "Lake of the Woods",
                                        "STATE_NAME": "Minnesota",
                                        "STATE_FIPS": "27",
                                        "CNTY_FIPS": "077",
                                        "FIPS": "27077",
                                        "AREA": 1784.0634,
                                        "POP1990": 4076.0,
                                        "POP2000": 4651.0,
                                        "POP90_SQMI": 2,
                                        "Shape_Leng": 4.055_459_982_439_919,
                                        "Shape_Area": 0.565_449_933_741_450_9,
                                    },
                                    "id": "U+::9998",
                                },
                            ],
                            [
                                {
                                    "type": "Feature",
                                    "geometry": None,
                                    "properties": {
                                        "OBJECTID": 2,
                                        "AREA": 2280.2319,
                                        "CNTY_FIPS": "019",
                                        "FIPS": "53019",
                                        "NAME": "Ferry",
                                        "POP1990": 6295.0,
                                        "POP2000": 7199.0,
                                        "POP90_SQMI": 3,
                                        "STATE_FIPS": "53",
                                        "STATE_NAME": "Washington",
                                        "Shape_Area": 0.718_059_302_645_116_1,
                                        "Shape_Leng": 3.786_160_993_863_997,
                                    },
                                    "id": "U-::2",
                                },
                                {
                                    "type": "Feature",
                                    "geometry": None,
                                    "properties": {
                                        "OBJECTID": 2,
                                        "NAME": "test",
                                        "STATE_NAME": "Washington",
                                        "STATE_FIPS": "53",
                                        "CNTY_FIPS": "019",
                                        "FIPS": "53019",
                                        "AREA": 2280.2319,
                                        "POP1990": 6295.0,
                                        "POP2000": 9867.0,
                                        "POP90_SQMI": 3,
                                        "Shape_Leng": 3.786_160_993_863_997,
                                        "Shape_Area": 0.718_059_302_645_116_1,
                                    },
                                    "id": "U+::2",
                                },
                            ],
                        ],
                    }
                }
            }
