import functools
import json
import re
import string
import time
from pathlib import Path
import webbrowser

import html5lib
import pytest

import kart
from kart.tabular.v3 import TableV3
from kart.base_dataset import BaseDataset
from kart.diff_format import DiffFormat
from kart.diff_structs import Delta, DeltaDiff
from kart.html_diff_writer import HtmlDiffWriter
from kart.json_diff_writers import JsonLinesDiffWriter
from kart.geometry import hex_wkb_to_ogr
from kart.repo import KartRepo
from kart.serialise_util import b64decode_str


H = pytest.helpers.helpers()

DIFF_OUTPUT_FORMATS = ["text", "geojson", "json", "json-lines", "quiet", "html"]
SHOW_OUTPUT_FORMATS = DIFF_OUTPUT_FORMATS


def _check_html_output(s):
    parser = html5lib.HTMLParser(strict=True, namespaceHTMLElements=False)
    # throw errors on invalid HTML
    document = parser.parse(s)
    # find the <script> element containing data
    el = document.find("./head/script[@id='kart-data']")
    # Make sure we're parsing it as JSON.
    assert el.attrib == {"id": "kart-data", "type": "application/json"}
    # validate it
    return json.loads(el.text)


@pytest.mark.parametrize("output_format", DIFF_OUTPUT_FORMATS)
def test_diff_points(output_format, data_working_copy, cli_runner):
    """diff the working copy against HEAD"""
    with data_working_copy("points") as (repo_path, wc):
        # empty
        r = cli_runner.invoke(
            ["diff", f"--output-format={output_format}", "--output=-", "--exit-code"]
        )
        assert r.exit_code == 0, r.stderr

        # make some changes
        repo = KartRepo(repo_path)
        with repo.working_copy.tabular.session() as sess:
            r = sess.execute(H.POINTS.INSERT, H.POINTS.RECORD)
            assert r.rowcount == 1
            r = sess.execute(f"UPDATE {H.POINTS.LAYER} SET fid=9998 WHERE fid=1;")
            assert r.rowcount == 1
            r = sess.execute(
                f"UPDATE {H.POINTS.LAYER} SET name='test', t50_fid=NULL WHERE fid=2;"
            )
            assert r.rowcount == 1
            r = sess.execute(f"DELETE FROM {H.POINTS.LAYER} WHERE fid=3;")
            assert r.rowcount == 1

        r = cli_runner.invoke(
            ["diff", f"--output-format={output_format}", "--output=-"]
        )
        if output_format == "quiet":
            assert r.exit_code == 1, r
            assert r.stdout == ""
        elif output_format == "text":
            assert r.exit_code == 0, r
            assert r.stdout.splitlines() == [
                "--- nz_pa_points_topo_150k:feature:1",
                "+++ nz_pa_points_topo_150k:feature:9998",
                "-                                      fid = 1",
                "+                                      fid = 9998",
                "--- nz_pa_points_topo_150k:feature:2",
                "+++ nz_pa_points_topo_150k:feature:2",
                "-                                  t50_fid = 2426272",
                "+                                  t50_fid = ␀",
                "-                                     name = ␀",
                "+                                     name = test",
                "--- nz_pa_points_topo_150k:feature:3",
                "-                                      fid = 3",
                "-                                     geom = POINT(...)",
                "-                                  t50_fid = 2426273",
                "-                               name_ascii = Tauwhare Pa",
                "-                               macronated = N",
                "-                                     name = Tauwhare Pa",
                "+++ nz_pa_points_topo_150k:feature:9999",
                "+                                      fid = 9999",
                "+                                     geom = POINT(...)",
                "+                                  t50_fid = 9999999",
                "+                               name_ascii = Te Motu-a-kore",
                "+                               macronated = N",
                "+                                     name = Te Motu-a-kore",
            ]
        elif output_format == "geojson":
            assert r.exit_code == 0, r
            odata = json.loads(r.stdout)
            assert len(odata["features"]) == 6
            assert odata == {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [177.0959629713586, -38.00433803621768],
                        },
                        "properties": {
                            "fid": 1,
                            "t50_fid": 2426271,
                            "name_ascii": None,
                            "macronated": "N",
                            "name": None,
                        },
                        "id": "nz_pa_points_topo_150k:feature:1:U-",
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [177.0959629713586, -38.00433803621768],
                        },
                        "properties": {
                            "fid": 9998,
                            "t50_fid": 2426271,
                            "name_ascii": None,
                            "macronated": "N",
                            "name": None,
                        },
                        "id": "nz_pa_points_topo_150k:feature:9998:U+",
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [177.0786628443959, -37.9881848576018],
                        },
                        "properties": {
                            "fid": 2,
                            "t50_fid": 2426272,
                            "name_ascii": None,
                            "macronated": "N",
                            "name": None,
                        },
                        "id": "nz_pa_points_topo_150k:feature:2:U-",
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [177.0786628443959, -37.9881848576018],
                        },
                        "properties": {
                            "fid": 2,
                            "t50_fid": None,
                            "name_ascii": None,
                            "macronated": "N",
                            "name": "test",
                        },
                        "id": "nz_pa_points_topo_150k:feature:2:U+",
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [177.07125219628702, -37.97947548462757],
                        },
                        "properties": {
                            "fid": 3,
                            "t50_fid": 2426273,
                            "name_ascii": "Tauwhare Pa",
                            "macronated": "N",
                            "name": "Tauwhare Pa",
                        },
                        "id": "nz_pa_points_topo_150k:feature:3:D",
                    },
                    {
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
                        "properties": {
                            "fid": 9999,
                            "t50_fid": 9999999,
                            "name_ascii": "Te Motu-a-kore",
                            "macronated": "N",
                            "name": "Te Motu-a-kore",
                        },
                        "id": "nz_pa_points_topo_150k:feature:9999:I",
                    },
                ],
            }

        elif output_format == "json":
            assert r.exit_code == 0, r
            odata = json.loads(r.stdout)
            assert (
                len(odata["kart.diff/v1+hexwkb"]["nz_pa_points_topo_150k"]["feature"])
                == 4
            )
            assert odata == {
                "kart.diff/v1+hexwkb": {
                    "nz_pa_points_topo_150k": {
                        "feature": [
                            {
                                "+": {
                                    "fid": 9998,
                                    "geom": "010100000097F3EF201223664087D715268E0043C0",
                                    "macronated": "N",
                                    "name": None,
                                    "name_ascii": None,
                                    "t50_fid": 2426271,
                                },
                                "-": {
                                    "fid": 1,
                                    "geom": "010100000097F3EF201223664087D715268E0043C0",
                                    "macronated": "N",
                                    "name": None,
                                    "name_ascii": None,
                                    "t50_fid": 2426271,
                                },
                            },
                            {
                                "+": {
                                    "fid": 2,
                                    "geom": "0101000000E702F16784226640ADE666D77CFE42C0",
                                    "macronated": "N",
                                    "name": "test",
                                    "name_ascii": None,
                                    "t50_fid": None,
                                },
                                "-": {
                                    "fid": 2,
                                    "geom": "0101000000E702F16784226640ADE666D77CFE42C0",
                                    "macronated": "N",
                                    "name": None,
                                    "name_ascii": None,
                                    "t50_fid": 2426272,
                                },
                            },
                            {
                                "-": {
                                    "fid": 3,
                                    "geom": "0101000000459AAFB247226640C6DAE2735FFD42C0",
                                    "macronated": "N",
                                    "name": "Tauwhare Pa",
                                    "name_ascii": "Tauwhare Pa",
                                    "t50_fid": 2426273,
                                }
                            },
                            {
                                "+": {
                                    "fid": 9999,
                                    "geom": "010100000000000000000000000000000000000000",
                                    "macronated": "N",
                                    "name": "Te Motu-a-kore",
                                    "name_ascii": "Te Motu-a-kore",
                                    "t50_fid": 9999999,
                                }
                            },
                        ],
                    }
                }
            }

        elif output_format == "json-lines":
            assert r.exit_code == 0, r
            assert r.stdout.splitlines() == [
                '{"type":"version","version":"kart.diff/v2","outputFormat":"JSONL+hexwkb"}',
                '{"type":"datasetInfo","path":"nz_pa_points_topo_150k","value":{"type":"table","version":3}}',
                '{"type":"metaInfo","dataset":"nz_pa_points_topo_150k","key":"schema.json","value":[{"id":"e97b4015-2765-3a33-b174-2ece5c33343b","name":"fid","dataType":"integer","primaryKeyIndex":0,"size":64},{"id":"f488ae9b-6e15-1fe3-0bda-e0d5d38ea69e","name":"geom","dataType":"geometry","geometryType":"POINT","geometryCRS":"EPSG:4326"},{"id":"4a1c7a86-c425-ea77-7f1a-d74321a10edc","name":"t50_fid","dataType":"integer","size":32},{"id":"d2a62351-a66d-bde2-ce3e-356fec9641e9","name":"name_ascii","dataType":"text","length":75},{"id":"c3389414-a511-5385-7dcd-891c4ead1663","name":"macronated","dataType":"text","length":1},{"id":"45b00eaa-5700-662d-8a21-9614e40c437b","name":"name","dataType":"text","length":75}]}',
                '{"type":"feature","dataset":"nz_pa_points_topo_150k","change":{"-":{"fid":1,"geom":"010100000097F3EF201223664087D715268E0043C0","t50_fid":2426271,"name_ascii":null,"macronated":"N","name":null},"+":{"fid":9998,"geom":"010100000097F3EF201223664087D715268E0043C0","t50_fid":2426271,"name_ascii":null,"macronated":"N","name":null}}}',
                '{"type":"feature","dataset":"nz_pa_points_topo_150k","change":{"-":{"fid":2,"geom":"0101000000E702F16784226640ADE666D77CFE42C0","t50_fid":2426272,"name_ascii":null,"macronated":"N","name":null},"+":{"fid":2,"geom":"0101000000E702F16784226640ADE666D77CFE42C0","t50_fid":null,"name_ascii":null,"macronated":"N","name":"test"}}}',
                '{"type":"feature","dataset":"nz_pa_points_topo_150k","change":{"-":{"fid":3,"geom":"0101000000459AAFB247226640C6DAE2735FFD42C0","t50_fid":2426273,"name_ascii":"Tauwhare Pa","macronated":"N","name":"Tauwhare Pa"}}}',
                '{"type":"feature","dataset":"nz_pa_points_topo_150k","change":{"+":{"fid":9999,"geom":"010100000000000000000000000000000000000000","t50_fid":9999999,"name_ascii":"Te Motu-a-kore","macronated":"N","name":"Te Motu-a-kore"}}}',
            ]

        elif output_format == "html":
            _check_html_output(r.stdout)


@pytest.mark.slow
def test_diff_json_lines_with_feature_count_estimate(
    data_working_copy, cli_runner, monkeypatch
):
    with data_working_copy("points") as (repo_path, wc):

        def slow_down_the_diff(self, ds_path, ds_diff, diff_format=DiffFormat.FULL):
            time.sleep(1)

        # this is a tiny diff, but we make it arbitrarily slower so that we have time to generate the estimate and insert it into the stream
        monkeypatch.setattr(JsonLinesDiffWriter, "write_ds_diff", slow_down_the_diff)

        r = cli_runner.invoke(
            [
                "diff",
                f"--output-format=json-lines",
                "--add-feature-count-estimate=exact",
                "HEAD^^?...",
                "nz_pa_points_topo_150k:feature",  # suppress file diff
            ]
        )

        assert r.exit_code == 0
        lines = [json.loads(line) for line in r.stdout.splitlines()]
        assert lines == [
            {
                "type": "version",
                "version": "kart.diff/v2",
                "outputFormat": "JSONL+hexwkb",
            },
            {
                "type": "featureCountEstimate",
                "accuracy": "exact",
                "datasets": {"nz_pa_points_topo_150k": 2143},
            },
        ]


def test_diff_doesnt_evaluate_all_deltas_up_front_if_you_dont_sort_keys(
    data_archive_readonly, monkeypatch, cli_runner
):
    # Test that we can start outputting features before we have instantiated all the feature deltas.
    # Otherwise, diffs containing millions of deltas will be slow to start, and will use a lot of memory
    # to buffer the deltas in memory.
    # We explicitly avoid doing that, when the users has asked for a `--no-sort-keys` diff.
    with data_archive_readonly("points") as repo_path:
        orig_delta_as_json = JsonLinesDiffWriter.delta_as_json
        features_written = 0

        def delta_as_json(self, *args, **kwargs):
            nonlocal features_written
            features_written += 1
            return orig_delta_as_json(self, *args, **kwargs)

        monkeypatch.setattr(JsonLinesDiffWriter, "delta_as_json", delta_as_json)
        orig_wrap_deltas_from_raw_diff = BaseDataset.wrap_deltas_from_raw_diff

        def wrap_deltas_from_raw_diff(self, *args, **kwargs):
            yield from orig_wrap_deltas_from_raw_diff(self, *args, **kwargs)
            if not features_written:
                pytest.fail(
                    "All deltas shouldn't be evaluated until some features are written"
                )

        monkeypatch.setattr(
            BaseDataset, "wrap_deltas_from_raw_diff", wrap_deltas_from_raw_diff
        )
        r = cli_runner.invoke(
            [
                "diff",
                f"--output-format=json-lines",
                "--no-sort-keys",
                "[EMPTY]...",
            ]
        )


@pytest.mark.parametrize("output_format", DIFF_OUTPUT_FORMATS)
def test_diff_reprojection(output_format, data_working_copy, cli_runner):
    """diff the working copy against HEAD"""
    with data_working_copy("points") as (repo_path, wc):
        # make some changes
        repo = KartRepo(repo_path)
        with repo.working_copy.tabular.session() as sess:
            r = sess.execute(
                f"UPDATE {H.POINTS.LAYER} SET name='test', t50_fid=NULL WHERE fid=2;"
            )
            assert r.rowcount == 1

        r = cli_runner.invoke(
            [
                "diff",
                f"--output-format={output_format}",
                "--output=-",
                f"--crs=epsg:2193",
            ]
        )

        def _check_geojson(featurecollection):
            features = featurecollection["features"]
            assert len(features) == 2
            assert features[0]["geometry"]["coordinates"] == [
                1958227.0621957763,
                5787640.540304652,
            ]
            assert features[1]["geometry"]["coordinates"] == [
                1958227.0621957763,
                5787640.540304652,
            ]

        if output_format == "quiet":
            assert r.exit_code == 1, r.stderr
            assert r.stdout == ""
        elif output_format == "text":
            assert r.exit_code == 0, r.stderr
        elif output_format == "geojson":
            assert r.exit_code == 0, r.stderr
            featurecollection = json.loads(r.stdout)
            _check_geojson(featurecollection)
        elif output_format == "json":
            assert r.exit_code == 0, r.stderr
            odata = json.loads(r.stdout)
            features = odata["kart.diff/v1+hexwkb"]["nz_pa_points_topo_150k"]["feature"]
            assert len(features) == 1
            expected_wkt = "POINT (1958227.06219578 5787640.54030465)"
            assert (
                hex_wkb_to_ogr(features[0]["+"]["geom"]).ExportToWkt() == expected_wkt
            )
            assert (
                hex_wkb_to_ogr(features[0]["-"]["geom"]).ExportToWkt() == expected_wkt
            )

        elif output_format == "html":
            odata = _check_html_output(r.stdout)
            _check_geojson(odata["nz_pa_points_topo_150k"])


def test_show_crs_with_aspatial_dataset(data_archive, cli_runner):
    """
    --crs should be ignored when used with aspatial data
    """
    with data_archive("table"):
        r = cli_runner.invoke(
            [
                "show",
                f"--output-format=json",
                f"--crs=epsg:2193",
            ]
        )
        assert r.exit_code == 0, r.stderr


@pytest.mark.parametrize("output_format", DIFF_OUTPUT_FORMATS)
def test_diff_polygons(output_format, data_working_copy, cli_runner):
    """diff the working copy against HEAD"""
    with data_working_copy("polygons") as (repo, wc):
        # empty
        r = cli_runner.invoke(
            ["diff", f"--output-format={output_format}", "--output=-", "--exit-code"]
        )
        assert r.exit_code == 0, r.stderr

        # make some changes
        repo = KartRepo(repo)
        with repo.working_copy.tabular.session() as sess:
            r = sess.execute(H.POLYGONS.INSERT, H.POLYGONS.RECORD)
            assert r.rowcount == 1
            r = sess.execute(f"UPDATE {H.POLYGONS.LAYER} SET id=9998 WHERE id=1424927;")
            assert r.rowcount == 1
            r = sess.execute(
                f"UPDATE {H.POLYGONS.LAYER} SET survey_reference='test', date_adjusted='2019-01-01T00:00:00Z' WHERE id=1443053;"
            )
            assert r.rowcount == 1
            r = sess.execute(f"DELETE FROM {H.POLYGONS.LAYER} WHERE id=1452332;")
            assert r.rowcount == 1

        r = cli_runner.invoke(
            ["diff", f"--output-format={output_format}", "--output=-"]
        )
        if output_format == "quiet":
            assert r.exit_code == 1, r
            assert r.stdout == ""
        elif output_format == "text":
            assert r.exit_code == 0, r
            assert r.stdout.splitlines() == [
                "--- nz_waca_adjustments:feature:1424927",
                "+++ nz_waca_adjustments:feature:9998",
                "-                                       id = 1424927",
                "+                                       id = 9998",
                "--- nz_waca_adjustments:feature:1443053",
                "+++ nz_waca_adjustments:feature:1443053",
                "-                            date_adjusted = 2011-05-10T12:09:10",
                "+                            date_adjusted = 2019-01-01T00:00:00",
                "-                         survey_reference = ␀",
                "+                         survey_reference = test",
                "--- nz_waca_adjustments:feature:1452332",
                "-                                       id = 1452332",
                "-                                     geom = MULTIPOLYGON(...)",
                "-                            date_adjusted = 2011-06-07T15:22:58",
                "-                         survey_reference = ␀",
                "-                           adjusted_nodes = 558",
                "+++ nz_waca_adjustments:feature:9999999",
                "+                                       id = 9999999",
                "+                                     geom = MULTIPOLYGON(...)",
                "+                            date_adjusted = 2019-07-05T13:04:00",
                "+                         survey_reference = Null Island™ 🗺",
                "+                           adjusted_nodes = 123",
            ]

        elif output_format == "geojson":
            assert r.exit_code == 0, r
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
                                        [175.36501955, -37.8677371333],
                                        [175.3594248167, -37.8596774667],
                                        [175.3587766, -37.8587394],
                                        [175.3575281667, -37.8568299667],
                                        [175.3573468, -37.8564048833],
                                        [175.3503192167, -37.8384090167],
                                        [175.3516358, -37.8348565833],
                                        [175.3577393167, -37.8277652167],
                                        [175.3581963667, -37.8273121833],
                                        [175.3613082667, -37.8270649667],
                                        [175.3843470333, -37.84913405],
                                        [175.38430045, -37.8493045833],
                                        [175.3774678333, -37.8602782667],
                                        [175.3750135667, -37.8641522],
                                        [175.3739396667, -37.8658466833],
                                        [175.3726953667, -37.8674995333],
                                        [175.3725163333, -37.86759125],
                                        [175.36501955, -37.8677371333],
                                    ]
                                ]
                            ],
                        },
                        "properties": {
                            "id": 1424927,
                            "date_adjusted": "2011-03-25T07:30:45",
                            "survey_reference": None,
                            "adjusted_nodes": 1122,
                        },
                        "id": "nz_waca_adjustments:feature:1424927:U-",
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "MultiPolygon",
                            "coordinates": [
                                [
                                    [
                                        [175.36501955, -37.8677371333],
                                        [175.3594248167, -37.8596774667],
                                        [175.3587766, -37.8587394],
                                        [175.3575281667, -37.8568299667],
                                        [175.3573468, -37.8564048833],
                                        [175.3503192167, -37.8384090167],
                                        [175.3516358, -37.8348565833],
                                        [175.3577393167, -37.8277652167],
                                        [175.3581963667, -37.8273121833],
                                        [175.3613082667, -37.8270649667],
                                        [175.3843470333, -37.84913405],
                                        [175.38430045, -37.8493045833],
                                        [175.3774678333, -37.8602782667],
                                        [175.3750135667, -37.8641522],
                                        [175.3739396667, -37.8658466833],
                                        [175.3726953667, -37.8674995333],
                                        [175.3725163333, -37.86759125],
                                        [175.36501955, -37.8677371333],
                                    ]
                                ]
                            ],
                        },
                        "properties": {
                            "id": 9998,
                            "date_adjusted": "2011-03-25T07:30:45",
                            "survey_reference": None,
                            "adjusted_nodes": 1122,
                        },
                        "id": "nz_waca_adjustments:feature:9998:U+",
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "MultiPolygon",
                            "coordinates": [
                                [
                                    [
                                        [174.2166180833, -39.1160069167],
                                        [174.2105324333, -39.0628896333],
                                        [174.2187671333, -39.0444813667],
                                        [174.2336286, -39.0435761833],
                                        [174.2489834333, -39.0673477167],
                                        [174.2371150833, -39.1042998],
                                        [174.2370479667, -39.1043865],
                                        [174.2230324667, -39.11499395],
                                        [174.2221168, -39.11534705],
                                        [174.2199784667, -39.1158339833],
                                        [174.2166180833, -39.1160069167],
                                    ]
                                ]
                            ],
                        },
                        "properties": {
                            "id": 1443053,
                            "date_adjusted": "2011-05-10T12:09:10",
                            "survey_reference": None,
                            "adjusted_nodes": 1238,
                        },
                        "id": "nz_waca_adjustments:feature:1443053:U-",
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "MultiPolygon",
                            "coordinates": [
                                [
                                    [
                                        [174.2166180833, -39.1160069167],
                                        [174.2105324333, -39.0628896333],
                                        [174.2187671333, -39.0444813667],
                                        [174.2336286, -39.0435761833],
                                        [174.2489834333, -39.0673477167],
                                        [174.2371150833, -39.1042998],
                                        [174.2370479667, -39.1043865],
                                        [174.2230324667, -39.11499395],
                                        [174.2221168, -39.11534705],
                                        [174.2199784667, -39.1158339833],
                                        [174.2166180833, -39.1160069167],
                                    ]
                                ]
                            ],
                        },
                        "properties": {
                            "id": 1443053,
                            "date_adjusted": "2019-01-01T00:00:00",
                            "survey_reference": "test",
                            "adjusted_nodes": 1238,
                        },
                        "id": "nz_waca_adjustments:feature:1443053:U+",
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "MultiPolygon",
                            "coordinates": [
                                [
                                    [
                                        [174.7311576833, -36.79928385],
                                        [174.7304707167, -36.79640095],
                                        [174.7304722, -36.7963232833],
                                        [174.7312468333, -36.7955355667],
                                        [174.7317962167, -36.7951379833],
                                        [174.7318702333, -36.7950879667],
                                        [174.7318998167, -36.7950707167],
                                        [174.73205185, -36.7949820833],
                                        [174.7322039, -36.79489345],
                                        [174.7328121333, -36.7945389],
                                        [174.7331398833, -36.79434785],
                                        [174.7333079833, -36.7942498667],
                                        [174.7333417167, -36.7942316667],
                                        [174.7337021, -36.7941665],
                                        [174.7339906833, -36.7942629333],
                                        [174.73428805, -36.7944331167],
                                        [174.7365411333, -36.7964726167],
                                        [174.73656865, -36.7965525667],
                                        [174.7365538333, -36.796667],
                                        [174.7363353, -36.79687885],
                                        [174.7361800167, -36.7970018167],
                                        [174.7329695167, -36.79907145],
                                        [174.7326544833, -36.7992142],
                                        [174.7311576833, -36.79928385],
                                    ]
                                ]
                            ],
                        },
                        "properties": {
                            "id": 1452332,
                            "date_adjusted": "2011-06-07T15:22:58",
                            "survey_reference": None,
                            "adjusted_nodes": 558,
                        },
                        "id": "nz_waca_adjustments:feature:1452332:D",
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "MultiPolygon",
                            "coordinates": [
                                [
                                    [
                                        [0.0, 0.0],
                                        [0.0, 0.001],
                                        [0.001, 0.001],
                                        [0.001, 0.0],
                                        [0.0, 0.0],
                                    ]
                                ]
                            ],
                        },
                        "properties": {
                            "id": 9999999,
                            "date_adjusted": "2019-07-05T13:04:00",
                            "survey_reference": "Null Island™ 🗺",
                            "adjusted_nodes": 123,
                        },
                        "id": "nz_waca_adjustments:feature:9999999:I",
                    },
                ],
            }

        elif output_format == "json":
            assert r.exit_code == 0, r
            odata = json.loads(r.stdout)
            assert (
                len(odata["kart.diff/v1+hexwkb"]["nz_waca_adjustments"]["feature"]) == 4
            )
            assert odata == {
                "kart.diff/v1+hexwkb": {
                    "nz_waca_adjustments": {
                        "feature": [
                            {
                                "+": {
                                    "adjusted_nodes": 1122,
                                    "date_adjusted": "2011-03-25T07:30:45",
                                    "geom": "01060000000100000001030000000100000012000000D2B47A3DAEEB65402E86A80212EF42C01D23796880EB6540D54A46E909EE42C03E7210197BEB6540B164332CEBED42C003ECE8DE70EB6540C99AB69AACED42C0916A8E626FEB654040F4DAAC9EED42C0615CA5D035EB6540F2B295FC50EB42C04AA3B89940EB6540D90F9D94DCEA42C00937B99972EB6540163FEB35F4E942C0B9103A5876EB65408D6D995DE5E942C008A85AD68FEB654069D2CB43DDE942C0D24A26924CEC6540C455AF6CB0EC42C0D21275304CEC6540E6CE3803B6EC42C018EA6B3714EC6540D17726991DEE42C00D91731C00EC65401BE20E8A9CEE42C0EBE45150F7EB6540D10F6A10D4EE42C01C6BD51EEDEB6540CD6886390AEF42C0FB975FA7EBEB6540DB85E63A0DEF42C0D2B47A3DAEEB65402E86A80212EF42C0",
                                    "id": 9998,
                                    "survey_reference": None,
                                },
                                "-": {
                                    "adjusted_nodes": 1122,
                                    "date_adjusted": "2011-03-25T07:30:45",
                                    "geom": "01060000000100000001030000000100000012000000D2B47A3DAEEB65402E86A80212EF42C01D23796880EB6540D54A46E909EE42C03E7210197BEB6540B164332CEBED42C003ECE8DE70EB6540C99AB69AACED42C0916A8E626FEB654040F4DAAC9EED42C0615CA5D035EB6540F2B295FC50EB42C04AA3B89940EB6540D90F9D94DCEA42C00937B99972EB6540163FEB35F4E942C0B9103A5876EB65408D6D995DE5E942C008A85AD68FEB654069D2CB43DDE942C0D24A26924CEC6540C455AF6CB0EC42C0D21275304CEC6540E6CE3803B6EC42C018EA6B3714EC6540D17726991DEE42C00D91731C00EC65401BE20E8A9CEE42C0EBE45150F7EB6540D10F6A10D4EE42C01C6BD51EEDEB6540CD6886390AEF42C0FB975FA7EBEB6540DB85E63A0DEF42C0D2B47A3DAEEB65402E86A80212EF42C0",
                                    "id": 1424927,
                                    "survey_reference": None,
                                },
                            },
                            {
                                "+": {
                                    "adjusted_nodes": 1238,
                                    "date_adjusted": "2019-01-01T00:00:00",
                                    "geom": "0106000000010000000103000000010000000B000000DDEF0B89EEC665400CAB8C50D98E43C0AA7883AEBCC66540F6237BC40C8843C0D25EEE2300C7654002A1BF90B18543C0218DAFE279C76540391485E7938543C09EE81AACF7C76540E85798D99E8843C02E055F7296C765405BFD22B2598D43C0EA119EE595C765406BD26D895C8D43C087CDFB1423C76540723E2B1FB88E43C08DFCB0941BC7654054B82FB1C38E43C0A00948100AC76540FB04E1A5D38E43C0DDEF0B89EEC665400CAB8C50D98E43C0",
                                    "id": 1443053,
                                    "survey_reference": "test",
                                },
                                "-": {
                                    "adjusted_nodes": 1238,
                                    "date_adjusted": "2011-05-10T12:09:10",
                                    "geom": "0106000000010000000103000000010000000B000000DDEF0B89EEC665400CAB8C50D98E43C0AA7883AEBCC66540F6237BC40C8843C0D25EEE2300C7654002A1BF90B18543C0218DAFE279C76540391485E7938543C09EE81AACF7C76540E85798D99E8843C02E055F7296C765405BFD22B2598D43C0EA119EE595C765406BD26D895C8D43C087CDFB1423C76540723E2B1FB88E43C08DFCB0941BC7654054B82FB1C38E43C0A00948100AC76540FB04E1A5D38E43C0DDEF0B89EEC665400CAB8C50D98E43C0",
                                    "id": 1443053,
                                    "survey_reference": None,
                                },
                            },
                            {
                                "-": {
                                    "adjusted_nodes": 558,
                                    "date_adjusted": "2011-06-07T15:22:58",
                                    "geom": "01060000000100000001030000000100000018000000C43FCCA465D7654049FCE5EE4E6642C031DD1F0460D765406D606177F06542C064343C0760D765408E68DDEBED6542C0774AC25F66D7654003E4041CD46542C00442E6DF6AD765405B0AD914C76542C00F9E1F7B6BD76540B7354771C56542C099152AB96BD76540ED1D93E0C46542C03E5700F86CD76540F85610F9C16542C01E90DF366ED76540FDC68D11BF6542C056546E3273D765402D735F73B36542C056C5C5E175D76540EFB2BA30AD6542C06AC54D4277D76540182AC9FAA96542C09C400C8977D7654048F61C62A96542C03590D37C7AD76540168A743FA76542C0F38A07DA7CD7654069796568AA6542C0FF12A7497FD76540FD8AFFFBAF6542C0D5F5B5BE91D765406A7190D0F26542C049E06AF891D76540BCC23B6FF56542C08B3858D991D76540B6662B2FF96542C07E0C0C0F90D76540E2CF4B20006642C03FF664C98ED7654020CAD027046642C020E67C7C74D765406A7528F9476642C052A1D0E771D76540D9BFA1A64C6642C0C43FCCA465D7654049FCE5EE4E6642C0",
                                    "id": 1452332,
                                    "survey_reference": None,
                                }
                            },
                            {
                                "+": {
                                    "adjusted_nodes": 123,
                                    "date_adjusted": "2019-07-05T13:04:00",
                                    "geom": "01060000000100000001030000000100000005000000000000000000000000000000000000000000000000000000FCA9F1D24D62503FFCA9F1D24D62503FFCA9F1D24D62503FFCA9F1D24D62503F000000000000000000000000000000000000000000000000",
                                    "id": 9999999,
                                    "survey_reference": "Null Island™ 🗺",
                                }
                            },
                        ],
                    }
                }
            }

        elif output_format == "json-lines":
            assert r.exit_code == 0, r
            assert r.stdout.splitlines() == [
                '{"type":"version","version":"kart.diff/v2","outputFormat":"JSONL+hexwkb"}',
                '{"type":"datasetInfo","path":"nz_waca_adjustments","value":{"type":"table","version":3}}',
                '{"type":"metaInfo","dataset":"nz_waca_adjustments","key":"schema.json","value":[{"id":"79d3c4ca-3abd-0a30-2045-45169357113c","name":"id","dataType":"integer","primaryKeyIndex":0,"size":64},{"id":"c1d4dea1-c0ad-0255-7857-b5695e3ba2e9","name":"geom","dataType":"geometry","geometryType":"MULTIPOLYGON","geometryCRS":"EPSG:4167"},{"id":"d3d4b64b-d48e-4069-4bb5-dfa943d91e6b","name":"date_adjusted","dataType":"timestamp","timezone":"UTC"},{"id":"dff34196-229d-f0b5-7fd4-b14ecf835b2c","name":"survey_reference","dataType":"text","length":50},{"id":"13dc4918-974e-978f-05ce-3b4321077c50","name":"adjusted_nodes","dataType":"integer","size":32}]}',
                '{"type":"feature","dataset":"nz_waca_adjustments","change":{"-":{"id":1424927,"geom":"01060000000100000001030000000100000012000000D2B47A3DAEEB65402E86A80212EF42C01D23796880EB6540D54A46E909EE42C03E7210197BEB6540B164332CEBED42C003ECE8DE70EB6540C99AB69AACED42C0916A8E626FEB654040F4DAAC9EED42C0615CA5D035EB6540F2B295FC50EB42C04AA3B89940EB6540D90F9D94DCEA42C00937B99972EB6540163FEB35F4E942C0B9103A5876EB65408D6D995DE5E942C008A85AD68FEB654069D2CB43DDE942C0D24A26924CEC6540C455AF6CB0EC42C0D21275304CEC6540E6CE3803B6EC42C018EA6B3714EC6540D17726991DEE42C00D91731C00EC65401BE20E8A9CEE42C0EBE45150F7EB6540D10F6A10D4EE42C01C6BD51EEDEB6540CD6886390AEF42C0FB975FA7EBEB6540DB85E63A0DEF42C0D2B47A3DAEEB65402E86A80212EF42C0","date_adjusted":"2011-03-25T07:30:45","survey_reference":null,"adjusted_nodes":1122},"+":{"id":9998,"geom":"01060000000100000001030000000100000012000000D2B47A3DAEEB65402E86A80212EF42C01D23796880EB6540D54A46E909EE42C03E7210197BEB6540B164332CEBED42C003ECE8DE70EB6540C99AB69AACED42C0916A8E626FEB654040F4DAAC9EED42C0615CA5D035EB6540F2B295FC50EB42C04AA3B89940EB6540D90F9D94DCEA42C00937B99972EB6540163FEB35F4E942C0B9103A5876EB65408D6D995DE5E942C008A85AD68FEB654069D2CB43DDE942C0D24A26924CEC6540C455AF6CB0EC42C0D21275304CEC6540E6CE3803B6EC42C018EA6B3714EC6540D17726991DEE42C00D91731C00EC65401BE20E8A9CEE42C0EBE45150F7EB6540D10F6A10D4EE42C01C6BD51EEDEB6540CD6886390AEF42C0FB975FA7EBEB6540DB85E63A0DEF42C0D2B47A3DAEEB65402E86A80212EF42C0","date_adjusted":"2011-03-25T07:30:45","survey_reference":null,"adjusted_nodes":1122}}}',
                '{"type":"feature","dataset":"nz_waca_adjustments","change":{"-":{"id":1443053,"geom":"0106000000010000000103000000010000000B000000DDEF0B89EEC665400CAB8C50D98E43C0AA7883AEBCC66540F6237BC40C8843C0D25EEE2300C7654002A1BF90B18543C0218DAFE279C76540391485E7938543C09EE81AACF7C76540E85798D99E8843C02E055F7296C765405BFD22B2598D43C0EA119EE595C765406BD26D895C8D43C087CDFB1423C76540723E2B1FB88E43C08DFCB0941BC7654054B82FB1C38E43C0A00948100AC76540FB04E1A5D38E43C0DDEF0B89EEC665400CAB8C50D98E43C0","date_adjusted":"2011-05-10T12:09:10","survey_reference":null,"adjusted_nodes":1238},"+":{"id":1443053,"geom":"0106000000010000000103000000010000000B000000DDEF0B89EEC665400CAB8C50D98E43C0AA7883AEBCC66540F6237BC40C8843C0D25EEE2300C7654002A1BF90B18543C0218DAFE279C76540391485E7938543C09EE81AACF7C76540E85798D99E8843C02E055F7296C765405BFD22B2598D43C0EA119EE595C765406BD26D895C8D43C087CDFB1423C76540723E2B1FB88E43C08DFCB0941BC7654054B82FB1C38E43C0A00948100AC76540FB04E1A5D38E43C0DDEF0B89EEC665400CAB8C50D98E43C0","date_adjusted":"2019-01-01T00:00:00","survey_reference":"test","adjusted_nodes":1238}}}',
                '{"type":"feature","dataset":"nz_waca_adjustments","change":{"-":{"id":1452332,"geom":"01060000000100000001030000000100000018000000C43FCCA465D7654049FCE5EE4E6642C031DD1F0460D765406D606177F06542C064343C0760D765408E68DDEBED6542C0774AC25F66D7654003E4041CD46542C00442E6DF6AD765405B0AD914C76542C00F9E1F7B6BD76540B7354771C56542C099152AB96BD76540ED1D93E0C46542C03E5700F86CD76540F85610F9C16542C01E90DF366ED76540FDC68D11BF6542C056546E3273D765402D735F73B36542C056C5C5E175D76540EFB2BA30AD6542C06AC54D4277D76540182AC9FAA96542C09C400C8977D7654048F61C62A96542C03590D37C7AD76540168A743FA76542C0F38A07DA7CD7654069796568AA6542C0FF12A7497FD76540FD8AFFFBAF6542C0D5F5B5BE91D765406A7190D0F26542C049E06AF891D76540BCC23B6FF56542C08B3858D991D76540B6662B2FF96542C07E0C0C0F90D76540E2CF4B20006642C03FF664C98ED7654020CAD027046642C020E67C7C74D765406A7528F9476642C052A1D0E771D76540D9BFA1A64C6642C0C43FCCA465D7654049FCE5EE4E6642C0","date_adjusted":"2011-06-07T15:22:58","survey_reference":null,"adjusted_nodes":558}}}',
                '{"type":"feature","dataset":"nz_waca_adjustments","change":{"+":{"id":9999999,"geom":"01060000000100000001030000000100000005000000000000000000000000000000000000000000000000000000FCA9F1D24D62503FFCA9F1D24D62503FFCA9F1D24D62503FFCA9F1D24D62503F000000000000000000000000000000000000000000000000","date_adjusted":"2019-07-05T13:04:00","survey_reference":"Null Island™ 🗺","adjusted_nodes":123}}}',
            ]

        elif output_format == "html":
            _check_html_output(r.stdout)


@pytest.mark.parametrize("output_format", DIFF_OUTPUT_FORMATS)
def test_diff_table(output_format, data_working_copy, cli_runner):
    """diff the working copy against HEAD"""
    with data_working_copy("table") as (repo_path, wc):
        # empty
        r = cli_runner.invoke(
            ["diff", f"--output-format={output_format}", "--output=-", "--exit-code"]
        )
        assert r.exit_code == 0, r.stderr

        # make some changes
        repo = KartRepo(repo_path)
        with repo.working_copy.tabular.session() as sess:
            r = sess.execute(H.TABLE.INSERT, H.TABLE.RECORD)
            assert r.rowcount == 1
            r = sess.execute(
                f'UPDATE {H.TABLE.LAYER} SET "OBJECTID"=9998 WHERE OBJECTID=1;'
            )
            assert r.rowcount == 1
            r = sess.execute(
                f"UPDATE {H.TABLE.LAYER} SET name='test', POP2000=9867 WHERE OBJECTID=2;"
            )
            assert r.rowcount == 1
            r = sess.execute(f'DELETE FROM {H.TABLE.LAYER} WHERE "OBJECTID"=3;')
            assert r.rowcount == 1

        r = cli_runner.invoke(
            ["diff", f"--output-format={output_format}", "--output=-"]
        )
        if output_format == "quiet":
            assert r.exit_code == 1, r
            assert r.stdout == ""
        elif output_format == "text":
            assert r.exit_code == 0, r
            assert r.stdout.splitlines() == [
                "--- countiestbl:feature:1",
                "+++ countiestbl:feature:9998",
                "-                                 OBJECTID = 1",
                "+                                 OBJECTID = 9998",
                "--- countiestbl:feature:2",
                "+++ countiestbl:feature:2",
                "-                                     NAME = Ferry",
                "+                                     NAME = test",
                "-                                  POP2000 = 7199.0",
                "+                                  POP2000 = 9867.0",
                "--- countiestbl:feature:3",
                "-                                 OBJECTID = 3",
                "-                                     NAME = Stevens",
                "-                               STATE_NAME = Washington",
                "-                               STATE_FIPS = 53",
                "-                                CNTY_FIPS = 065",
                "-                                     FIPS = 53065",
                "-                                     AREA = 2529.9794",
                "-                                  POP1990 = 30948.0",
                "-                                  POP2000 = 40652.0",
                "-                               POP90_SQMI = 12",
                "-                               Shape_Leng = 4.876296245235406",
                "-                               Shape_Area = 0.7954858988987561",
                "+++ countiestbl:feature:9999",
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
            ]

        elif output_format == "geojson":
            assert r.exit_code == 0, r
            odata = json.loads(r.stdout)
            assert len(odata["features"]) == 6
            assert odata == {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": None,
                        "properties": {
                            "OBJECTID": 1,
                            "NAME": "Lake of the Woods",
                            "STATE_NAME": "Minnesota",
                            "STATE_FIPS": "27",
                            "CNTY_FIPS": "077",
                            "FIPS": "27077",
                            "AREA": 1784.0634,
                            "POP1990": 4076.0,
                            "POP2000": 4651.0,
                            "POP90_SQMI": 2,
                            "Shape_Leng": 4.055459982439919,
                            "Shape_Area": 0.5654499337414509,
                        },
                        "id": "countiestbl:feature:1:U-",
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
                            "Shape_Leng": 4.055459982439919,
                            "Shape_Area": 0.5654499337414509,
                        },
                        "id": "countiestbl:feature:9998:U+",
                    },
                    {
                        "type": "Feature",
                        "geometry": None,
                        "properties": {
                            "OBJECTID": 2,
                            "NAME": "Ferry",
                            "STATE_NAME": "Washington",
                            "STATE_FIPS": "53",
                            "CNTY_FIPS": "019",
                            "FIPS": "53019",
                            "AREA": 2280.2319,
                            "POP1990": 6295.0,
                            "POP2000": 7199.0,
                            "POP90_SQMI": 3,
                            "Shape_Leng": 3.786160993863997,
                            "Shape_Area": 0.7180593026451161,
                        },
                        "id": "countiestbl:feature:2:U-",
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
                            "Shape_Leng": 3.786160993863997,
                            "Shape_Area": 0.7180593026451161,
                        },
                        "id": "countiestbl:feature:2:U+",
                    },
                    {
                        "type": "Feature",
                        "geometry": None,
                        "properties": {
                            "OBJECTID": 3,
                            "NAME": "Stevens",
                            "STATE_NAME": "Washington",
                            "STATE_FIPS": "53",
                            "CNTY_FIPS": "065",
                            "FIPS": "53065",
                            "AREA": 2529.9794,
                            "POP1990": 30948.0,
                            "POP2000": 40652.0,
                            "POP90_SQMI": 12,
                            "Shape_Leng": 4.876296245235406,
                            "Shape_Area": 0.7954858988987561,
                        },
                        "id": "countiestbl:feature:3:D",
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
                            "Shape_Leng": 4.05545998243992,
                            "Shape_Area": 0.565449933741451,
                        },
                        "id": "countiestbl:feature:9999:I",
                    },
                ],
            }

        elif output_format == "json":
            assert r.exit_code == 0, r
            odata = json.loads(r.stdout)
            assert len(odata["kart.diff/v1+hexwkb"]["countiestbl"]["feature"]) == 4
            assert odata == {
                "kart.diff/v1+hexwkb": {
                    "countiestbl": {
                        "feature": [
                            {
                                "+": {
                                    "AREA": 1784.0634,
                                    "CNTY_FIPS": "077",
                                    "FIPS": "27077",
                                    "NAME": "Lake of the Woods",
                                    "OBJECTID": 9998,
                                    "POP1990": 4076.0,
                                    "POP2000": 4651.0,
                                    "POP90_SQMI": 2,
                                    "STATE_FIPS": "27",
                                    "STATE_NAME": "Minnesota",
                                    "Shape_Area": 0.5654499337414509,
                                    "Shape_Leng": 4.055459982439919,
                                },
                                "-": {
                                    "AREA": 1784.0634,
                                    "CNTY_FIPS": "077",
                                    "FIPS": "27077",
                                    "NAME": "Lake of the Woods",
                                    "OBJECTID": 1,
                                    "POP1990": 4076.0,
                                    "POP2000": 4651.0,
                                    "POP90_SQMI": 2,
                                    "STATE_FIPS": "27",
                                    "STATE_NAME": "Minnesota",
                                    "Shape_Area": 0.5654499337414509,
                                    "Shape_Leng": 4.055459982439919,
                                },
                            },
                            {
                                "+": {
                                    "AREA": 2280.2319,
                                    "CNTY_FIPS": "019",
                                    "FIPS": "53019",
                                    "NAME": "test",
                                    "OBJECTID": 2,
                                    "POP1990": 6295.0,
                                    "POP2000": 9867.0,
                                    "POP90_SQMI": 3,
                                    "STATE_FIPS": "53",
                                    "STATE_NAME": "Washington",
                                    "Shape_Area": 0.7180593026451161,
                                    "Shape_Leng": 3.786160993863997,
                                },
                                "-": {
                                    "AREA": 2280.2319,
                                    "CNTY_FIPS": "019",
                                    "FIPS": "53019",
                                    "NAME": "Ferry",
                                    "OBJECTID": 2,
                                    "POP1990": 6295.0,
                                    "POP2000": 7199.0,
                                    "POP90_SQMI": 3,
                                    "STATE_FIPS": "53",
                                    "STATE_NAME": "Washington",
                                    "Shape_Area": 0.7180593026451161,
                                    "Shape_Leng": 3.786160993863997,
                                },
                            },
                            {
                                "-": {
                                    "AREA": 2529.9794,
                                    "CNTY_FIPS": "065",
                                    "FIPS": "53065",
                                    "NAME": "Stevens",
                                    "OBJECTID": 3,
                                    "POP1990": 30948.0,
                                    "POP2000": 40652.0,
                                    "POP90_SQMI": 12,
                                    "STATE_FIPS": "53",
                                    "STATE_NAME": "Washington",
                                    "Shape_Area": 0.7954858988987561,
                                    "Shape_Leng": 4.876296245235406,
                                }
                            },
                            {
                                "+": {
                                    "AREA": 1784.0634,
                                    "CNTY_FIPS": "077",
                                    "FIPS": "27077",
                                    "NAME": "Lake of the Gruffalo",
                                    "OBJECTID": 9999,
                                    "POP1990": 4076.0,
                                    "POP2000": 4651.0,
                                    "POP90_SQMI": 2,
                                    "STATE_FIPS": "27",
                                    "STATE_NAME": "Minnesota",
                                    "Shape_Area": 0.565449933741451,
                                    "Shape_Leng": 4.05545998243992,
                                }
                            },
                        ],
                    }
                }
            }
        elif output_format == "json-lines":
            assert r.exit_code == 0, r
            assert r.stdout.splitlines() == [
                '{"type":"version","version":"kart.diff/v2","outputFormat":"JSONL+hexwkb"}',
                '{"type":"datasetInfo","path":"countiestbl","value":{"type":"table","version":3}}',
                '{"type":"metaInfo","dataset":"countiestbl","key":"schema.json","value":[{"id":"1ec8704f-4d90-08ca-8a94-2cc59dcf63bd","name":"OBJECTID","dataType":"integer","primaryKeyIndex":0,"size":64},{"id":"e160628f-d812-2b8d-4507-585e9e4950e2","name":"NAME","dataType":"text","length":32},{"id":"1b9bc047-3358-2b0a-6350-861d0f3cb91b","name":"STATE_NAME","dataType":"text","length":25},{"id":"b8ee8bdc-eeb3-776d-556e-052e6138172b","name":"STATE_FIPS","dataType":"text","length":2},{"id":"a3c0c4b5-b2c8-8b67-2832-e75612668d98","name":"CNTY_FIPS","dataType":"text","length":3},{"id":"000d0d61-4e56-a711-5b95-68d744c96cef","name":"FIPS","dataType":"text","length":5},{"id":"a75c1017-b76f-d2ea-d3e9-48845bec11ff","name":"AREA","dataType":"float","size":64},{"id":"e28f5b52-e90c-88ca-928e-0e3b0046cb2a","name":"POP1990","dataType":"float","size":64},{"id":"c9dd2446-3d80-e799-9a87-72088b4d79df","name":"POP2000","dataType":"float","size":64},{"id":"bb6c3a7a-7ed5-d384-aed5-b328fb2adf0b","name":"POP90_SQMI","dataType":"integer","size":32},{"id":"e5ab8cbf-f768-0ef4-e685-b6cacf5075bf","name":"Shape_Leng","dataType":"float","size":64},{"id":"45e4a30c-8dc1-4210-8843-88d28eb5d3b2","name":"Shape_Area","dataType":"float","size":64}]}',
                '{"type":"feature","dataset":"countiestbl","change":{"-":{"OBJECTID":1,"NAME":"Lake of the Woods","STATE_NAME":"Minnesota","STATE_FIPS":"27","CNTY_FIPS":"077","FIPS":"27077","AREA":1784.0634,"POP1990":4076.0,"POP2000":4651.0,"POP90_SQMI":2,"Shape_Leng":4.055459982439919,"Shape_Area":0.5654499337414509},"+":{"OBJECTID":9998,"NAME":"Lake of the Woods","STATE_NAME":"Minnesota","STATE_FIPS":"27","CNTY_FIPS":"077","FIPS":"27077","AREA":1784.0634,"POP1990":4076.0,"POP2000":4651.0,"POP90_SQMI":2,"Shape_Leng":4.055459982439919,"Shape_Area":0.5654499337414509}}}',
                '{"type":"feature","dataset":"countiestbl","change":{"-":{"OBJECTID":2,"NAME":"Ferry","STATE_NAME":"Washington","STATE_FIPS":"53","CNTY_FIPS":"019","FIPS":"53019","AREA":2280.2319,"POP1990":6295.0,"POP2000":7199.0,"POP90_SQMI":3,"Shape_Leng":3.786160993863997,"Shape_Area":0.7180593026451161},"+":{"OBJECTID":2,"NAME":"test","STATE_NAME":"Washington","STATE_FIPS":"53","CNTY_FIPS":"019","FIPS":"53019","AREA":2280.2319,"POP1990":6295.0,"POP2000":9867.0,"POP90_SQMI":3,"Shape_Leng":3.786160993863997,"Shape_Area":0.7180593026451161}}}',
                '{"type":"feature","dataset":"countiestbl","change":{"-":{"OBJECTID":3,"NAME":"Stevens","STATE_NAME":"Washington","STATE_FIPS":"53","CNTY_FIPS":"065","FIPS":"53065","AREA":2529.9794,"POP1990":30948.0,"POP2000":40652.0,"POP90_SQMI":12,"Shape_Leng":4.876296245235406,"Shape_Area":0.7954858988987561}}}',
                '{"type":"feature","dataset":"countiestbl","change":{"+":{"OBJECTID":9999,"NAME":"Lake of the Gruffalo","STATE_NAME":"Minnesota","STATE_FIPS":"27","CNTY_FIPS":"077","FIPS":"27077","AREA":1784.0634,"POP1990":4076.0,"POP2000":4651.0,"POP90_SQMI":2,"Shape_Leng":4.05545998243992,"Shape_Area":0.565449933741451}}}',
            ]

        elif output_format == "html":
            _check_html_output(r.stdout)


@pytest.mark.parametrize(
    "head_sha,head1_sha",
    [
        pytest.param(H.POINTS.HEAD_SHA, H.POINTS.HEAD1_SHA, id="commit_hash"),
        pytest.param(H.POINTS.HEAD_TREE_SHA, H.POINTS.HEAD1_TREE_SHA, id="tree_hash"),
    ],
)
def test_diff_rev_noop(head_sha, head1_sha, data_archive_readonly, cli_runner):
    """diff between trees / commits - no-op"""

    NOOP_SPECS = (
        f"{head_sha[:6]}...{head_sha[:6]}",
        f"{head_sha}...{head_sha}",
        f"{head1_sha}...{head1_sha}",
        "HEAD^1...HEAD^1",
        f"{head_sha}...",
        f"...{head_sha}",
    )

    with data_archive_readonly("points"):
        for spec in NOOP_SPECS:
            print(f"noop: {spec}")
            r = cli_runner.invoke(["diff", "--exit-code", spec])
            assert r.exit_code == 0, r


@pytest.mark.parametrize(
    "head_sha,head1_sha",
    [
        pytest.param(H.POINTS.HEAD_SHA, H.POINTS.HEAD1_SHA, id="commit_hash"),
        pytest.param(H.POINTS.HEAD_TREE_SHA, H.POINTS.HEAD1_TREE_SHA, id="tree_hash"),
    ],
)
def test_diff_rev_rev(head_sha, head1_sha, data_archive_readonly, cli_runner):
    """diff between trees / commits - no-op"""

    F_SPECS = (
        f"{head1_sha}...{head_sha}",
        f"{head1_sha}...",
        "HEAD^1...HEAD",
        ["HEAD^1", "HEAD"],
    )

    R_SPECS = (
        f"{head_sha}...{head1_sha}",
        f"...{head1_sha}",
        "HEAD...HEAD^1",
        ["HEAD", "HEAD^1"],
    )

    CHANGE_IDS = {
        (1182, 1182),
        (1181, 1181),
        (1168, 1168),
        (1166, 1166),
        (1095, 1095),
    }

    with data_archive_readonly("points"):
        for spec in F_SPECS:
            print(f"fwd: {spec}")
            if isinstance(spec, str):
                spec = [spec]
            r = cli_runner.invoke(["diff", "--exit-code", "-o", "json", *spec])
            assert r.exit_code == 1, r
            odata = json.loads(r.stdout)["kart.diff/v1+hexwkb"]
            assert len(odata[H.POINTS.LAYER]["feature"]) == 5

            change_ids = {
                (
                    f.get("-", {}).get(H.POINTS.LAYER_PK),
                    f.get("+", {}).get(H.POINTS.LAYER_PK),
                )
                for f in odata[H.POINTS.LAYER]["feature"]
            }
            assert change_ids == CHANGE_IDS
            # this commit _adds_ names
            change_names = {
                (f["-"]["name"], f["+"]["name"])
                for f in odata[H.POINTS.LAYER]["feature"]
            }
            assert not any(n[0] for n in change_names)
            assert all(n[1] for n in change_names)

        for spec in R_SPECS:
            print(f"rev: {spec}")
            if isinstance(spec, str):
                spec = [spec]
            r = cli_runner.invoke(["diff", "--exit-code", "-o", "json", *spec])
            assert r.exit_code == 1, r
            odata = json.loads(r.stdout)["kart.diff/v1+hexwkb"]
            assert len(odata[H.POINTS.LAYER]["feature"]) == 5
            change_ids = {
                (
                    f.get("-", {}).get(H.POINTS.LAYER_PK),
                    f.get("+", {}).get(H.POINTS.LAYER_PK),
                )
                for f in odata[H.POINTS.LAYER]["feature"]
            }
            assert change_ids == CHANGE_IDS
            # so names are _removed_
            change_names = {
                (f["-"]["name"], f["+"]["name"])
                for f in odata[H.POINTS.LAYER]["feature"]
            }
            assert all(n[0] for n in change_names)
            assert not any(n[1] for n in change_names)


def test_diff_rev_wc(data_working_copy, cli_runner):
    """diff the working copy against commits"""
    # ID  R0  ->  R1  ->  WC
    # 1   a       a1      a
    # 2   b       b1      b1
    # 3   c       c       c1
    # 4   d       d1      d2
    # 5   e       e1      e*
    # 6   f       f*      f+
    # 7   g       g*      -
    # 8   -       h+      h1
    # 9   -       i+      i*
    # 10  -       j+      j
    # 11  -       -       k+
    # 12  l       l*      l1+

    # Legend:
    #     x     existing
    #     xN    edit
    #     x*    delete
    #     x+    insert
    #     -     not there

    # Columns: id,value

    R0 = "e1b8c966d4d35451cf26cecf2575b0cbbd880e75"
    R1 = "349a25079b0743206d662aab4c1f759ecd3fc25e"  # HEAD

    with data_working_copy("editing") as (repo_path, wc):
        # empty HEAD -> no working copy changes
        # r = cli_runner.invoke(["diff", "--exit-code", f"HEAD"])
        # assert r.exit_code == 0, r

        # make the R1 -> WC changes
        repo = KartRepo(repo_path)
        with repo.working_copy.tabular.session() as sess:
            EDITS = ((1, "a"), (3, "c1"), (4, "d2"), (8, "h1"))
            for pk, value in EDITS:
                r = sess.execute(
                    "UPDATE editing SET value = :value WHERE id = :id;",
                    {"value": value, "id": pk},
                )
                assert r.rowcount == 1

            r = sess.execute("DELETE FROM editing WHERE id IN (5, 9);")
            assert r.rowcount == 2

            r = sess.execute(
                "INSERT INTO editing (id, value) VALUES (6, 'f'), (11, 'k'), (12, 'l1');"
            )
            assert r.rowcount == 3

        def _extract(diff_json):
            ds = {}
            for f in odata["editing"]["feature"]:
                old = f.get("-")
                new = f.get("+")
                pk = old["id"] if old else new["id"]
                v_old = old["value"] if old else None
                v_new = new["value"] if new else None
                ds[pk] = (v_old, v_new)
            return ds

        # changes from HEAD (R1 -> WC)
        r = cli_runner.invoke(["diff", "--exit-code", "-o", "json", R1])
        assert r.exit_code == 1, r
        odata = json.loads(r.stdout)["kart.diff/v1+hexwkb"]
        ddata = _extract(odata)
        assert ddata == {
            1: ("a1", "a"),
            3: ("c", "c1"),
            4: ("d1", "d2"),
            5: ("e1", None),
            6: (None, "f"),
            8: ("h", "h1"),
            9: ("i", None),
            11: (None, "k"),
            12: (None, "l1"),
        }

        # changes from HEAD^1 (R0 -> WC)
        r = cli_runner.invoke(["diff", "--exit-code", "-o", "json", R0])
        assert r.exit_code == 1, r
        odata = json.loads(r.stdout)["kart.diff/v1+hexwkb"]
        ddata = _extract(odata)
        assert ddata == {
            2: ("b", "b1"),
            3: ("c", "c1"),
            4: ("d", "d2"),
            5: ("e", None),
            7: ("g", None),
            8: (None, "h1"),
            10: (None, "j"),
            11: (None, "k"),
            12: ("l", "l1"),
        }


def test_diff_rev_wc_with_deleted_dataset(data_working_copy, cli_runner):
    """
    Diff the working copy against a commit, where a dataset has been deleted since the commit
    """

    with data_working_copy("points") as (repo_path, wc):
        r = cli_runner.invoke(
            ["data", "rm", "nz_pa_points_topo_150k", "--message=destroy"]
        )
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["diff", "HEAD^"])
        assert r.exit_code == 0, r.stderr
        assert "--- nz_pa_points_topo_150k:meta:schema.json" in r.stdout


@pytest.mark.parametrize("output_format", ["text", "json"])
def test_3d_diff_wc(data_archive, cli_runner, tmp_path, output_format):
    with data_archive("gpkg-3d-points") as src:
        src_gpkg_path = src / "points-3d.gpkg"
        repo_path = tmp_path / "repo"
        r = cli_runner.invoke(["init", "--import", src_gpkg_path, repo_path])
        assert r.exit_code == 0, r.stderr
        repo = KartRepo(repo_path)
        with repo.working_copy.tabular.session() as sess:
            r = sess.execute(
                """UPDATE "points-3d" SET geometry = ST_GeomFromText('POINT Z(1 2 3)', 4326) WHERE id = 1"""
            )
            assert r.rowcount == 1
        r = cli_runner.invoke(["-C", repo_path, "diff", "-o", output_format])
        assert r.exit_code == 0, r.stderr
        if output_format == "text":
            assert r.stdout.splitlines() == [
                "--- points-3d:feature:1",
                "+++ points-3d:feature:1",
                "-                                 geometry = POINT Z(...)",
                "+                                 geometry = POINT Z(...)",
            ]
        elif output_format == "json":
            assert json.loads(r.stdout) == {
                "kart.diff/v1+hexwkb": {
                    "points-3d": {
                        "feature": [
                            {
                                "-": {
                                    "id": 1,
                                    "geometry": "01E90300002EB6B430EC7F46C03E08336CE8C844400000000000000000",
                                },
                                "+": {
                                    "id": 1,
                                    "geometry": "01E9030000000000000000F03F00000000000000400000000000000840",
                                },
                            }
                        ]
                    }
                }
            }


def test_diff_object_add_empty():
    null_diff = DeltaDiff()
    assert len(null_diff) == 0

    assert null_diff + null_diff is not null_diff
    assert null_diff + null_diff == null_diff

    diff = DeltaDiff(
        [
            Delta.insert((20, {"pk": 20})),
            Delta.update((10, {"pk": 10}), (11, {"pk": 11})),
            Delta.delete((30, {"pk": 30})),
        ],
    )
    assert diff + null_diff == diff
    assert null_diff + diff == diff


# ID  R0  ->  R1  ->  R2
# 1   a       a1      a
# 2   b       b1      b1
# 3   c       c       c1
# 4   d       d1      d2
# 5   e       e1      e*
# 6   f       f*      f+
# 7   g       g*      -
# 8   -       h+      h1
# 9   -       i+      i*
# 10  -       j+      j
# 11  -       -       k+
# 12  l       l*      l1+
DIFF_R1 = DeltaDiff(
    [
        Delta.update((1, {"pk": 1, "v": "a"}), (1, {"pk": 1, "v": "a1"})),
        Delta.update((2, {"pk": 2, "v": "b"}), (2, {"pk": 2, "v": "b1"})),
        # 3 no-op: c -> c
        Delta.update((4, {"pk": 4, "v": "d"}), (4, {"pk": 4, "v": "d1"})),
        Delta.update((5, {"pk": 5, "v": "e"}), (5, {"pk": 5, "v": "e1"})),
        Delta.delete((6, {"pk": 6, "v": "f"})),
        Delta.delete((7, {"pk": 7, "v": "g"})),
        Delta.insert((8, {"pk": 8, "v": "h"})),
        Delta.insert((9, {"pk": 9, "v": "i"})),
        Delta.insert((10, {"pk": 10, "v": "j"})),
        # 11 no-op: None -> None
        Delta.delete((12, {"pk": 12, "v": "l"})),
    ],
)

DIFF_R2 = DeltaDiff(
    [
        Delta.update((1, {"pk": 1, "v": "a1"}), (1, {"pk": 1, "v": "a"})),
        # 2 no-op: b1 -> b1
        Delta.update((3, {"pk": 3, "v": "c"}), (3, {"pk": 3, "v": "c1"})),
        Delta.update((4, {"pk": 4, "v": "d1"}), (4, {"pk": 4, "v": "d2"})),
        Delta.delete((5, {"pk": 5, "v": "e1"})),
        Delta.insert((6, {"pk": 6, "v": "f"})),
        # 7 no-op: None -> None
        Delta.update((8, {"pk": 8, "v": "h"}), (8, {"pk": 8, "v": "h1"})),
        Delta.delete((9, {"pk": 9, "v": "i"})),
        # 10 no-op: j -> j
        Delta.insert((11, {"pk": 11, "v": "k"})),
        Delta.insert((12, {"pk": 12, "v": "l1"})),
    ],
)

DIFF_R0_R2 = DeltaDiff(
    [
        # 1 no-op: a -> a
        Delta.update((2, {"pk": 2, "v": "b"}), (2, {"pk": 2, "v": "b1"})),
        Delta.update((3, {"pk": 3, "v": "c"}), (3, {"pk": 3, "v": "c1"})),
        Delta.update((4, {"pk": 4, "v": "d"}), (4, {"pk": 4, "v": "d2"})),
        Delta.delete((5, {"pk": 5, "v": "e"})),
        # 6 no-op: f -> f
        Delta.delete((7, {"pk": 7, "v": "g"})),
        Delta.insert((8, {"pk": 8, "v": "h1"})),
        # 9 no-op: None -> None
        Delta.insert((10, {"pk": 10, "v": "j"})),
        Delta.insert((11, {"pk": 11, "v": "k"})),
        Delta.update((12, {"pk": 12, "v": "l"}), (12, {"pk": 12, "v": "l1"})),
    ],
)


def test_diff_object_add():
    assert DIFF_R1 != DIFF_R0_R2
    assert DIFF_R2 != DIFF_R0_R2
    assert DIFF_R1 + DIFF_R2 == DIFF_R0_R2

    diff = DIFF_R1.copy()
    assert diff != DIFF_R0_R2
    diff += DIFF_R2
    assert diff == DIFF_R0_R2

    assert ~DIFF_R0_R2 == ~DIFF_R2 + ~DIFF_R1


def test_diff_object_add_reverse():
    """
    Check that ~(A + B) == (~B + ~A)
    """
    assert ~DIFF_R0_R2 == ~DIFF_R2 + ~DIFF_R1


def _count(generator):
    return len(list(generator))


def test_diff_object_eq_reverse():
    forward = DIFF_R0_R2
    reverse = ~forward
    forward_counts = forward.type_counts()
    reverse_counts = reverse.type_counts()
    assert forward_counts["inserts"] == reverse_counts["deletes"]
    assert forward_counts["deletes"] == reverse_counts["inserts"]
    assert forward_counts["updates"] == reverse_counts["updates"]
    assert list(forward.values()) == [Delta(d.new, d.old) for d in reverse.values()]


def test_diff_3way(data_working_copy, cli_runner, insert, request):
    with data_working_copy("points") as (repo_path, wc):
        repo = KartRepo(repo_path)

        # new branch
        r = cli_runner.invoke(["checkout", "-b", "changes"])
        assert r.exit_code == 0, r.stderr
        assert repo.head.name == "refs/heads/changes"

        # make some changes
        with repo.working_copy.tabular.session() as sess:
            insert(sess)
            insert(sess)
            b_commit_id = insert(sess)
        assert repo.head.target.hex == b_commit_id

        r = cli_runner.invoke(["checkout", "main"])
        assert r.exit_code == 0, r.stderr
        assert repo.head.target.hex != b_commit_id

        with repo.working_copy.tabular.session() as sess:
            m_commit_id = insert(sess)

        H.git_graph(request, "pre-merge-main")

        # Three dots diff should show both sets of changes.
        r = cli_runner.invoke(["diff", "-o", "json", f"{m_commit_id}...{b_commit_id}"])
        assert r.exit_code == 0, r.stderr
        features = json.loads(r.stdout)["kart.diff/v1+hexwkb"][
            "nz_pa_points_topo_150k"
        ]["feature"]
        assert len(features) == 4

        r = cli_runner.invoke(["diff", "-o", "json", f"{b_commit_id}...{m_commit_id}"])
        assert r.exit_code == 0, r.stderr
        features = json.loads(r.stdout)["kart.diff/v1+hexwkb"][
            "nz_pa_points_topo_150k"
        ]["feature"]
        assert len(features) == 4

        # Two dots diff should show only one set of changes - the changes on the target branch.
        r = cli_runner.invoke(["diff", "-o", "json", f"{m_commit_id}..{b_commit_id}"])
        assert r.exit_code == 0, r.stderr
        features = json.loads(r.stdout)["kart.diff/v1+hexwkb"][
            "nz_pa_points_topo_150k"
        ]["feature"]
        assert len(features) == 3

        r = cli_runner.invoke(["diff", "-o", "json", f"{b_commit_id}..{m_commit_id}"])
        assert r.exit_code == 0, r.stderr
        features = json.loads(r.stdout)["kart.diff/v1+hexwkb"][
            "nz_pa_points_topo_150k"
        ]["feature"]
        assert len(features) == 1


@pytest.mark.parametrize("output_format", SHOW_OUTPUT_FORMATS)
def test_show_points_HEAD(output_format, data_archive_readonly, cli_runner):
    """
    Show a patch; ref defaults to HEAD
    """
    with data_archive_readonly("points"):
        r = cli_runner.invoke(
            ["show", f"--output-format={output_format}", "--output=-", "HEAD"]
        )
        if output_format == "quiet":
            assert r.exit_code == 1, r
            assert r.stdout == ""
        else:
            assert r.exit_code == 0, r.stderr

        if output_format == "text":
            assert r.stdout.splitlines() == [
                f"commit {H.POINTS.HEAD_SHA}",
                "Author: Robert Coup <robert@coup.net.nz>",
                f"Date:   {H.POINTS.DATE_TIME}",
                "",
                "    Improve naming on Coromandel East coast",
                "",
                "--- nz_pa_points_topo_150k:feature:1095",
                "+++ nz_pa_points_topo_150k:feature:1095",
                "-                               name_ascii = ␀",
                "+                               name_ascii = Harataunga (Rakairoa)",
                "",
                "-                               macronated = N",
                "+                               macronated = Y",
                "-                                     name = ␀",
                "+                                     name = Harataunga (Rākairoa)",
                "",
                "--- nz_pa_points_topo_150k:feature:1166",
                "+++ nz_pa_points_topo_150k:feature:1166",
                "-                               name_ascii = ␀",
                "+                               name_ascii = Oturu",
                "-                                     name = ␀",
                "+                                     name = Oturu",
                "--- nz_pa_points_topo_150k:feature:1168",
                "+++ nz_pa_points_topo_150k:feature:1168",
                "-                               name_ascii = ␀",
                "+                               name_ascii = Tairua",
                "-                                     name = ␀",
                "+                                     name = Tairua",
                "--- nz_pa_points_topo_150k:feature:1181",
                "+++ nz_pa_points_topo_150k:feature:1181",
                "-                               name_ascii = ␀",
                "+                               name_ascii = Ko Te Ra Matiti (Wharekaho)",
                "-                               macronated = N",
                "+                               macronated = Y",
                "-                                     name = ␀",
                "+                                     name = Ko Te Rā Matiti (Wharekaho)",
                "--- nz_pa_points_topo_150k:feature:1182",
                "+++ nz_pa_points_topo_150k:feature:1182",
                "-                               name_ascii = ␀",
                "+                               name_ascii = Ko Te Ra Matiti (Wharekaho)",
                "-                               macronated = N",
                "+                               macronated = Y",
                "-                                     name = ␀",
                "+                                     name = Ko Te Rā Matiti (Wharekaho)",
            ]

        elif output_format == "json":
            j = json.loads(r.stdout)
            # check the diff's present, but this test doesn't need to have hundreds of lines
            # to know exactly what it is (we have diff tests above)
            assert "kart.diff/v1+hexwkb" in j
            assert j["kart.show/v1"] == {
                "authorEmail": "robert@coup.net.nz",
                "authorName": "Robert Coup",
                "authorTime": "2019-06-20T14:28:33Z",
                "authorTimeOffset": "+01:00",
                "committerEmail": "robert@coup.net.nz",
                "committerName": "Robert Coup",
                "commitTime": "2019-06-20T14:28:33Z",
                "commitTimeOffset": "+01:00",
                "commit": H.POINTS.HEAD_SHA,
                "abbrevCommit": H.POINTS.HEAD_SHA[:7],
                "message": "Improve naming on Coromandel East coast",
                "parents": [H.POINTS.HEAD1_SHA],
                "abbrevParents": [H.POINTS.HEAD1_SHA[:7]],
            }


@pytest.mark.parametrize(
    "diff_command",
    [
        ["diff", "HEAD^", "HEAD", "nz_pa_points_topo_150k:1182"],
        ["diff", "HEAD^", "HEAD", "--", "nz_pa_points_topo_150k:1182"],
        ["diff", "HEAD^...", "nz_pa_points_topo_150k:1182"],
        ["diff", "HEAD^...", "--", "nz_pa_points_topo_150k:1182"],
        ["show", "nz_pa_points_topo_150k:1182"],
        ["show", "--", "nz_pa_points_topo_150k:1182"],
        ["show", "HEAD", "nz_pa_points_topo_150k:1182"],
        ["show", "HEAD", "--", "nz_pa_points_topo_150k:1182"],
    ],
)
def test_diff_filtered_text(
    diff_command, data_archive_readonly, cli_runner, monkeypatch
):
    def _get_raw_diff_for_subtree(self, *args, **kwargs):
        # When only nz_pa_points_topo_150k:1182 is requested, a different more efficient code-path should be used.
        pytest.fail(
            "This method should not be called when only a single feature is required"
        )

    monkeypatch.setattr(TableV3, "get_raw_diff_for_subtree", _get_raw_diff_for_subtree)

    with data_archive_readonly("points"):
        r = cli_runner.invoke(diff_command)
        assert r.exit_code == 0, r.stderr

        is_show = diff_command[0] == "show"
        diff_lines = r.stdout.splitlines()[6:] if is_show else r.stdout.splitlines()

        assert diff_lines == [
            "--- nz_pa_points_topo_150k:feature:1182",
            "+++ nz_pa_points_topo_150k:feature:1182",
            "-                               name_ascii = ␀",
            "+                               name_ascii = Ko Te Ra Matiti (Wharekaho)",
            "-                               macronated = N",
            "+                               macronated = Y",
            "-                                     name = ␀",
            "+                                     name = Ko Te Rā Matiti (Wharekaho)",
        ]


def test_diff_wildcard_dataset_filters(data_archive, cli_runner):
    with data_archive("polygons") as repo_path:
        # Add another dataset at "second/dataset"
        with data_archive("gpkg-polygons") as src:
            src_gpkg_path = src / "nz-waca-adjustments.gpkg"
            r = cli_runner.invoke(
                [
                    "-C",
                    repo_path,
                    "import",
                    src_gpkg_path,
                    "nz_waca_adjustments:second/dataset",
                ]
            )
            assert r.exit_code == 0, r.stderr

        # Find all meta changes for datasets matching a filter
        r = cli_runner.invoke(["diff", "HEAD^^?...", "second/*:meta", "-o", "json"])
        assert r.exit_code == 0, r.stderr
        diff = json.loads(r.stdout)["kart.diff/v1+hexwkb"]
        assert diff.keys() == {"second/dataset"}
        assert diff["second/dataset"].keys() == {"meta"}
        assert diff["second/dataset"]["meta"].keys() == {
            "title",
            "crs/EPSG:4167.wkt",
            "schema.json",
            "description",
        }

        # Find title changes for all datasets
        r = cli_runner.invoke(["diff", "HEAD^^?...", "*:meta:title", "-o", "json"])
        assert r.exit_code == 0, r.stderr
        assert json.loads(r.stdout) == {
            "kart.diff/v1+hexwkb": {
                "nz_waca_adjustments": {
                    "meta": {"title": {"+": "NZ WACA Adjustments"}}
                },
                "second/dataset": {"meta": {"title": {"+": "NZ WACA Adjustments"}}},
            }
        }

        # Wildcard dataset filter for specific feature ID in all datasets.
        # This mostly exists for consistency with the meta ones shown above, but might be useful for ... something?
        r = cli_runner.invoke(["diff", "HEAD^^?...", "*:feature:4408145", "-o", "json"])
        assert r.exit_code == 0, r.stderr
        assert json.loads(r.stdout) == {
            "kart.diff/v1+hexwkb": {
                "nz_waca_adjustments": {
                    "feature": [
                        {
                            "+": {
                                "id": 4408145,
                                "geom": "0106000000010000000103000000010000000A000000D7D232C2528C6540224B1CB992CC45C035AC93FE308C6540AED61AE518CC45C077BF65A9308C65400CE8853B17CC45C0F188658E208C654079F5E0A49FCB45C03C3B141A248C654006E470019FCB45C0896DFC19278C6540671929E5A3CB45C0DF597160288C654000A7080BA6CB45C064B3C319648C65408C0F44B114CC45C0C885FE1E988C6540B64D609F81CC45C0D7D232C2528C6540224B1CB992CC45C0",
                                "date_adjusted": "2016-12-15T12:37:17",
                                "survey_reference": None,
                                "adjusted_nodes": 891,
                            }
                        }
                    ]
                },
                "second/dataset": {
                    "feature": [
                        {
                            "+": {
                                "id": 4408145,
                                "geom": "0106000000010000000103000000010000000A000000D7D232C2528C6540224B1CB992CC45C035AC93FE308C6540AED61AE518CC45C077BF65A9308C65400CE8853B17CC45C0F188658E208C654079F5E0A49FCB45C03C3B141A248C654006E470019FCB45C0896DFC19278C6540671929E5A3CB45C0DF597160288C654000A7080BA6CB45C064B3C319648C65408C0F44B114CC45C0C885FE1E988C6540B64D609F81CC45C0D7D232C2528C6540224B1CB992CC45C0",
                                "date_adjusted": "2016-12-15T12:37:17",
                                "survey_reference": None,
                                "adjusted_nodes": 891,
                            }
                        }
                    ]
                },
            }
        }

        # Filter for features in datasets whose name matches a pattern
        r = cli_runner.invoke(
            ["diff", "HEAD^^?...", "*/dataset:feature:4408145", "-o", "json"]
        )
        assert r.exit_code == 0, r.stderr
        assert json.loads(r.stdout) == {
            "kart.diff/v1+hexwkb": {
                "second/dataset": {
                    "feature": [
                        {
                            "+": {
                                "id": 4408145,
                                "geom": "0106000000010000000103000000010000000A000000D7D232C2528C6540224B1CB992CC45C035AC93FE308C6540AED61AE518CC45C077BF65A9308C65400CE8853B17CC45C0F188658E208C654079F5E0A49FCB45C03C3B141A248C654006E470019FCB45C0896DFC19278C6540671929E5A3CB45C0DF597160288C654000A7080BA6CB45C064B3C319648C65408C0F44B114CC45C0C885FE1E988C6540B64D609F81CC45C0D7D232C2528C6540224B1CB992CC45C0",
                                "date_adjusted": "2016-12-15T12:37:17",
                                "survey_reference": None,
                                "adjusted_nodes": 891,
                            }
                        }
                    ]
                }
            }
        }


@pytest.mark.parametrize("output_format", SHOW_OUTPUT_FORMATS)
def test_show_polygons_initial(output_format, data_archive_readonly, cli_runner):
    """Make sure we can show the initial commit"""
    with data_archive_readonly("polygons"):
        r = cli_runner.invoke(["log"])
        assert r.exit_code == 0, r.stderr
        initial_commit = re.findall("commit ([0-9a-f]+)\n", r.stdout)[-1]

        r = cli_runner.invoke(
            ["show", f"--output-format={output_format}", "--output=-", initial_commit]
        )
        if output_format == "quiet":
            assert r.exit_code == 1, r
            assert r.stdout == ""
        else:
            assert r.exit_code == 0, r.stderr

        if output_format == "text":
            lines = r.stdout.splitlines()
            assert lines[0:6] == [
                f"commit {H.POLYGONS.HEAD_SHA}",
                "Author: Robert Coup <robert@coup.net.nz>",
                f"Date:   {H.POLYGONS.DATE_TIME}",
                "",
                "    Import from nz-waca-adjustments.gpkg",
                "",
            ]

            assert "+++ nz_waca_adjustments:meta:title" in lines
            index = lines.index("+++ nz_waca_adjustments:meta:title")
            assert lines[index : index + 2] == [
                "+++ nz_waca_adjustments:meta:title",
                "+ NZ WACA Adjustments",
            ]

            assert "+++ nz_waca_adjustments:feature:1424927" in lines
            index = lines.index("+++ nz_waca_adjustments:feature:1424927")
            assert lines[index : index + 6] == [
                "+++ nz_waca_adjustments:feature:1424927",
                "+                                       id = 1424927",
                "+                                     geom = MULTIPOLYGON(...)",
                "+                            date_adjusted = 2011-03-25T07:30:45",
                "+                         survey_reference = ␀",
                "+                           adjusted_nodes = 1122",
            ]

        elif output_format == "json":
            j = json.loads(r.stdout)
            assert "kart.diff/v1+hexwkb" in j
            assert j["kart.show/v1"] == {
                "authorEmail": "robert@coup.net.nz",
                "authorName": "Robert Coup",
                "authorTime": "2019-07-22T11:05:39Z",
                "authorTimeOffset": "+01:00",
                "committerEmail": "robert@coup.net.nz",
                "committerName": "Robert Coup",
                "commitTime": "2019-07-22T11:05:39Z",
                "commitTimeOffset": "+01:00",
                "commit": H.POLYGONS.HEAD_SHA,
                "abbrevCommit": H.POLYGONS.HEAD_SHA[0:7],
                "message": "Import from nz-waca-adjustments.gpkg\n",
                "parents": [],
                "abbrevParents": [],
            }


def test_show_json_format(data_archive_readonly, cli_runner):
    with data_archive_readonly("points"):
        r = cli_runner.invoke(["show", f"-o", "json:compact", "HEAD"])

        assert r.exit_code == 0, r.stderr
        # output is compact, no indentation
        assert '"kart.diff/v1+hexwkb": {"' in r.stdout


def test_show_json_coloured(data_archive_readonly, cli_runner, monkeypatch):
    always_output_colour = lambda x: True
    monkeypatch.setattr(kart.output_util, "can_output_colour", always_output_colour)

    with data_archive_readonly("points"):
        r = cli_runner.invoke(["show", f"-o", "json:pretty", "HEAD"])
        assert r.exit_code == 0, r.stderr
        # No asserts about colour codes - that would be system specific. Just a basic check:
        assert '"kart.diff/v1+hexwkb"' in r.stdout


def test_create_patch(data_archive_readonly, cli_runner):
    """
    Show a patch; ref defaults to HEAD
    """
    with data_archive_readonly("points"):
        r = cli_runner.invoke(["create-patch"])
        assert r.exit_code == 2, r.stderr
        r = cli_runner.invoke(["create-patch", "HEAD"])
        assert r.exit_code == 0, r.stderr

        j = json.loads(r.stdout)
        # check the diff's present, but this test doesn't need to have hundreds of lines
        # to know exactly what it is (we have diff tests above)
        assert "kart.diff/v1+hexwkb" in j
        assert j["kart.patch/v1"] == {
            "authorEmail": "robert@coup.net.nz",
            "authorName": "Robert Coup",
            "authorTime": "2019-06-20T14:28:33Z",
            "authorTimeOffset": "+01:00",
            "message": "Improve naming on Coromandel East coast",
            "base": "6e2984a28150330a6c51019a70f9e8fcfe405e8c",
        }


def test_show_shallow_clone(data_archive_readonly, cli_runner, tmp_path, chdir):
    # just checking you can 'show' the first commit of a shallow clone
    with data_archive_readonly("points") as original_path:
        clone_path = tmp_path / "shallow-clone"
        r = cli_runner.invoke(["clone", "--depth=1", original_path, clone_path])
        assert r.exit_code == 0, r.stderr

        with chdir(clone_path):
            r = cli_runner.invoke(["show"])
            assert r.exit_code == 0, r


def test_diff_streaming(data_archive_readonly):
    # Test that a diff can be created without reading every feature,
    # and that the features in that diff can be read one by one.
    with data_archive_readonly("points") as repo_path:
        repo = KartRepo(repo_path)
        old = repo.datasets("HEAD^")[H.POINTS.LAYER]
        new = repo.datasets("HEAD")[H.POINTS.LAYER]

        def override_get_feature(self, *args, **kwargs):
            self.get_feature_calls += 1
            return self.__class__.get_feature(self, *args, **kwargs)

        old.get_feature_calls = 0
        new.get_feature_calls = 0

        old.get_feature = functools.partial(override_get_feature, old)
        new.get_feature = functools.partial(override_get_feature, new)

        feature_diff = old.diff(new)["feature"]

        expected_calls = 0
        assert old.get_feature_calls == expected_calls
        assert new.get_feature_calls == expected_calls

        for key, delta in sorted(feature_diff.items()):
            print(delta.old_value)
            print(delta.new_value)
            expected_calls += 1
            assert old.get_feature_calls == expected_calls
            assert new.get_feature_calls == expected_calls


@pytest.mark.parametrize(
    "output_format", [o for o in SHOW_OUTPUT_FORMATS if o not in {"html", "quiet"}]
)
def test_show_output_to_file(output_format, data_archive, cli_runner):
    with data_archive("points") as repo_path:
        r = cli_runner.invoke(
            ["show", f"--output-format={output_format}", "--output=out"]
        )
        assert r.exit_code == 0, r
        assert (repo_path / "out").exists()


def test_diff_geojson_usage(data_archive, cli_runner, tmp_path):
    with data_archive("points") as repo_path:
        # output to stdout
        r = cli_runner.invoke(
            ["diff", "--output-format=geojson", "--output=-", "HEAD^..."]
        )
        assert r.exit_code == 0, r.stderr
        # output to stdout (by default)
        r = cli_runner.invoke(["diff", "--output-format=geojson", "HEAD^..."])
        assert r.exit_code == 0, r.stderr

        # output to a directory that doesn't yet exist
        r = cli_runner.invoke(
            [
                "diff",
                "--output-format=geojson",
                f"--output={tmp_path / 'abc'}",
                "HEAD^...",
            ]
        )
        assert r.exit_code == 0, r.stderr
        assert {p.name for p in (tmp_path / "abc").iterdir()} == {
            "nz_pa_points_topo_150k.geojson"
        }

        # output to a directory that does exist
        d = tmp_path / "def"
        d.mkdir()
        # this gets left alone
        (d / "empty.file").write_bytes(b"")
        # this gets deleted.
        (d / "some.geojson").write_bytes(b"{}")
        r = cli_runner.invoke(
            [
                "diff",
                "--output-format=geojson",
                f"--output={d}",
                "HEAD^...",
            ]
        )
        assert r.exit_code == 0, r.stderr
        assert {p.name for p in d.iterdir()} == {
            "nz_pa_points_topo_150k.geojson",
            "empty.file",
        }

        # (add another dataset)
        with data_archive("gpkg-3d-points") as src:
            src_gpkg_path = src / "points-3d.gpkg"
            r = cli_runner.invoke(["-C", repo_path, "import", src_gpkg_path])
            assert r.exit_code == 0, r.stderr

        # stdout output is allowed even though there are multiple datasets - as long as only one has changed:
        r = cli_runner.invoke(
            [
                "diff",
                "--output-format=geojson",
                "HEAD^...",
            ]
        )
        assert r.exit_code == 0, r.stderr

        # stdout output is not allowed when there are changes to multiple datasets:
        r = cli_runner.invoke(
            [
                "diff",
                "--output-format=geojson",
                "HEAD^^...",
            ]
        )
        assert r.exit_code == 2, r.stderr
        assert (
            r.stderr.splitlines()[-1]
            == "Error: Invalid value for --output: Need to specify a directory via --output for GeoJSON with more than one dataset"
        )

        # Can't specify an (existing) regular file either
        myfile = tmp_path / "ghi"
        myfile.write_bytes(b"")
        assert myfile.exists()
        r = cli_runner.invoke(
            [
                "diff",
                "--output-format=geojson",
                f"--output={myfile}",
                "HEAD^^...",
            ]
        )
        assert r.exit_code == 2, r.stderr
        assert (
            r.stderr.splitlines()[-1]
            == "Error: Invalid value for --output: Output path should be a directory for GeoJSON format."
        )


@pytest.mark.parametrize(
    "output_format",
    [o for o in SHOW_OUTPUT_FORMATS if o not in {"html", "quiet"}],
)
def test_attached_files_diff(output_format, data_archive, cli_runner):
    with data_archive("points-with-attached-files") as repo_path:
        r = cli_runner.invoke(["show", f"--output-format={output_format}"])
        assert r.exit_code == 0, r.stderr
        if output_format == "text":
            assert r.stdout.splitlines()[-6:] == [
                "+++ LICENSE.txt",
                "+ (file 1674aa1)",
                "+++ logo.png",
                "+ (file f8555b6)",
                "+++ nz_pa_points_topo_150k/metadata.xml",
                "+ (file a39253e)",
            ]
        elif output_format == "json":
            jdict = json.loads(r.stdout)
            files = jdict["kart.diff/v1+hexwkb"]["<files>"]
            assert files == {
                "LICENSE.txt": {"+": "1674aa1"},
                "logo.png": {"+": "f8555b6"},
                "nz_pa_points_topo_150k/metadata.xml": {"+": "a39253e"},
            }

        elif output_format == "json-lines":
            lines = r.stdout.splitlines()
            jdict = json.loads(lines[-3])
            assert jdict == {
                "type": "file",
                "path": "LICENSE.txt",
                "change": {"+": "1674aa1"},
            }

            jdict = json.loads(lines[-2])
            assert jdict == {
                "type": "file",
                "path": "logo.png",
                "change": {"+": "f8555b6"},
            }

            jdict = json.loads(lines[-1])
            assert jdict == {
                "type": "file",
                "path": "nz_pa_points_topo_150k/metadata.xml",
                "change": {"+": "a39253e"},
            }


@pytest.mark.parametrize(
    "output_format",
    [o for o in SHOW_OUTPUT_FORMATS if o not in {"html", "quiet"}],
)
def test_full_attached_files_diff(output_format, data_archive, cli_runner):
    with data_archive("points-with-attached-files") as repo_path:
        r = cli_runner.invoke(
            [
                "show",
                f"--output-format={output_format}",
                "--diff-files",
                "LICENSE.txt",
                "logo.png",
            ]
        )
        assert r.exit_code == 0, r.stderr
        if output_format == "text":
            assert r.stdout.splitlines()[-8:] == [
                "+++ LICENSE.txt",
                "+ NZ Pa Points (Topo, 1:50k)",
                "+ https://data.linz.govt.nz/layer/50308-nz-pa-points-topo-150k/",
                "+ Land Information New Zealand",
                "+ CC-BY",
                "+ ",
                "+++ logo.png",
                "+ (binary file f8555b6)",
            ]
        elif output_format == "json":
            jdict = json.loads(r.stdout)
            files = jdict["kart.diff/v1+hexwkb"]["<files>"]
            logo = files["logo.png"]
            # Check just the first 4 bytes of the binary file data...
            logo["+"] = b64decode_str(logo["+"])[:4]

            assert files == {
                "LICENSE.txt": {
                    "+": "NZ Pa Points (Topo, 1:50k)\nhttps://data.linz.govt.nz/layer/50308-nz-pa-points-topo-150k/\nLand Information New Zealand\nCC-BY\n"
                },
                "logo.png": {"+": b"\x89PNG"},
            }

        elif output_format == "json-lines":
            lines = r.stdout.splitlines()
            jdict = json.loads(lines[-2])
            assert jdict == {
                "type": "file",
                "path": "LICENSE.txt",
                "binary": False,
                "change": {
                    "+": "NZ Pa Points (Topo, 1:50k)\nhttps://data.linz.govt.nz/layer/50308-nz-pa-points-topo-150k/\nLand Information New Zealand\nCC-BY\n"
                },
            }

            jdict = json.loads(lines[-1])
            # Check just the first 4 bytes of the binary file data...
            jdict["change"]["+"] = b64decode_str(jdict["change"]["+"])[:4]
            assert jdict == {
                "type": "file",
                "path": "logo.png",
                "binary": True,
                "change": {"+": b"\x89PNG"},
            }


def test_attached_files_patch(data_archive, cli_runner):
    with data_archive("points-with-attached-files") as repo_path:
        r = cli_runner.invoke(["create-patch", "HEAD"])
        assert r.exit_code == 0, r.stderr
        jdict = json.loads(r.stdout)
        files = jdict["kart.diff/v1+hexwkb"]["<files>"]
        # Check just the first 4 bytes of the binary file data...
        logo = files["logo.png"]
        logo["+"] = b64decode_str(logo["+"])[:4]
        # Check just the first 346 chars of the XML metadata.
        metadata_xml = files["nz_pa_points_topo_150k/metadata.xml"]
        metadata_xml["+"] = metadata_xml["+"][:346]

        assert files == {
            "LICENSE.txt": {
                "+": "text:NZ Pa Points (Topo, 1:50k)\nhttps://data.linz.govt.nz/layer/50308-nz-pa-points-topo-150k/\nLand Information New Zealand\nCC-BY\n"
            },
            "logo.png": {"+": b"\x89PNG"},
            "nz_pa_points_topo_150k/metadata.xml": {
                "+": 'text:<gmd:MD_Metadata xmlns:gco="http://www.isotc211.org/2005/gco" xmlns:gmd="http://www.isotc211.org/2005/gmd" xmlns:gml="http://www.opengis.net/gml" xmlns:gts="http://www.isotc211.org/2005/gts" xmlns:topo="http://www.linz.govt.nz/schemas/topo/data-dictionary" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns="http://www.isotc211.org/2005/gmd">'
            },
        }


@pytest.mark.parametrize("cmd", ["diff", "show"])
@pytest.mark.parametrize("commit", ["HEAD^", "HEAD"])
@pytest.mark.parametrize("delta_filter", ["--,-", "++,+"])
def test_delta_filter(delta_filter, commit, cmd, data_archive, cli_runner):
    with data_archive("points") as repo_path:
        if cmd == "diff":
            commit_spec = {"HEAD": "HEAD^...HEAD", "HEAD^": "HEAD^^?...HEAD^"}[commit]
        else:
            commit_spec = commit

        r = cli_runner.invoke(
            [cmd, "-ojson", f"--delta-filter={delta_filter}", commit_spec]
        )
        assert r.exit_code == 0, r.stderr
        jdict = json.loads(r.stdout)
        features = jdict["kart.diff/v1+hexwkb"]["nz_pa_points_topo_150k"]["feature"]
        assert len(features) >= 5
        if commit == "HEAD":
            # Second commit has only updates
            for feature in features:
                assert set(feature.keys()) == {"+", "-"}
        else:
            # Initial commit has only inserts
            for feature in features:
                assert set(feature.keys()) == {"++"}
        filter_parts = set(delta_filter.split(","))
        for feature in features:
            for key in feature:
                if key not in filter_parts:
                    assert feature[key] is None
                else:
                    assert feature[key] is not None


def test_load_user_provided_html_template(data_archive, cli_runner, monkeypatch):
    def noop(*args, **kwargs):
        pass

    monkeypatch.setattr(webbrowser, "open_new", noop)
    with data_archive("points") as repo_path:
        r = cli_runner.invoke(
            [
                "diff",
                "--output-format=html",
                "--html-template="
                + str(
                    Path(__file__).absolute().parent.parent / "kart" / "diff-view.html"
                ),
                "HEAD^...",
            ]
        )
        assert r.exit_code == 0, r.stderr


def test_xss_protection():
    TEMPLATE = """
<html>
  <head>
    <title>Kart Diff: ${title}</title>
    <script type="application/json">${geojson_data}</script>
  </head>
  <body>...</body>
</html>
""".lstrip()
    html_xss = "<script>alert(1);</script>"
    json_xss = {"key": "</script><script>alert(1);</script>"}
    result = HtmlDiffWriter.substitute_into_template(
        string.Template(TEMPLATE), html_xss, json_xss
    )

    EXPECTED_RESULT = """
<html>
  <head>
    <title>Kart Diff: &lt;script&gt;alert(1);&lt;/script&gt;</title>
    <script type="application/json">{"key": "\\x3c\\x2fscript\\x3e\\x3cscript\\x3ealert(1);\\x3c\\x2fscript\\x3e"}</script>
  </head>
  <body>...</body>
</html>
""".lstrip()

    assert result == EXPECTED_RESULT


def test_diff_format_no_data_changes_json(cli_runner, data_archive):
    # Check that the json output contains a boolean for data_changes (feature/tile changes)
    with data_archive("points.tgz"):
        r = cli_runner.invoke(
            ["diff", "--diff-format=no-data-changes", "-o", "json", "HEAD^...HEAD"]
        )
        output = json.loads(r.stdout)
        assert output["kart.diff/v1+hexwkb"] == {
            "nz_pa_points_topo_150k": {"data_changes": True}
        }


def test_diff_json_lines_with_no_data_changes(cli_runner, data_archive, monkeypatch):
    # Check that the json output contains a boolean for data_changes (feature/tile changes)
    with data_archive("points.tgz"):
        r = cli_runner.invoke(
            [
                "diff",
                "--diff-format=no-data-changes",
                "-o",
                "json-lines",
                "HEAD^...HEAD",
            ]
        )
        assert r.exit_code == 0, r.stderr
        output = r.stdout.splitlines()
        assert len(output) == 4
        jsons = [json.loads(line) for line in output]
        types = [j["type"] for j in jsons]
        # note no "feature" items here
        assert types == ["version", "datasetInfo", "metaInfo", "dataChanges"]
        assert jsons[-1] == {
            "type": "dataChanges",
            "dataset": "nz_pa_points_topo_150k",
            "value": True,
        }
