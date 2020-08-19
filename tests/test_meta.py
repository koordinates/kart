import json
import pytest

EXPECTED_GCG_JSON = {
    "column_name": "geom",
    "geometry_type_name": "POINT",
    "m": 0,
    "srs_id": 4326,
    "table_name": "nz_pa_points_topo_150k",
    "z": 0,
}

EXPECTED_GCG_TEXT = """
gpkg_geometry_columns
    {
      "column_name": "geom",
      "geometry_type_name": "POINT",
      "m": 0,
      "srs_id": 4326,
      "table_name": "nz_pa_points_topo_150k",
      "z": 0
    }
""".strip()

EXPECTED_TITLE = """NZ Pa Points (Topo, 1:50k)"""


class TestMetaGet:
    def test_errors(self, data_archive_readonly, cli_runner):
        with data_archive_readonly("points"):
            r = cli_runner.invoke(["meta", "get", "nonexistent_dataset"])
            assert r.exit_code == 2, r
            assert 'No such dataset: nonexistent_dataset' in r.stderr

            r = cli_runner.invoke(
                ["meta", "get", "nz_pa_points_topo_150k", "nonexistent_meta"]
            )
            assert r.exit_code == 2, r
            assert "Couldn't find items: nonexistent_meta" in r.stderr

    @pytest.mark.parametrize("output_format", ("text", "json"))
    def test_all(self, output_format, data_archive_readonly, cli_runner):
        # All datasets now support getting metadata in either V1 or V2 format,
        # but if you don't specify a particular item, they will show all V2 items -
        # these are more self-explanatory to an end-user.
        with data_archive_readonly("points"):
            r = cli_runner.invoke(
                ["meta", "get", "nz_pa_points_topo_150k", "-o", output_format]
            )
            assert r.exit_code == 0, r
            if output_format == 'text':
                assert "title" in r.stdout
                assert EXPECTED_TITLE in r.stdout
                assert "description" in r.stdout
                assert "schema.json" in r.stdout
                assert "crs/EPSG:4326.wkt" in r.stdout
            else:
                output = json.loads(r.stdout)
                assert output["title"] == EXPECTED_TITLE
                assert output["description"]
                assert output["schema.json"]
                assert output["crs/EPSG:4326.wkt"]

    @pytest.mark.parametrize("output_format", ("text", "json"))
    def test_keys(self, output_format, data_archive_readonly, cli_runner):
        with data_archive_readonly("points"):
            r = cli_runner.invoke(
                [
                    "meta",
                    "get",
                    "nz_pa_points_topo_150k",
                    "-o",
                    output_format,
                    "gpkg_geometry_columns",
                ]
            )
            assert r.exit_code == 0, r
            if output_format == 'text':
                assert EXPECTED_GCG_TEXT in r.stdout
                assert "gpkg_contents" not in r.stdout
                assert "sqlite_table_info" not in r.stdout
            else:
                output = json.loads(r.stdout)
                assert output["gpkg_geometry_columns"] == EXPECTED_GCG_JSON
                assert "gpkg_contents" not in output
                assert "sqlite_table_info" not in output


def test_meta_set(data_archive, cli_runner):
    with data_archive("points2"):
        r = cli_runner.invoke(
            [
                "meta",
                "set",
                "nz_pa_points_topo_150k",
                "title=newtitle",
                "description=newdescription",
            ]
        )
        assert r.exit_code == 0, r.stderr
        r = cli_runner.invoke(["show", "-o", "json"])
        assert r.exit_code == 0, r.stderr
        output = json.loads(r.stdout)
        patch_info = output.pop('sno.patch/v1')
        assert patch_info['message'] == 'Update metadata for nz_pa_points_topo_150k'
        meta = output['sno.diff/v1+hexwkb']['nz_pa_points_topo_150k']['meta']
        assert meta['title'] == {'-': 'NZ Pa Points (Topo, 1:50k)', '+': 'newtitle'}
        assert meta['description']['+'] == 'newdescription'
