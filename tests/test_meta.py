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
        with data_archive_readonly("points"):
            r = cli_runner.invoke(
                ["meta", "get", "nz_pa_points_topo_150k", "-o", output_format]
            )
            assert r.exit_code == 0, r
            if output_format == 'text':
                assert EXPECTED_GCG_TEXT in r.stdout
                assert "gpkg_contents" in r.stdout
                assert "sqlite_table_info" in r.stdout
            else:
                output = json.loads(r.stdout)
                assert output["gpkg_geometry_columns"] == EXPECTED_GCG_JSON
                assert output["gpkg_contents"]
                assert output["sqlite_table_info"]

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
