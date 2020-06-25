import json
import pytest


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
                assert 'fields/name_ascii\n    3\nfields/t50_fid\n    2' in r.stdout
            else:
                output = json.loads(r.stdout)
                assert output['fields/name_ascii'] == 3

    @pytest.mark.parametrize("output_format", ("text", "json"))
    def test_all_exclude_readonly(
        self, output_format, data_archive_readonly, cli_runner
    ):
        with data_archive_readonly("points"):
            r = cli_runner.invoke(
                [
                    "meta",
                    "get",
                    "nz_pa_points_topo_150k",
                    "-o",
                    output_format,
                    "--exclude-readonly",
                ]
            )
            assert r.exit_code == 0, r
            if output_format == 'text':
                assert 'gpkg_contents\n' in r.stdout
                assert 'fields/name_ascii' not in r.stdout
            else:
                output = json.loads(r.stdout)
                assert 'gpkg_contents' in output
                assert 'fields/name_ascii' not in output

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
                    "gpkg_contents",
                    "fields/name_ascii",
                ]
            )
            assert r.exit_code == 0, r
            if output_format == 'text':
                assert 'fields/name_ascii\n    3' in r.stdout
                assert 'fields/t50_fid' not in r.stdout
            else:
                output = json.loads(r.stdout)
                assert output['fields/name_ascii'] == 3
                assert 'fields/t50_fid' not in output
