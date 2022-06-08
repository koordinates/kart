from kart.cli_util import OutputFormatType

from kart.completion_shared import conflict_completer, ref_completer, path_completer

DIFF_OUTPUT_FORMATS = ["text", "geojson", "json", "json-lines", "quiet", "html"]
SHOW_OUTPUT_FORMATS = DIFF_OUTPUT_FORMATS


def test_ref_completer(data_archive, cli_runner):
    with data_archive("points") as _:
        r = cli_runner.invoke(["checkout", "-b", "one"])
        assert r.exit_code == 0, r.stderr
        r = cli_runner.invoke(["checkout", "-b", "two"])
        assert r.exit_code == 0, r.stderr

        assert ref_completer() == [
            "one",
            "two",
            "main",
        ]
        assert ref_completer(incomplete="r") == [
            "refs/heads/one",
            "refs/heads/two",
            "refs/heads/main",
        ]


def test_path_completer(data_archive, cli_runner):
    with data_archive("points") as _:
        assert path_completer() == [
            "nz_pa_points_topo_150k",
        ]
        assert path_completer(incomplete="nz") == [
            "nz_pa_points_topo_150k",
        ]


def test_conflict_completer(data_archive, cli_runner):
    with data_archive("conflicts/points.tgz") as _:
        r = cli_runner.invoke(["merge", "theirs_branch"])
        assert r.exit_code == 0, r.stderr
        assert conflict_completer() == [
            "nz_pa_points_topo_150k",
        ]
        assert conflict_completer(incomplete="nz_pa_points_topo_150k") == [
            "nz_pa_points_topo_150k:feature:3",
            "nz_pa_points_topo_150k:feature:4",
            "nz_pa_points_topo_150k:feature:5",
            "nz_pa_points_topo_150k:feature:98001",
        ]
        assert conflict_completer(incomplete="nz_pa_points_topo_150k:feature:9") == [
            "nz_pa_points_topo_150k:feature:98001",
        ]


def test_show_output_format_completer(data_archive_readonly):
    with data_archive_readonly("polygons"):
        output_type = OutputFormatType(
            output_types=SHOW_OUTPUT_FORMATS, allow_text_formatstring=False
        )
        assert [
            type.value for type in output_type.shell_complete()
        ] == SHOW_OUTPUT_FORMATS
        assert [type.value for type in output_type.shell_complete(incomplete="j")] == [
            "json",
            "json-lines",
        ]
