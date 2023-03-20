import json


def test_show_diff_format_for_feature_changes_diff_format_none_json(
    cli_runner, data_archive
):
    # Check that the json output is a commit metadata only
    with data_archive("points.tgz"):
        r = cli_runner.invoke(["show", "--diff-format=none", "-o", "json"])
        output = json.loads(r.stdout)

        # Assert that the output is a dict with a key for metadata only
        assert output.keys() == {"kart.show/v1"}


def test_show_diff_format_for_feature_changes_diff_format_no_data_changes_json(
    cli_runner, data_archive
):
    # Check that the json output is a commit metadata (if any) and a boolean for data_changes (feature/tile changes)
    with data_archive("points.tgz"):
        r = cli_runner.invoke(["show", "--diff-format=no-data-changes", "-o", "json"])
        output = json.loads(r.stdout)
        assert output["kart.diff/v1+hexwkb"] == {
            "nz_pa_points_topo_150k": {"data_changes": True}
        }


def test_show_diff_format_for_feature_changes_diff_format_none_text(
    cli_runner, data_archive
):
    # Check that the text output is a commit metadata only
    with data_archive("points.tgz"):
        r = cli_runner.invoke(["show", "--diff-format=none", "-o", "text"])
        output = r.stdout.splitlines()

        result = [
            "commit 1582725544d9122251acd4b3fc75b5c88ac3fd17",
            "Author: Robert Coup <robert@coup.net.nz>",
            "Date:   Thu Jun 20 15:28:33 2019 +0100",
            "",
            "    Improve naming on Coromandel East coast",
            "",
        ]
        assert output == result


def test_show_diff_format_for_feature_changes_diff_format_no_data_changes_text(
    cli_runner, data_archive
):
    # Check that the text output is a commit metadata (if any) and a boolean for data_changes (feature/tile changes)
    with data_archive("points.tgz"):
        r = cli_runner.invoke(["show", "--diff-format=no-data-changes", "-o", "text"])
        output = r.stdout.splitlines()

        result = [
            "commit 1582725544d9122251acd4b3fc75b5c88ac3fd17",
            "Author: Robert Coup <robert@coup.net.nz>",
            "Date:   Thu Jun 20 15:28:33 2019 +0100",
            "",
            "    Improve naming on Coromandel East coast",
            "",
        ]
        assert output == result


def test_show_diff_format_for_meta_changes_diff_format_no_data_changes_json(
    cli_runner, data_archive
):
    # Check that for meta-only commit, the data_changes boolean is present and set to False. For json ouput
    with data_archive("meta-updates.tgz"):
        r = cli_runner.invoke(["show", "--diff-format=no-data-changes", "-o", "json"])
        output = json.loads(r.stdout)

        assert "meta" in output["kart.diff/v1+hexwkb"]["nz_pa_points_topo_150k"]
        assert (
            output["kart.diff/v1+hexwkb"]["nz_pa_points_topo_150k"]["data_changes"]
            == False
        )


def test_show_diff_format_for_meta_changes_diff_format_no_data_changes_text(
    cli_runner, data_archive
):
    # Check that for meta-only commit, the data_changes boolean is present and set to False. For text ouput
    with data_archive("meta-updates.tgz"):
        r = cli_runner.invoke(["show", "--diff-format=no-data-changes", "-o", "text"])
        output = r.stdout.splitlines()
        result = "--- nz_pa_points_topo_150k:meta:schema.json"
        assert result in output
