import json
import pytest
import subprocess

import sno

EXPECTED_TITLE = """NZ Pa Points (Topo, 1:50k)"""


class TestMetaGet:
    def test_errors(self, data_archive_readonly, cli_runner):
        with data_archive_readonly("points"):
            r = cli_runner.invoke(["meta", "get", "nonexistent_dataset"])
            assert r.exit_code == 2, r
            assert "No such dataset: nonexistent_dataset" in r.stderr

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
            if output_format == "text":
                assert "title" in r.stdout
                assert EXPECTED_TITLE in r.stdout
                assert "description" in r.stdout
                assert "schema.json" in r.stdout
                assert "crs/EPSG:4326.wkt" in r.stdout
            else:
                output = json.loads(r.stdout)
                output = output["nz_pa_points_topo_150k"]
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
                    "title",
                ]
            )
            assert r.exit_code == 0, r
            if output_format == "text":
                assert "nz_pa_points_topo_150k" in r.stdout
                assert "title" in r.stdout
                assert "description" not in r.stdout
                assert "schema.json" not in r.stdout
            else:
                output = json.loads(r.stdout)
                output = output["nz_pa_points_topo_150k"]
                assert output["title"] == EXPECTED_TITLE
                assert "description" not in output
                assert "schema.json" not in output


def test_meta_set(data_archive, cli_runner):
    with data_archive("points"):
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
        patch_info = output.pop("kart.show/v1")
        assert patch_info["message"] == "Update metadata for nz_pa_points_topo_150k"
        meta = output["kart.diff/v1+hexwkb"]["nz_pa_points_topo_150k"]["meta"]
        assert meta["title"] == {"-": "NZ Pa Points (Topo, 1:50k)", "+": "newtitle"}
        assert meta["description"]["+"] == "newdescription"


def test_meta_get_ref(data_archive, cli_runner):
    with data_archive("points"):
        r = cli_runner.invoke(
            [
                "meta",
                "set",
                "nz_pa_points_topo_150k",
                "title=newtitle",
            ]
        )
        assert r.exit_code == 0, r.stderr
        r = cli_runner.invoke(
            [
                "meta",
                "get",
                "--ref=HEAD^",
                "nz_pa_points_topo_150k",
                "title",
                "-o",
                "json",
            ]
        )
        assert r.exit_code == 0, r.stderr
        assert json.loads(r.stdout) == {
            "nz_pa_points_topo_150k": {"title": "NZ Pa Points (Topo, 1:50k)"}
        }


def test_meta_get_coloured(data_archive, cli_runner, monkeypatch):
    always_output_colour = lambda x: True
    monkeypatch.setattr(sno.output_util, "can_output_colour", always_output_colour)

    with data_archive("points"):
        r = cli_runner.invoke(
            [
                "meta",
                "get",
                "--ref=HEAD^",
                "nz_pa_points_topo_150k",
                "-o",
                "json",
            ]
        )
        assert r.exit_code == 0, r.stderr
        # No asserts about colour codes - that would be system specific. Just a basic check:
        assert "nz_pa_points_topo_150k" in r.stdout


def test_commit_files(data_archive, cli_runner):
    with data_archive("points"):
        r = cli_runner.invoke(
            [
                "commit-files",
                "-m",
                "Updating attachments",
                "LICENSE=Do not even look at this data",
                "nz_pa_points_topo_150k/metadata.xml=<xml></xml>",
            ]
        )
        assert r.exit_code == 0, r.stderr
        r = subprocess.check_output(["git", "show"], encoding="utf-8")
        diff = r.splitlines()

        assert diff[9:13] == [
            "--- /dev/null",
            "+++ b/LICENSE",
            "@@ -0,0 +1 @@",
            "+Do not even look at this data",
        ]

        assert diff[17:21] == [
            "--- /dev/null",
            "+++ b/nz_pa_points_topo_150k/metadata.xml",
            "@@ -0,0 +1 @@",
            "+<xml></xml>",
        ]

        # committing a noop change is rejected (unless amending)
        r = cli_runner.invoke(
            [
                "commit-files",
                "-m",
                "Updating attachments 2",
                "LICENSE=Do not even look at this data",
            ]
        )
        assert r.exit_code == 44, r.stderr


def test_commit_files_remove_empty(data_archive, cli_runner):
    with data_archive("points"):
        r = cli_runner.invoke(
            [
                "commit-files",
                "-m",
                "adding some files",
                "x=x",
                "y=",
            ]
        )
        assert r.exit_code == 0, r.stderr
        x = subprocess.check_output(["git", "show", "HEAD:x"], encoding="utf-8")
        assert x == "x"
        y = subprocess.check_output(["git", "show", "HEAD:y"], encoding="utf-8")
        assert y == ""

        r = cli_runner.invoke(
            [
                "commit-files",
                "-m",
                "adding some files",
                "--remove-empty-files",
                "x=x",
                "y=",
            ]
        )
        assert r.exit_code == 0, r.stderr
        x = subprocess.check_output(["git", "show", "HEAD:x"], encoding="utf-8")
        assert x == "x"
        with pytest.raises(subprocess.CalledProcessError):
            subprocess.check_output(["git", "show", "HEAD:y"], encoding="utf-8")


def test_commit_files_amend(data_archive, cli_runner):
    with data_archive("points"):
        r = cli_runner.invoke(["log", "--pretty=%s"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            "Improve naming on Coromandel East coast",
            "Import from nz-pa-points-topo-150k.gpkg",
        ]

        # --amend the previous commit
        r = cli_runner.invoke(
            [
                "commit-files",
                "-m",
                "new commit message",
                "--amend",
                "myfile.txt=myfile",
            ]
        )
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["log", "--pretty=%t"])
        assert r.exit_code == 0, r.stderr
        actual_tree_contents = r.stdout.splitlines()

        # it's okay to amend with an empty change
        r = cli_runner.invoke(
            [
                "commit-files",
                "-m",
                "A more informative commit message",
                "--amend",
                "myfile.txt=myfile",
            ]
        )

        r = cli_runner.invoke(["log", "--pretty=%s"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            "A more informative commit message",
            "Import from nz-pa-points-topo-150k.gpkg",
        ]
        myfile = subprocess.check_output(
            ["git", "show", "HEAD:myfile.txt"], encoding="utf-8"
        )
        assert myfile == "myfile"

        r = cli_runner.invoke(["log", "--pretty=%t"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == actual_tree_contents

        # --amend without a message just uses the same message as previous commit
        r = cli_runner.invoke(["commit-files", "--amend", "x=y"])
        r = cli_runner.invoke(["log", "--pretty=%s"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            "A more informative commit message",
            "Import from nz-pa-points-topo-150k.gpkg",
        ]
