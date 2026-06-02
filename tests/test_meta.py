import json
import os
import pytest
import subprocess
import pathlib

import kart
from kart.repo import KartRepo

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
    def test_with_dataset_types(self, output_format, data_archive_readonly, cli_runner):
        with data_archive_readonly("points"):
            r = cli_runner.invoke(
                [
                    "meta",
                    "get",
                    "nz_pa_points_topo_150k",
                    "--with-dataset-types",
                    "-o",
                    output_format,
                ]
            )
            assert r.exit_code == 0, r
            if output_format == "text":
                assert "datasetType" in r.stdout
                assert "version" in r.stdout
                assert "title" in r.stdout
                assert EXPECTED_TITLE in r.stdout
                assert "description" in r.stdout
                assert "schema.json" in r.stdout
                assert "crs/EPSG:4326.wkt" in r.stdout
            else:
                output = json.loads(r.stdout)
                output = output["nz_pa_points_topo_150k"]
                assert output["datasetType"] == "table"
                assert output["version"] == 3
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


def test_meta_set_amend(data_archive, cli_runner):
    with data_archive("points"):
        r = cli_runner.invoke(
            [
                "meta",
                "set",
                "nz_pa_points_topo_150k",
                "title=newtitle",
                "description=newdescription",
                "--amend",
            ]
        )
        assert r.exit_code == 0, r.stderr
        r = cli_runner.invoke(["show", "-o", "json"])
        assert r.exit_code == 0, r.stderr
        output = json.loads(r.stdout)
        patch_info = output.pop("kart.show/v1")
        assert patch_info["message"] == "Improve naming on Coromandel East coast"
        feature = output["kart.diff/v1+hexwkb"]["nz_pa_points_topo_150k"]["feature"]
        assert len(feature) == 5
        meta = output["kart.diff/v1+hexwkb"]["nz_pa_points_topo_150k"]["meta"]
        assert meta["title"] == {"-": "NZ Pa Points (Topo, 1:50k)", "+": "newtitle"}
        assert meta["description"]["+"] == "newdescription"


def test_meta_set_custom_fields(data_archive, cli_runner):
    with data_archive("points"):
        # Make sure this works even when we have a working copy
        # (working copies have to handle certain meta-item changes).
        r = cli_runner.invoke(["checkout"])
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(
            [
                "meta",
                "set",
                "nz_pa_points_topo_150k",
                "custom_string=example",
                "custom_list.json=[1, 2, 3]",
            ]
        )
        assert r.exit_code == 0, r.stderr
        r = cli_runner.invoke(["show", "-o", "json"])
        assert r.exit_code == 0, r.stderr
        output = json.loads(r.stdout)
        patch_info = output.pop("kart.show/v1")
        assert patch_info["message"] == "Update metadata for nz_pa_points_topo_150k"
        meta = output["kart.diff/v1+hexwkb"]["nz_pa_points_topo_150k"]["meta"]
        assert meta["custom_string"] == {"+": "example"}
        assert meta["custom_list.json"]["+"] == [1, 2, 3]


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
    monkeypatch.setattr(kart.output_util, "can_output_colour", always_output_colour)

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
        r = subprocess.check_output(["git", "show", "--numstat"], encoding="utf-8")
        assert r.splitlines()[-4:] == [
            "    Updating attachments",
            "",
            "1\t0\tLICENSE",  # 1 line added, 0 deleted
            "1\t55\tnz_pa_points_topo_150k/metadata.xml",  # 1 line added, 55 deleted.
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


def test_commit_files_add_and_delete(data_working_copy, cli_runner, monkeypatch):
    with data_working_copy("point-cloud/auckland") as (repo_dir, wc):
        # Set the environment variable (monkeypatch so it's restored after the test)
        monkeypatch.setenv("X_KART_ATTACHMENTS", "true")
        # Define file path
        file_path = pathlib.Path(repo_dir) / "my_attachment.txt"

        # Make sure the file does not exist yet
        assert not file_path.exists()

        # Add the file
        r = cli_runner.invoke(
            [
                "commit-files",
                "-m",
                "Adding a new file",
                "my_attachment.txt=My text",
            ]
        )
        assert r.exit_code == 0, r.stderr

        # Check that the file exists now
        assert file_path.exists()

        # Check file contents
        expected_contents = "My text"
        actual_contents = file_path.read_text()
        assert actual_contents == expected_contents

        # Delete the file
        r = cli_runner.invoke(
            [
                "commit-files",
                "-m",
                "Deleting a file",
                "--remove-empty-files",
                "my_attachment.txt=",
            ]
        )
        assert r.exit_code == 0, r.stderr

        # Check that the file does not exist now
        assert not file_path.exists()


def test_attachment_checkout_roundtrip(data_working_copy, cli_runner, monkeypatch):
    # Checking out a commit writes its attachments to the workdir; checking out a commit without
    # them removes them again - even though no datasets changed between the two commits.
    monkeypatch.setenv("X_KART_ATTACHMENTS", "true")
    with data_working_copy("point-cloud/auckland") as (repo_dir, wc):
        file_path = pathlib.Path(repo_dir) / "my_attachment.txt"
        assert not file_path.exists()

        r = cli_runner.invoke(
            ["commit-files", "-m", "Add attachment", "my_attachment.txt=hello"]
        )
        assert r.exit_code == 0, r.stderr
        assert file_path.read_text() == "hello"

        # The previous commit has no attachment - checking it out removes the file.
        r = cli_runner.invoke(["checkout", "HEAD^"])
        assert r.exit_code == 0, r.stderr
        assert not file_path.exists()

        # Checking out the commit with the attachment again restores it.
        r = cli_runner.invoke(["checkout", "main"])
        assert r.exit_code == 0, r.stderr
        assert file_path.read_text() == "hello"


def test_attachment_create_workingcopy(data_working_copy, cli_runner, monkeypatch):
    # Creating a working copy from scratch checks out attachments (base tree is the empty tree).
    monkeypatch.setenv("X_KART_ATTACHMENTS", "true")
    with data_working_copy("point-cloud/auckland") as (repo_dir, wc):
        file_path = pathlib.Path(repo_dir) / "my_attachment.txt"
        r = cli_runner.invoke(
            ["commit-files", "-m", "Add attachment", "my_attachment.txt=hello"]
        )
        assert r.exit_code == 0, r.stderr
        assert file_path.read_text() == "hello"

        # Remove the file then recreate the working copy from scratch - it should be checked out again.
        # (--discard-changes since removing the file makes the working copy dirty.)
        file_path.unlink()
        r = cli_runner.invoke(
            ["create-workingcopy", "--delete-existing", "--discard-changes"]
        )
        assert r.exit_code == 0, r.stderr
        assert file_path.read_text() == "hello"


def test_attachment_delete_workingcopy(data_working_copy, cli_runner, monkeypatch):
    # Deleting the file-system working copy cleans up checked-out attachment files.
    monkeypatch.setenv("X_KART_ATTACHMENTS", "true")
    with data_working_copy("point-cloud/auckland") as (repo_dir, wc):
        file_path = pathlib.Path(repo_dir) / "my_attachment.txt"
        r = cli_runner.invoke(
            ["commit-files", "-m", "Add attachment", "my_attachment.txt=hello"]
        )
        assert r.exit_code == 0, r.stderr
        assert file_path.exists()

        repo = KartRepo(repo_dir)
        repo.working_copy.workdir.delete()
        assert not file_path.exists()


def test_attachment_workingcopy_diff_status_commit(
    data_working_copy, cli_runner, monkeypatch
):
    # Editing/adding/deleting attachments in the workdir shows up in diff and status, and can be committed.
    monkeypatch.setenv("X_KART_ATTACHMENTS", "true")
    with data_working_copy("point-cloud/auckland") as (repo_dir, wc):
        repo_dir = pathlib.Path(repo_dir)

        # Start with a committed attachment.
        r = cli_runner.invoke(
            ["commit-files", "-m", "Add attachment", "my_attachment.txt=hello"]
        )
        assert r.exit_code == 0, r.stderr

        # Clean to start with.
        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r.stderr
        assert "Nothing to commit, working copy clean" in r.stdout

        # Edit the attachment, add a new one, in the filesystem.
        (repo_dir / "my_attachment.txt").write_text("goodbye")
        (repo_dir / "new_attachment.txt").write_text("brand new")

        # status reports the file changes.
        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r.stderr
        assert "<files>" in r.stdout

        # diff reports the file changes.
        r = cli_runner.invoke(["diff"])
        assert r.exit_code == 0, r.stderr
        assert "my_attachment.txt" in r.stdout
        assert "new_attachment.txt" in r.stdout

        # diff --diff-files shows the actual (uncommitted) contents.
        r = cli_runner.invoke(["diff", "--diff-files"])
        assert r.exit_code == 0, r.stderr
        assert "goodbye" in r.stdout
        assert "brand new" in r.stdout

        # Commit the changes.
        r = cli_runner.invoke(["commit", "-m", "Edit attachments"])
        assert r.exit_code == 0, r.stderr

        # Working copy is clean again, and the changes are committed.
        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r.stderr
        assert "Nothing to commit, working copy clean" in r.stdout

        assert (
            subprocess.check_output(
                ["git", "show", "HEAD:my_attachment.txt"],
                cwd=repo_dir,
                encoding="utf-8",
            )
            == "goodbye"
        )
        assert (
            subprocess.check_output(
                ["git", "show", "HEAD:new_attachment.txt"],
                cwd=repo_dir,
                encoding="utf-8",
            )
            == "brand new"
        )


def test_attachment_workingcopy_delete_and_commit(
    data_working_copy, cli_runner, monkeypatch
):
    # Deleting a checked-out attachment from the filesystem is detected and can be committed.
    monkeypatch.setenv("X_KART_ATTACHMENTS", "true")
    with data_working_copy("point-cloud/auckland") as (repo_dir, wc):
        repo_dir = pathlib.Path(repo_dir)

        r = cli_runner.invoke(
            ["commit-files", "-m", "Add attachment", "my_attachment.txt=hello"]
        )
        assert r.exit_code == 0, r.stderr

        # Delete the attachment from the filesystem.
        (repo_dir / "my_attachment.txt").unlink()

        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r.stderr
        assert "<files>" in r.stdout
        assert "1 delete" in r.stdout

        r = cli_runner.invoke(["commit", "-m", "Delete attachment"])
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r.stderr
        assert "Nothing to commit, working copy clean" in r.stdout

        # The file is no longer in the tree.
        r = subprocess.run(
            ["git", "show", "HEAD:my_attachment.txt"],
            cwd=repo_dir,
            capture_output=True,
        )
        assert r.returncode != 0


def test_attachment_checkout_aborts_when_dirty(
    data_working_copy, cli_runner, monkeypatch
):
    # A checkout that would lose uncommitted attachment changes must abort (unless --discard-changes).
    monkeypatch.setenv("X_KART_ATTACHMENTS", "true")
    with data_working_copy("point-cloud/auckland") as (repo_dir, wc):
        repo_dir = pathlib.Path(repo_dir)
        file_path = repo_dir / "my_attachment.txt"

        r = cli_runner.invoke(
            ["commit-files", "-m", "Add attachment", "my_attachment.txt=hello"]
        )
        assert r.exit_code == 0, r.stderr

        # Edit the attachment in the workdir (now dirty).
        file_path.write_text("dirty edit")

        # Switching to the previous commit without --discard-changes must abort, leaving the edit intact.
        r = cli_runner.invoke(["checkout", "HEAD^"])
        assert r.exit_code != 0
        assert "uncommitted changes" in r.stderr.lower()
        assert file_path.read_text() == "dirty edit"

        # With --discard-changes the checkout proceeds, and the attachment (absent at HEAD^) is removed.
        r = cli_runner.invoke(["checkout", "HEAD^", "--discard-changes"])
        assert r.exit_code == 0, r.stderr
        assert not file_path.exists()


def test_attachment_discard_changes_restores_committed_state(
    data_working_copy, cli_runner, monkeypatch
):
    # `reset --discard-changes` on the same commit restores attachments to their committed state.
    monkeypatch.setenv("X_KART_ATTACHMENTS", "true")
    with data_working_copy("point-cloud/auckland") as (repo_dir, wc):
        repo_dir = pathlib.Path(repo_dir)
        committed = repo_dir / "my_attachment.txt"

        r = cli_runner.invoke(
            ["commit-files", "-m", "Add attachment", "my_attachment.txt=hello"]
        )
        assert r.exit_code == 0, r.stderr

        # Make a mess in the workdir: edit a committed attachment, delete it... and add an untracked one.
        committed.write_text("dirty edit")
        untracked = repo_dir / "untracked.txt"
        untracked.write_text("should be discarded")

        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r.stderr
        assert "<files>" in r.stdout

        # Discard all changes (same commit).
        r = cli_runner.invoke(["reset", "--discard-changes"])
        assert r.exit_code == 0, r.stderr

        # The committed attachment is restored, the untracked one is removed, and the WC is clean.
        assert committed.read_text() == "hello"
        assert not untracked.exists()
        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r.stderr
        assert "Nothing to commit, working copy clean" in r.stdout


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
        r = cli_runner.invoke(["log", "--output-format=text:%s"])
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

        r = cli_runner.invoke(["log", "--output-format=text:%t"])
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

        r = cli_runner.invoke(["log", "--output-format=text:%s"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            "A more informative commit message",
            "Import from nz-pa-points-topo-150k.gpkg",
        ]
        myfile = subprocess.check_output(
            ["git", "show", "HEAD:myfile.txt"], encoding="utf-8"
        )
        assert myfile == "myfile"

        r = cli_runner.invoke(["log", "--output-format=text:%t"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == actual_tree_contents

        # --amend without a message just uses the same message as previous commit
        r = cli_runner.invoke(["commit-files", "--amend", "x=y"])
        r = cli_runner.invoke(["log", "--output-format=text:%s"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            "A more informative commit message",
            "Import from nz-pa-points-topo-150k.gpkg",
        ]
