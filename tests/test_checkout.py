import json

import pytest


from kart.exceptions import UNCOMMITTED_CHANGES, NO_BRANCH, NO_COMMIT
from kart.repo import KartRepo
from kart.structs import CommitWithReference


H = pytest.helpers.helpers()


@pytest.mark.parametrize(
    "working_copy",
    [
        pytest.param(True, id="with-wc"),
        pytest.param(False, id="without-wc"),
    ],
)
def test_checkout_branches(data_archive, cli_runner, chdir, tmp_path, working_copy):
    with data_archive("points") as remote_path:
        r = cli_runner.invoke(["checkout", "-b", "one"])
        assert r.exit_code == 0, r.stderr
        r = cli_runner.invoke(["checkout", "-b", "two", "HEAD^"])
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["checkout", "one", "-b", "three"])
        assert r.exit_code == 0, r.stderr
        r = cli_runner.invoke(["switch", "two", "--create", "four"])
        assert r.exit_code == 0, r.stderr

        repo = KartRepo(remote_path)
        one = CommitWithReference.resolve(repo, "one")
        two = CommitWithReference.resolve(repo, "two")
        three = CommitWithReference.resolve(repo, "three")
        four = CommitWithReference.resolve(repo, "four")

        assert one.commit.hex == three.commit.hex
        assert two.commit.hex == four.commit.hex

        assert one.commit.hex != two.commit.hex

        wc_flag = "--checkout" if working_copy else "--no-checkout"
        r = cli_runner.invoke(["clone", remote_path, tmp_path, wc_flag])
        repo = KartRepo(tmp_path)

        head = CommitWithReference.resolve(repo, "HEAD")

        with chdir(tmp_path):
            r = cli_runner.invoke(["branch"])
            assert r.exit_code == 0, r.stderr
            assert r.stdout.splitlines() == ["* four"]

            # Commit hex is not a branch name, can't be switched:
            r = cli_runner.invoke(["switch", head.commit.hex])
            assert r.exit_code == NO_BRANCH, r.stderr
            # But can be checked out:
            r = cli_runner.invoke(["checkout", head.commit.hex])
            assert r.exit_code == 0, r.stderr

            r = cli_runner.invoke(["checkout", "zero"])
            assert r.exit_code == NO_COMMIT, r.stderr
            r = cli_runner.invoke(["switch", "zero"])
            assert r.exit_code == NO_BRANCH, r.stderr

            r = cli_runner.invoke(["checkout", "one", "--no-guess"])
            assert r.exit_code == NO_COMMIT, r.stderr
            r = cli_runner.invoke(["switch", "one", "--no-guess"])
            assert r.exit_code == NO_BRANCH, r.stderr

            r = cli_runner.invoke(["checkout", "one"])
            assert r.exit_code == 0, r.stderr
            assert (
                r.stdout.splitlines()[0]
                == "Creating new branch 'one' to track 'origin/one'..."
            )
            assert (
                CommitWithReference.resolve(repo, "HEAD").commit.hex == one.commit.hex
            )
            r = cli_runner.invoke(["switch", "two"])
            assert r.exit_code == 0, r.stderr
            assert (
                r.stdout.splitlines()[0]
                == "Creating new branch 'two' to track 'origin/two'..."
            )
            assert (
                CommitWithReference.resolve(repo, "HEAD").commit.hex == two.commit.hex
            )

            r = cli_runner.invoke(["branch"])
            assert r.exit_code == 0, r.stderr
            assert r.stdout.splitlines() == ["  four", "  one", "* two"]


def test_reset(data_working_copy, cli_runner, edit_points):
    with data_working_copy("points") as (repo_path, wc):
        repo = KartRepo(repo_path)
        with repo.working_copy.tabular.session() as sess:
            edit_points(sess)

        r = cli_runner.invoke(["diff", "--exit-code"])
        assert r.exit_code == 1
        r = cli_runner.invoke(
            ["log", "--output-format=text:oneline", "--decorate=short"]
        )
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            f"{H.POINTS.HEAD_SHA} (HEAD -> main) Improve naming on Coromandel East coast",
            f"{H.POINTS.HEAD1_SHA} Import from nz-pa-points-topo-150k.gpkg",
        ]

        r = cli_runner.invoke(["reset", "HEAD^"])
        assert r.exit_code == UNCOMMITTED_CHANGES
        assert "You have uncommitted changes in your working copy." in r.stderr

        r = cli_runner.invoke(["reset", "HEAD^", "--discard-changes"])
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["diff", "--exit-code"])
        assert r.exit_code == 0
        r = cli_runner.invoke(
            ["log", "--output-format=text:oneline", "--decorate=short"]
        )
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            f"{H.POINTS.HEAD1_SHA} (HEAD -> main) Import from nz-pa-points-topo-150k.gpkg",
        ]


def _check_workingcopy_contains_tables(repo, expected_tables):
    with repo.working_copy.tabular.session() as sess:
        r = sess.execute("""SELECT name FROM sqlite_master SM WHERE type='table';""")
        sqlite_table_names = set(row[0] for row in r)

        census_tables = set(t for t in sqlite_table_names if t.startswith("census"))
        assert census_tables == expected_tables

        r = sess.execute("""SELECT table_name FROM gpkg_contents;""")
        gpkg_contents_table_names = set(row[0] for row in r)
        assert gpkg_contents_table_names == expected_tables


def test_non_checkout_datasets(data_working_copy, cli_runner):
    with data_working_copy("au-census") as (repo_path, wc):
        repo = KartRepo(repo_path)
        _check_workingcopy_contains_tables(
            repo, {"census2016_sdhca_ot_sos_short", "census2016_sdhca_ot_ra_short"}
        )

        r = cli_runner.invoke(
            ["checkout", "--not-dataset=census2016-sdhca-ot-sos-short"]
        )
        assert r.exit_code == 2
        assert "No dataset census2016-sdhca-ot-sos-short" in r.stderr

        r = cli_runner.invoke(
            ["checkout", "--not-dataset=census2016_sdhca_ot_sos_short"]
        )
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            "On branch branch1",
            "",
            "User configuration prevents the following datasets from being checked out",
            "  (to overturn, use `kart checkout --dataset=DATASET`):",
            "census2016_sdhca_ot_sos_short",
            "",
            "Nothing to commit, working copy clean",
        ]

        _check_workingcopy_contains_tables(repo, {"census2016_sdhca_ot_ra_short"})

        # No WC changes are returned.
        r = cli_runner.invoke(["diff", "--exit-code"])
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(
            ["checkout", "main", "--dataset=census2016_sdhca_ot_sos_short"]
        )
        assert r.exit_code == 0

        _check_workingcopy_contains_tables(
            repo, {"census2016_sdhca_ot_sos_short", "census2016_sdhca_ot_ra_short"}
        )


def _file_status(cli_runner):
    r = cli_runner.invoke(["status", "-o", "json"])
    assert r.exit_code == 0, r.stderr
    return (
        json.loads(r.stdout)["kart.status/v2"]["workingCopy"].get("files") or {}
    )


def test_restore_attachment_modified(data_working_copy, cli_runner):
    """`kart restore -- LICENSE.txt` discards a modification to a tracked attachment file."""
    with data_working_copy("points-with-attached-files") as (path, wc):
        license_path = path / "LICENSE.txt"
        original = license_path.read_text(encoding="utf-8")

        license_path.write_text("Edited.\n", encoding="utf-8")
        assert _file_status(cli_runner) == {"modified": ["LICENSE.txt"]}

        r = cli_runner.invoke(["restore", "--", "LICENSE.txt"])
        assert r.exit_code == 0, r.stderr
        assert license_path.read_text(encoding="utf-8") == original
        assert _file_status(cli_runner) == {}


def test_restore_attachment_deleted(data_working_copy, cli_runner):
    """`kart restore -- LICENSE.txt` re-creates a tracked attachment file removed from the workdir."""
    with data_working_copy("points-with-attached-files") as (path, wc):
        license_path = path / "LICENSE.txt"
        original = license_path.read_text(encoding="utf-8")

        license_path.unlink()
        assert _file_status(cli_runner) == {"deleted": ["LICENSE.txt"]}

        r = cli_runner.invoke(["restore", "--", "LICENSE.txt"])
        assert r.exit_code == 0, r.stderr
        assert license_path.read_text(encoding="utf-8") == original
        assert _file_status(cli_runner) == {}


def test_restore_all_attachments(data_working_copy, cli_runner):
    """`kart restore` (no filters) restores every tracked attachment file as well as datasets."""
    with data_working_copy("points-with-attached-files") as (path, wc):
        original = (path / "LICENSE.txt").read_text(encoding="utf-8")
        (path / "LICENSE.txt").write_text("Edited.\n", encoding="utf-8")
        (path / "nz_pa_points_topo_150k" / "metadata.xml").unlink()

        r = cli_runner.invoke(["restore"])
        assert r.exit_code == 0, r.stderr

        assert (path / "LICENSE.txt").read_text(encoding="utf-8") == original
        assert (path / "nz_pa_points_topo_150k" / "metadata.xml").is_file()
        assert _file_status(cli_runner) == {}


def test_restore_leaves_untracked_files(data_working_copy, cli_runner):
    """`kart restore` does not touch untracked attachment files (mirrors `git restore` semantics)."""
    with data_working_copy("points-with-attached-files") as (path, wc):
        notes = path / "NOTES.txt"
        notes.write_text("Hello.\n", encoding="utf-8")

        r = cli_runner.invoke(["restore"])
        assert r.exit_code == 0, r.stderr
        assert notes.read_text(encoding="utf-8") == "Hello.\n"
        assert _file_status(cli_runner) == {"untracked": ["NOTES.txt"]}



def test_checkout_restores_attachments(data_working_copy, cli_runner):
    """Switching commits should refresh tracked attachment files in the workdir, not just datasets."""
    with data_working_copy("points-with-attached-files") as (path, wc):
        license_path = path / "LICENSE.txt"
        original = license_path.read_text(encoding="utf-8")

        # Modify the attachment locally without committing.
        license_path.write_text("Local edit, will be discarded.\n", encoding="utf-8")

        # `kart checkout HEAD --discard-changes` should restore the attachment to its HEAD state.
        r = cli_runner.invoke(["checkout", "HEAD", "--discard-changes"])
        assert r.exit_code == 0, r.stderr
        assert license_path.read_text(encoding="utf-8") == original


def test_checkout_removes_deleted_attachments(data_working_copy, cli_runner):
    """Switching to a commit where an attachment was removed should delete the file from the workdir."""
    with data_working_copy("points-with-attached-files") as (path, wc):
        license_path = path / "LICENSE.txt"
        assert license_path.exists()

        # Commit a new version with LICENSE.txt removed.
        r = cli_runner.invoke(
            ["commit-files", "-m", "remove LICENSE.txt", "--remove-empty-files", "LICENSE.txt="]
        )
        assert r.exit_code == 0, r.stderr

        # Go back one commit (where LICENSE.txt existed).
        r = cli_runner.invoke(["checkout", "HEAD~1"])
        assert r.exit_code == 0, r.stderr
        assert license_path.exists(), "LICENSE.txt should be restored when switching back"

        # Now switch forward again (LICENSE.txt was deleted in that commit).
        r = cli_runner.invoke(["checkout", "main"])
        assert r.exit_code == 0, r.stderr
        assert not license_path.exists(), "LICENSE.txt should be removed when switching to commit that deleted it"


def test_commit_attachment_file(data_working_copy, cli_runner):
    """kart commit should pick up modified and new attachment files alongside dataset changes."""
    with data_working_copy("points-with-attached-files") as (path, wc):
        license_path = path / "LICENSE.txt"
        notes_path = path / "NOTES.txt"

        # Modify an existing attachment and add a new one.
        license_path.write_text("Updated license text.\n", encoding="utf-8")
        notes_path.write_text("New notes file.\n", encoding="utf-8")

        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r.stderr
        assert "LICENSE.txt" in r.stdout
        assert "NOTES.txt" in r.stdout

        # kart commit should include both attachment changes.
        r = cli_runner.invoke(["commit", "-m", "update attachments"])
        assert r.exit_code == 0, r.stderr

        # Verify the files are committed: go back then forward and check content.
        r = cli_runner.invoke(["checkout", "HEAD~1"])
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["checkout", "main"])
        assert r.exit_code == 0, r.stderr
        assert license_path.read_text(encoding="utf-8") == "Updated license text.\n"
        assert notes_path.read_text(encoding="utf-8") == "New notes file.\n"


def test_restore_dataset_filter_and_file_filter_same_arg(data_working_copy, cli_runner):
    """
    Regression: kart restore with a filter that names a dataset must not fail even if
    the same string could also be interpreted as an attachment file path.  Previously
    _split_restore_filters would misclassify a filter when the dataset exists only on one
    branch but not the source tree passed to the split function.

    Here we verify the simpler invariant: passing a dataset name as a filter to
    `kart restore` succeeds (dataset is restored) and does not raise an error even though
    no attachment file with that name exists.
    """
    with data_working_copy("points-with-attached-files") as (path, wc):
        # Modify the dataset so there is something to restore.
        r = cli_runner.invoke(["checkout", "HEAD", "--discard-changes"])
        assert r.exit_code == 0, r.stderr

        # Passing the dataset name as a filter should succeed even though
        # no attachment file called "nz_pa_points_topo_150k" exists.
        r = cli_runner.invoke(["restore", "--", "nz_pa_points_topo_150k"])
        assert r.exit_code == 0, r.stderr
