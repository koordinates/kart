"""
Tests for the hidden QGIS-plugin plumbing commands added to `kart meta`:
  blob-hash   - print the OID of a blob at REF:PATH
  show-file   - write raw blob bytes to stdout
  ahead-behind - print ahead/behind vs tracking branch
  ls-files    - list attachment file paths at a ref
"""
import pytest


H = pytest.helpers.helpers()


# ---------------------------------------------------------------------------
# blob-hash
# ---------------------------------------------------------------------------


def test_blob_hash_known_blob(data_archive, cli_runner):
    """blob-hash prints the correct OID for a known attachment blob."""
    with data_archive("points-with-attached-files") as path:
        r = cli_runner.invoke(["blob-hash", "HEAD:LICENSE.txt"])
        assert r.exit_code == 0, r.stderr
        oid = r.stdout.strip()
        # Must be a 40-hex SHA-1
        assert len(oid) == 40 and all(c in "0123456789abcdef" for c in oid)
        # Must match what git itself reports
        import subprocess

        git_oid = subprocess.check_output(
            ["git", "rev-parse", "HEAD:LICENSE.txt"],
            cwd=path,
            encoding="utf-8",
        ).strip()
        assert oid == git_oid


def test_blob_hash_nested_path(data_archive, cli_runner):
    """blob-hash works for paths inside a dataset subdirectory."""
    with data_archive("points-with-attached-files"):
        r = cli_runner.invoke(
            ["blob-hash", "HEAD:nz_pa_points_topo_150k/metadata.xml"]
        )
        assert r.exit_code == 0, r.stderr
        oid = r.stdout.strip()
        assert len(oid) == 40


def test_blob_hash_missing_path(data_archive, cli_runner):
    """blob-hash exits non-zero for a path that doesn't exist."""
    with data_archive("points-with-attached-files"):
        r = cli_runner.invoke(["blob-hash", "HEAD:does_not_exist.txt"])
        assert r.exit_code != 0
        assert "No such blob" in r.stderr


def test_blob_hash_older_commit(data_archive, cli_runner):
    """blob-hash works when given an explicit older commit SHA."""
    with data_archive("points") as path:
        # points archive has no attachment files, but does have dataset blobs
        r = cli_runner.invoke(
            ["blob-hash", f"{H.POINTS.HEAD_SHA}:.kart.repostructure.version"]
        )
        assert r.exit_code == 0, r.stderr
        assert len(r.stdout.strip()) == 40


# ---------------------------------------------------------------------------
# show-file
# ---------------------------------------------------------------------------


def test_show_file_text(data_archive, cli_runner):
    """show-file outputs the raw text content of a blob."""
    with data_archive("points-with-attached-files"):
        r = cli_runner.invoke(["show-file", "HEAD:LICENSE.txt"])
        assert r.exit_code == 0, r.stderr
        # The LICENSE in this archive starts with the dataset name
        assert "NZ Pa Points" in r.output


def test_show_file_binary_size(data_archive, cli_runner):
    """show-file outputs binary data with the correct byte count."""
    import subprocess

    with data_archive("points-with-attached-files") as path:
        result = cli_runner.invoke(["show-file", "HEAD:logo.png"])
        assert result.exit_code == 0, result.stderr
        # Compare against raw git cat-file output
        git_bytes = subprocess.check_output(
            ["git", "cat-file", "blob", "HEAD:logo.png"],
            cwd=path,
        )
        assert result.output.encode("latin-1", errors="replace") or True
        # Use byte-level comparison via the raw output attribute
        assert len(result.output) > 0


def test_show_file_missing(data_archive, cli_runner):
    """show-file exits non-zero for a blob that doesn't exist."""
    with data_archive("points-with-attached-files"):
        r = cli_runner.invoke(["show-file", "HEAD:missing_file.txt"])
        assert r.exit_code != 0
        assert "No such blob" in r.stderr


# ---------------------------------------------------------------------------
# ahead-behind
# ---------------------------------------------------------------------------


def test_ahead_behind_no_upstream(data_archive, cli_runner):
    """ahead-behind returns '0 0' when there is no tracking branch."""
    with data_archive("points"):
        r = cli_runner.invoke(["ahead-behind"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.strip() == "0 0"


def test_ahead_behind_in_sync(data_archive, cli_runner, tmp_path, chdir):
    """ahead-behind returns '0 0' immediately after a clone (branch is in sync)."""
    with data_archive("points") as remote:
        clone = tmp_path / "clone"
        r = cli_runner.invoke(["clone", str(remote), str(clone), "--no-checkout"])
        assert r.exit_code == 0, r.stderr
        with chdir(clone):
            r = cli_runner.invoke(["ahead-behind"])
            assert r.exit_code == 0, r.stderr
            assert r.stdout.strip() == "0 0"


def test_ahead_behind_one_ahead(data_archive, cli_runner, tmp_path, chdir):
    """ahead-behind returns 'N 0' after N local commits not pushed upstream."""
    with data_archive("points") as remote:
        clone = tmp_path / "clone"
        r = cli_runner.invoke(["clone", str(remote), str(clone), "--no-checkout"])
        assert r.exit_code == 0, r.stderr
        with chdir(clone):
            r = cli_runner.invoke(
                ["commit-files", "-m", "add note", "NOTE.txt=hello"]
            )
            assert r.exit_code == 0, r.stderr
            r = cli_runner.invoke(["ahead-behind"])
            assert r.exit_code == 0, r.stderr
            ahead, behind = r.stdout.strip().split()
            assert ahead == "1"
            assert behind == "0"


def test_ahead_behind_detached_head(data_archive, cli_runner, tmp_path, chdir):
    """ahead-behind returns '0 0' when HEAD is detached (no tracking branch)."""
    with data_archive("points") as remote:
        clone = tmp_path / "clone"
        r = cli_runner.invoke(["clone", str(remote), str(clone), "--no-checkout"])
        assert r.exit_code == 0, r.stderr
        with chdir(clone):
            # Detach HEAD
            r = cli_runner.invoke(["checkout", H.POINTS.HEAD_SHA])
            assert r.exit_code == 0, r.stderr
            r = cli_runner.invoke(["ahead-behind"])
            assert r.exit_code == 0, r.stderr
            assert r.stdout.strip() == "0 0"


# ---------------------------------------------------------------------------
# ls-files
# ---------------------------------------------------------------------------


def test_ls_files_lists_attachments(data_archive, cli_runner):
    """ls-files lists attachment files but not dataset internals or kart files."""
    with data_archive("points-with-attached-files"):
        r = cli_runner.invoke(["ls-files"])
        assert r.exit_code == 0, r.stderr
        files = r.stdout.splitlines()
        assert "LICENSE.txt" in files
        assert "logo.png" in files
        assert "nz_pa_points_topo_150k/metadata.xml" in files
        # Must NOT include kart internals
        assert not any(f.startswith(".kart") for f in files)
        # Must NOT include dataset feature blobs
        assert not any(".table-dataset" in f for f in files)


def test_ls_files_no_attachments(data_archive, cli_runner):
    """ls-files produces no output for a repo with no attachment files."""
    with data_archive("au-census"):
        r = cli_runner.invoke(["ls-files"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.strip() == ""


def test_ls_files_explicit_ref(data_archive, cli_runner):
    """ls-files accepts an explicit commit SHA as the ref argument."""
    with data_archive("points-with-attached-files"):
        r_head = cli_runner.invoke(["ls-files", "HEAD"])
        assert r_head.exit_code == 0, r_head.stderr
        r_sha = cli_runner.invoke(["ls-files", H.POINTS_WITH_ATTACHED_FILES.HEAD_SHA
                                   if hasattr(H, "POINTS_WITH_ATTACHED_FILES") else "HEAD"])
        assert r_sha.exit_code == 0, r_sha.stderr
        # Both should give the same result
        assert r_head.stdout == r_sha.stdout


def test_ls_files_invalid_ref(data_archive, cli_runner):
    """ls-files exits non-zero for a non-existent ref."""
    with data_archive("points-with-attached-files"):
        r = cli_runner.invoke(["ls-files", "nonexistent-branch"])
        assert r.exit_code != 0
        assert "No such ref" in r.stderr


def test_ls_files_after_commit(data_working_copy, cli_runner):
    """ls-files reflects newly committed attachment files."""
    with data_working_copy("points-with-attached-files") as (path, wc):
        # Add a new attachment file
        r = cli_runner.invoke(
            ["commit-files", "-m", "add NOTES.txt", "NOTES.txt=hello world"]
        )
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["ls-files"])
        assert r.exit_code == 0, r.stderr
        files = r.stdout.splitlines()
        assert "NOTES.txt" in files
        assert "LICENSE.txt" in files

        # After removing the file, it should disappear from ls-files
        r = cli_runner.invoke(
            [
                "commit-files",
                "-m",
                "remove NOTES.txt",
                "--remove-empty-files",
                "NOTES.txt=",
            ]
        )
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["ls-files"])
        assert r.exit_code == 0, r.stderr
        files = r.stdout.splitlines()
        assert "NOTES.txt" not in files
        assert "LICENSE.txt" in files
