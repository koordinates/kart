"""
Edge-case tests for the QGIS plumbing commands added in `kart meta`.
Pins behaviour of `blob-hash`, `ls-files` etc. against malformed input,
unusual targets, and nested paths so future tightening is deliberate.
"""
import pytest


H = pytest.helpers.helpers()


def test_blob_hash_empty_ref_path_resolves_to_root_tree(data_archive, cli_runner):
    """`blob-hash HEAD` (no `:path`) currently resolves to the HEAD root tree
    OID. Strictly the command name says "blob"-hash, but git's revparse semantics
    make `HEAD:` equivalent to the root tree, and the command echoes that.
    Pinned here so a future tightening (rejecting non-blob targets) is a
    deliberate behaviour change, not an accidental regression."""
    with data_archive("points-with-attached-files"):
        r = cli_runner.invoke(["blob-hash", "HEAD"])
        assert r.exit_code == 0, r.stderr
        assert len(r.stdout.strip()) == 40


def test_blob_hash_dataset_internal_blob(data_archive, cli_runner):
    """blob-hash works on dataset-internal paths (not just top-level attachments)."""
    with data_archive("points-with-attached-files"):
        r = cli_runner.invoke(
            ["blob-hash", "HEAD:nz_pa_points_topo_150k/metadata.xml"]
        )
        assert r.exit_code == 0, r.stderr
        assert len(r.stdout.strip()) == 40


def test_ls_files_excludes_dataset_internal_directories(data_archive, cli_runner):
    """ls-files must not list paths inside `.*dataset*` directories ? those
    are dataset internals managed by Kart, not user-managed attachment files
    the QGIS plugin should expose."""
    with data_archive("points-with-attached-files"):
        r = cli_runner.invoke(["ls-files"])
        assert r.exit_code == 0, r.stderr
        listed = r.stdout.splitlines()
        for p in listed:
            parts = p.split("/")[:-1]  # exclude the file basename
            assert not any(
                seg.startswith(".") and "dataset" in seg for seg in parts
            ), f"ls-files leaked dataset-internal path: {p}"


def test_ls_files_includes_top_level_and_dataset_folder_attachments(
    data_archive, cli_runner
):
    """ls-files must list both top-level attachments (LICENSE.txt) and
    dataset-folder attachments (nz_pa_points_topo_150k/metadata.xml)."""
    with data_archive("points-with-attached-files"):
        r = cli_runner.invoke(["ls-files"])
        assert r.exit_code == 0, r.stderr
        listed = set(r.stdout.splitlines())
        assert "LICENSE.txt" in listed
        # metadata.xml lives under the dataset folder but at the *user* level
        # (not inside a `.*dataset*` internal subdirectory) so it is exposed.
        assert any(p.endswith("metadata.xml") for p in listed)


def test_ls_files_invalid_ref_clean_error(data_archive, cli_runner):
    """ls-files with a bogus ref must produce a clean UsageError, not a Python
    traceback (the QGIS plugin parses stderr)."""
    with data_archive("points-with-attached-files"):
        r = cli_runner.invoke(["ls-files", "no-such-ref-xyzzy"])
        assert r.exit_code != 0
        assert "Traceback" not in r.stderr
