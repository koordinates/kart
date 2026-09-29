"""
Edge-case tests for the attachment-files feature.
Targets the specific fix in this PR:

  `kart/structure.py`: `repo[oid]` raises KeyError for missing OID — replaces
  `repo.get(oid)` which silently returned None and made `bytes(None)` raise a
  confusing TypeError downstream when applying a working-copy diff that
  references a blob no longer present in the ODB.
"""
import pygit2
import pytest


H = pytest.helpers.helpers()


# ---------------------------------------------------------------------------
# repo[oid] error semantics in RepoStructure.apply_files_diff
# ---------------------------------------------------------------------------


def test_repo_subscript_raises_keyerror_for_missing_oid(data_archive):
    """`repo[oid]` raises KeyError for a non-existent OID — pinning the new
    error path in structure.py. The previous `repo.get(oid)` silently returned
    None, and the subsequent `bytes(None)` raised a confusing
    `TypeError: cannot convert 'NoneType' object to bytes`."""
    from kart.repo import KartRepo

    with data_archive("points") as path:
        repo = KartRepo(path)
        bogus_oid = pygit2.Oid(hex="0" * 40)
        # New behaviour: raises KeyError, not silent None.
        with pytest.raises(KeyError):
            _ = repo[bogus_oid]
        # Sanity: repo.get() *would* have returned None for the same OID.
        assert repo.get(bogus_oid) is None


def test_repo_subscript_returns_blob_for_real_oid(data_archive):
    """A real attachment-blob OID resolves via repo[oid] just fine."""
    from kart.repo import KartRepo

    with data_archive("points-with-attached-files") as path:
        repo = KartRepo(path)
        # Resolve LICENSE.txt at HEAD via pygit2 directly.
        license_entry = repo.head_tree["LICENSE.txt"]
        blob = repo[license_entry.id]
        assert blob.type_str == "blob"
        assert len(bytes(blob)) > 0


# ---------------------------------------------------------------------------
# create-workingcopy / clone restore attachment files to disk
# ---------------------------------------------------------------------------


def test_create_workingcopy_restores_attachments(data_working_copy):
    """`kart create-workingcopy` must put tracked attachment files on disk —
    otherwise `kart clone` of a repo with `LICENSE.txt` leaves the file
    missing from the workdir, which is both surprising and breaks downstream
    tooling (the QGIS plugin, IDE git status, etc.)."""
    with data_working_copy("points-with-attached-files") as (path, _wc):
        # The data_working_copy fixture calls `kart create-workingcopy` under
        # the hood; LICENSE.txt should be on disk afterwards with no extra
        # checkout step.
        assert (path / "LICENSE.txt").is_file()
        assert (path / "LICENSE.txt").read_text(encoding="utf-8")
