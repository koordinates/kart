import pytest

from sno.sno_repo import SnoRepo
from sno.repository_version import get_repo_version


@pytest.mark.parametrize("archive", ["points", "polygons", "table"])
@pytest.mark.parametrize("repo_version", [0, 1, 2])
def test_get_repo_version(
    repo_version,
    archive,
    data_archive_readonly,
):
    if repo_version == 0:
        archive = f"{archive}0.snow"
    elif repo_version == 2:
        archive = f"{archive}2"
    with data_archive_readonly(archive):
        repo = SnoRepo(".")
        detected_version = get_repo_version(repo, allow_legacy_versions=True)
        assert detected_version == repo_version
