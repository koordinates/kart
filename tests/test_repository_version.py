import pygit2

import pytest

from sno.repository_version import get_repo_version


@pytest.mark.parametrize("archive", ["points", "polygons", "table"])
@pytest.mark.parametrize("repo_version", [0, 1, 2])
def test_get_repo_version(
    repo_version, archive, data_archive_readonly,
):
    if repo_version == 0:
        archive = f"{archive}0.snow"
    elif repo_version == 2:
        archive = f"{archive}2"
    with data_archive_readonly(archive):
        repo = pygit2.Repository('.')
        detected_version = get_repo_version(repo)
        assert detected_version == repo_version
