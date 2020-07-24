import pygit2

import pytest

from sno.structure_version import get_structure_version


@pytest.mark.parametrize("archive", ["points", "polygons", "table"])
@pytest.mark.parametrize("structure_version", [0, 1, 2])
def test_get_structure_version(
    structure_version, archive, data_archive_readonly,
):
    if structure_version == 0:
        archive = f"{archive}0.snow"
    elif structure_version == 2:
        archive = f"{archive}2"
    with data_archive_readonly(archive):
        repo = pygit2.Repository('.')
        detected_version = get_structure_version(repo)
        assert detected_version == structure_version
