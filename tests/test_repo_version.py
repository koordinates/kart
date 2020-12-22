from pathlib import Path
import pytest

from sno.repo import SnoRepo
from sno.repo_version import get_repo_version


@pytest.mark.parametrize("archive", ["points", "polygons", "table"])
@pytest.mark.parametrize("repo_version", [0, 1, 2])
def test_get_repo_version(
    repo_version,
    archive,
    data_archive_readonly,
):
    archive_paths = {
        0: Path("upgrade") / "v0" / f"{archive}0.snow.tgz",
        1: Path("upgrade") / "v1" / f"{archive}.tgz",
        2: Path(f"{archive}.tgz"),
    }
    with data_archive_readonly(archive_paths[repo_version]):
        repo = SnoRepo(".")
        detected_version = get_repo_version(repo, allow_legacy_versions=True)
        assert detected_version == repo_version
