from pathlib import Path
import pytest

from kart.repo import KartRepo


@pytest.mark.parametrize("archive", ["points", "polygons", "table"])
@pytest.mark.parametrize("repo_version", [0, 1, 2, 3])
def test_get_repo_version(
    repo_version,
    archive,
    data_archive_readonly,
):
    archive_paths = {
        0: Path("upgrade") / "v0" / f"{archive}0.snow.tgz",
        1: Path("upgrade") / "v1" / f"{archive}.tgz",
        2: Path("upgrade") / "v2.kart" / f"{archive}.tgz",
        3: Path(f"{archive}.tgz"),
    }
    with data_archive_readonly(archive_paths[repo_version]):
        assert KartRepo(".").table_dataset_version == repo_version
