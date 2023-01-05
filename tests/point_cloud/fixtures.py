import subprocess
from kart.cli_util import tool_environment
import pytest


# using a fixture instead of a skipif decorator means we get one aggregated skip
# message rather than one per test
@pytest.fixture(scope="session")
def requires_pdal():
    try:
        r = subprocess.run(["pdal", "--version"], env=tool_environment())
        has_pdal = r.returncode == 0
    except OSError:
        has_pdal = False

    pytest.helpers.feature_assert_or_skip(
        "pdal package installed", "KART_EXPECT_PDAL", has_pdal, ci_require=False
    )


@pytest.fixture(scope="session")
def requires_git_lfs():
    try:
        r = subprocess.run(["git", "lfs", "--version"], env=tool_environment())
        has_git_lfs = r.returncode == 0
    except OSError:
        has_git_lfs = False

    pytest.helpers.feature_assert_or_skip(
        "Git LFS installed", "KART_EXPECT_GIT_LFS", has_git_lfs, ci_require=False
    )
