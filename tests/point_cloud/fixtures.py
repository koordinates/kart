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
