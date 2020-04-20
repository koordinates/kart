import pytest


H = pytest.helpers.helpers()


def test_log(data_archive, cli_runner):
    """ review commit history """
    with data_archive("points"):
        r = cli_runner.invoke(["log"])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines() == [
            "commit 2a1b7be8bdef32aea1510668e3edccbc6d454852",
            "Author: Robert Coup <robert@coup.net.nz>",
            "Date:   Thu Jun 20 15:28:33 2019 +0100",
            "",
            "    Improve naming on Coromandel East coast",
            "",
            "commit 63a9492dd785b1f04dfc446330fa017f9459db4f",
            "Author: Robert Coup <robert@coup.net.nz>",
            "Date:   Tue Jun 11 12:03:58 2019 +0100",
            "",
            "    Import from nz-pa-points-topo-150k.gpkg",
        ]
