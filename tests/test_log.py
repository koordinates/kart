import pytest


H = pytest.helpers.helpers()


def test_log(data_archive, cli_runner):
    """ review commit history """
    with data_archive("points.snow"):
        r = cli_runner.invoke(["log"])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines() == [
            "commit d1bee0841307242ad7a9ab029dc73c652b9f74f3",
            "Author: Robert Coup <robert@coup.net.nz>",
            "Date:   Thu Jun 20 15:28:33 2019 +0100",
            "",
            "    Improve naming on Coromandel East coast",
            "",
            "commit edd5a4b02a7d2ce608f1839eea5e3a8ddb874e00",
            "Author: Robert Coup <robert@coup.net.nz>",
            "Date:   Tue Jun 11 12:03:58 2019 +0100",
            "",
            "    Import from nz-pa-points-topo-150k.gpkg",
        ]


def test_show(data_archive, cli_runner):
    """ review commit history """
    with data_archive("points.snow"):
        r = cli_runner.invoke(["show"])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines() == [
            "commit d1bee0841307242ad7a9ab029dc73c652b9f74f3",
            "Author: Robert Coup <robert@coup.net.nz>",
            "Date:   Thu Jun 20 15:28:33 2019 +0100",
            "",
            "    Improve naming on Coromandel East coast",
        ]
