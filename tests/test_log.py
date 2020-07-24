import json
import pytest


H = pytest.helpers.helpers()


@pytest.mark.parametrize("output_format", ["text", "json"])
def test_log(output_format, data_archive_readonly, cli_runner):
    """ review commit history """
    with data_archive_readonly("points"):
        extra_args = ["--dataset-changes"] if output_format == "json" else []
        r = cli_runner.invoke(["log", f"--output-format={output_format}"] + extra_args)
        assert r.exit_code == 0, r
        if output_format == "text":
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
        else:
            assert json.loads(r.stdout) == [
                {
                    'commit': '2a1b7be8bdef32aea1510668e3edccbc6d454852',
                    'abbrevCommit': '2a1b7be',
                    'message': 'Improve naming on Coromandel East coast',
                    'refs': ['HEAD -> master'],
                    'authorEmail': 'robert@coup.net.nz',
                    'authorName': 'Robert Coup',
                    'authorTime': '2019-06-20T14:28:33Z',
                    'authorTimeOffset': '+01:00',
                    'commitTime': '2019-06-20T14:28:33Z',
                    'commitTimeOffset': '+01:00',
                    'committerEmail': 'robert@coup.net.nz',
                    'committerName': 'Robert Coup',
                    'parents': ['63a9492dd785b1f04dfc446330fa017f9459db4f'],
                    'abbrevParents': ['63a9492'],
                    'datasetChanges': ['nz_pa_points_topo_150k'],
                },
                {
                    'commit': '63a9492dd785b1f04dfc446330fa017f9459db4f',
                    'abbrevCommit': '63a9492',
                    'message': 'Import from nz-pa-points-topo-150k.gpkg',
                    'refs': [],
                    'authorEmail': 'robert@coup.net.nz',
                    'authorName': 'Robert Coup',
                    'authorTime': '2019-06-11T11:03:58Z',
                    'authorTimeOffset': '+01:00',
                    'commitTime': '2019-06-11T11:03:58Z',
                    'commitTimeOffset': '+01:00',
                    'committerEmail': 'robert@coup.net.nz',
                    'committerName': 'Robert Coup',
                    'parents': [],
                    'abbrevParents': [],
                    'datasetChanges': ['nz_pa_points_topo_150k'],
                },
            ]
