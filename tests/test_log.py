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
                "commit 0c64d8211c072a08d5fc6e6fe898cbb59fc83d16",
                "Author: Robert Coup <robert@coup.net.nz>",
                "Date:   Thu Jun 20 15:28:33 2019 +0100",
                "",
                "    Improve naming on Coromandel East coast",
                "",
                "commit 7bc3b56f20d1559208bcf5bb56860dda6e190b70",
                "Author: Robert Coup <robert@coup.net.nz>",
                "Date:   Tue Jun 11 12:03:58 2019 +0100",
                "",
                "    Import from nz-pa-points-topo-150k.gpkg",
            ]
        else:
            assert json.loads(r.stdout) == [
                {
                    "commit": "0c64d8211c072a08d5fc6e6fe898cbb59fc83d16",
                    "abbrevCommit": "0c64d82",
                    "message": "Improve naming on Coromandel East coast",
                    "refs": ["HEAD -> main"],
                    "authorEmail": "robert@coup.net.nz",
                    "authorName": "Robert Coup",
                    "authorTime": "2019-06-20T14:28:33Z",
                    "authorTimeOffset": "+01:00",
                    "commitTime": "2019-06-20T14:28:33Z",
                    "commitTimeOffset": "+01:00",
                    "committerEmail": "robert@coup.net.nz",
                    "committerName": "Robert Coup",
                    "parents": ["7bc3b56f20d1559208bcf5bb56860dda6e190b70"],
                    "abbrevParents": ["7bc3b56"],
                    "datasetChanges": ["nz_pa_points_topo_150k"],
                },
                {
                    "commit": "7bc3b56f20d1559208bcf5bb56860dda6e190b70",
                    "abbrevCommit": "7bc3b56",
                    "message": "Import from nz-pa-points-topo-150k.gpkg",
                    "refs": [],
                    "authorEmail": "robert@coup.net.nz",
                    "authorName": "Robert Coup",
                    "authorTime": "2019-06-11T11:03:58Z",
                    "authorTimeOffset": "+01:00",
                    "commitTime": "2019-06-11T11:03:58Z",
                    "commitTimeOffset": "+01:00",
                    "committerEmail": "robert@coup.net.nz",
                    "committerName": "Robert Coup",
                    "parents": [],
                    "abbrevParents": [],
                    "datasetChanges": ["nz_pa_points_topo_150k"],
                },
            ]


@pytest.mark.parametrize("output_format", ["text", "json"])
def test_log_shallow_clone(
    output_format, data_archive_readonly, cli_runner, tmp_path, chdir
):
    """ review commit history """
    with data_archive_readonly("points") as path:

        clone_path = tmp_path / "shallow.clone"
        r = cli_runner.invoke(
            ["clone", "--bare", "--depth=1", f"file://{path}", str(clone_path)]
        )
        assert r.exit_code == 0, r.stderr

        with chdir(clone_path):
            r = cli_runner.invoke(["log", f"--output-format={output_format}"])
            assert r.exit_code == 0, r.stderr

        if output_format == "text":
            assert r.stdout.splitlines() == [
                "commit 0c64d8211c072a08d5fc6e6fe898cbb59fc83d16",
                "Author: Robert Coup <robert@coup.net.nz>",
                "Date:   Thu Jun 20 15:28:33 2019 +0100",
                "",
                "    Improve naming on Coromandel East coast",
            ]
        else:
            assert json.loads(r.stdout) == [
                {
                    "commit": "0c64d8211c072a08d5fc6e6fe898cbb59fc83d16",
                    "abbrevCommit": "0c64d82",
                    "message": "Improve naming on Coromandel East coast",
                    "refs": ["grafted", "HEAD -> main"],
                    "authorEmail": "robert@coup.net.nz",
                    "authorName": "Robert Coup",
                    "authorTime": "2019-06-20T14:28:33Z",
                    "authorTimeOffset": "+01:00",
                    "commitTime": "2019-06-20T14:28:33Z",
                    "commitTimeOffset": "+01:00",
                    "committerEmail": "robert@coup.net.nz",
                    "committerName": "Robert Coup",
                    "parents": ["7bc3b56f20d1559208bcf5bb56860dda6e190b70"],
                    "abbrevParents": ["7bc3b56f20d1559208bcf5bb56860dda6e190b70"],
                },
            ]


def test_log_with_feature_count(data_archive_readonly, cli_runner):
    """ review commit history """
    with data_archive_readonly("points"):
        r = cli_runner.invoke(
            ["log", "--output-format=json", "--with-feature-counts=exact"]
        )
        assert r.exit_code == 0, r
        result = json.loads(r.stdout)
        result = [c["featureChanges"] for c in result]
        assert result == [
            {"nz_pa_points_topo_150k": 5},
            {"nz_pa_points_topo_150k": 2143},
        ]
        r = cli_runner.invoke(
            ["log", "--output-format=json", "--with-feature-counts=good"]
        )
        assert r.exit_code == 0, r
        result = json.loads(r.stdout)
        result = [c["featureChanges"] for c in result]
        assert result == [
            {"nz_pa_points_topo_150k": 5},
            {"nz_pa_points_topo_150k": 2480},
        ]
