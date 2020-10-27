import json
import pytest


@pytest.mark.parametrize("output_format", ("text", "json"))
@pytest.mark.parametrize(
    "archive_name",
    [
        pytest.param(
            "points",
            id="1",
        ),
        pytest.param("points2", id="2"),
    ],
)
def test_data_ls(archive_name, output_format, data_archive_readonly, cli_runner):
    # All datasets now support getting metadata in either V1 or V2 format,
    # but if you don't specify a particular item, they will show all V2 items -
    # these are more self-explanatory to an end-user.
    with data_archive_readonly(archive_name):
        r = cli_runner.invoke(["data", "ls", "-o", output_format])
        assert r.exit_code == 0, r
        if output_format == "text":
            assert r.stdout.splitlines() == ["nz_pa_points_topo_150k"]
        else:
            output = json.loads(r.stdout)
            assert output == {"sno.data.ls/v1": ["nz_pa_points_topo_150k"]}


@pytest.mark.parametrize("output_format", ("text", "json"))
@pytest.mark.parametrize("repo_version", [1, 2])
def test_data_ls_empty(repo_version, output_format, tmp_path, cli_runner, chdir):
    repo_path = tmp_path / "emptydir"
    r = cli_runner.invoke(["init", repo_path, "--repo-version", repo_version])
    assert r.exit_code == 0
    with chdir(repo_path):
        r = cli_runner.invoke(["data", "ls", "-o", output_format])
        assert r.exit_code == 0, r
        if output_format == "text":
            assert r.stdout.splitlines() == [
                "Empty repository.",
                '  (use "sno import" to add some data)',
            ]
        else:
            output = json.loads(r.stdout)
            assert output == {"sno.data.ls/v1": []}


def test_data_ls_with_ref(data_archive_readonly, cli_runner):
    with data_archive_readonly("points2"):
        r = cli_runner.invoke(["data", "ls", "-o", "json", "HEAD^"])
        assert r.exit_code == 0, r

        output = json.loads(r.stdout)
        assert output == {"sno.data.ls/v1": ["nz_pa_points_topo_150k"]}


@pytest.mark.parametrize("output_format", ("text", "json"))
@pytest.mark.parametrize(
    "archive_name",
    [
        pytest.param(
            "points",
            id="1",
        ),
        pytest.param("points2", id="2"),
    ],
)
def test_data_version(archive_name, output_format, data_archive_readonly, cli_runner):
    version = 2 if archive_name.endswith("2") else 1
    with data_archive_readonly(archive_name):
        r = cli_runner.invoke(["data", "version", "-o", output_format])
        assert r.exit_code == 0, r
        if output_format == "text":
            assert r.stdout.splitlines()[0].endswith(str(version))
        else:
            output = json.loads(r.stdout)
            assert output == {"sno.data.version": version}
