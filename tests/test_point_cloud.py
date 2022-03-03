from glob import glob
import pytest

from kart.exceptions import INVALID_FILE_FORMAT


# using a fixture instead of a skipif decorator means we get one aggregated skip
# message rather than one per test
@pytest.fixture(scope="session")
def requires_pdal():
    has_pdal = False
    try:
        import pdal

        assert pdal.Pipeline
        has_pdal = True
    except ModuleNotFoundError:
        pass

    pytest.helpers.feature_assert_or_skip(
        "pdal package installed", "KART_EXPECT_PDAL", has_pdal, ci_require=False
    )


def test_import_single_las(
    tmp_path, chdir, cli_runner, data_archive_readonly, requires_pdal
):
    # Using postgres here because it has the best type preservation
    with data_archive_readonly("point-cloud/autzen.tgz") as autzen:
        repo_path = tmp_path / "point-cloud-repo"
        r = cli_runner.invoke(["init", repo_path])
        assert r.exit_code == 0, r.stderr
        with chdir(repo_path):
            r = cli_runner.invoke(["point-cloud-import", f"{autzen}/autzen.las"])
            assert r.exit_code == 0, r.stderr


def test_import_several_las(
    tmp_path, chdir, cli_runner, data_archive_readonly, requires_pdal
):
    # Using postgres here because it has the best type preservation
    with data_archive_readonly("point-cloud/auckland.tgz") as auckland:
        repo_path = tmp_path / "point-cloud-repo"
        r = cli_runner.invoke(["init", repo_path])
        assert r.exit_code == 0
        with chdir(repo_path):
            r = cli_runner.invoke(
                ["point-cloud-import", *glob(f"{auckland}/auckland_*.laz")]
            )
            assert r.exit_code == 0, r.stderr


def test_import_mismatched_las(
    tmp_path, chdir, cli_runner, data_archive_readonly, requires_pdal
):
    # Using postgres here because it has the best type preservation
    with data_archive_readonly("point-cloud/auckland.tgz") as auckland:
        with data_archive_readonly("point-cloud/autzen.tgz") as autzen:
            repo_path = tmp_path / "point-cloud-repo"
            r = cli_runner.invoke(["init", repo_path])
            assert r.exit_code == 0
            with chdir(repo_path):
                r = cli_runner.invoke(
                    [
                        "point-cloud-import",
                        *glob(f"{auckland}/auckland_*.laz"),
                        f"{autzen}/autzen.las",
                    ]
                )
                assert r.exit_code == INVALID_FILE_FORMAT
                assert "Non-homogenous" in r.stderr
