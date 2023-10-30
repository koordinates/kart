import pytest


from kart.exceptions import UNCOMMITTED_CHANGES, NO_BRANCH, NO_COMMIT
from kart.repo import KartRepo
from kart.structs import CommitWithReference


H = pytest.helpers.helpers()


@pytest.mark.parametrize(
    "working_copy",
    [
        pytest.param(True, id="with-wc"),
        pytest.param(False, id="without-wc"),
    ],
)
def test_checkout_branches(data_archive, cli_runner, chdir, tmp_path, working_copy):
    with data_archive("points") as remote_path:
        r = cli_runner.invoke(["checkout", "-b", "one"])
        assert r.exit_code == 0, r.stderr
        r = cli_runner.invoke(["checkout", "-b", "two", "HEAD^"])
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["checkout", "one", "-b", "three"])
        assert r.exit_code == 0, r.stderr
        r = cli_runner.invoke(["switch", "two", "--create", "four"])
        assert r.exit_code == 0, r.stderr

        repo = KartRepo(remote_path)
        one = CommitWithReference.resolve(repo, "one")
        two = CommitWithReference.resolve(repo, "two")
        three = CommitWithReference.resolve(repo, "three")
        four = CommitWithReference.resolve(repo, "four")

        assert one.commit.hex == three.commit.hex
        assert two.commit.hex == four.commit.hex

        assert one.commit.hex != two.commit.hex

        wc_flag = "--checkout" if working_copy else "--no-checkout"
        r = cli_runner.invoke(["clone", remote_path, tmp_path, wc_flag])
        repo = KartRepo(tmp_path)

        head = CommitWithReference.resolve(repo, "HEAD")

        with chdir(tmp_path):
            r = cli_runner.invoke(["branch"])
            assert r.exit_code == 0, r.stderr
            assert r.stdout.splitlines() == ["* four"]

            # Commit hex is not a branch name, can't be switched:
            r = cli_runner.invoke(["switch", head.commit.hex])
            assert r.exit_code == NO_BRANCH, r.stderr
            # But can be checked out:
            r = cli_runner.invoke(["checkout", head.commit.hex])
            assert r.exit_code == 0, r.stderr

            r = cli_runner.invoke(["checkout", "zero"])
            assert r.exit_code == NO_COMMIT, r.stderr
            r = cli_runner.invoke(["switch", "zero"])
            assert r.exit_code == NO_BRANCH, r.stderr

            r = cli_runner.invoke(["checkout", "one", "--no-guess"])
            assert r.exit_code == NO_COMMIT, r.stderr
            r = cli_runner.invoke(["switch", "one", "--no-guess"])
            assert r.exit_code == NO_BRANCH, r.stderr

            r = cli_runner.invoke(["checkout", "one"])
            assert r.exit_code == 0, r.stderr
            assert (
                r.stdout.splitlines()[0]
                == "Creating new branch 'one' to track 'origin/one'..."
            )
            assert (
                CommitWithReference.resolve(repo, "HEAD").commit.hex == one.commit.hex
            )
            r = cli_runner.invoke(["switch", "two"])
            assert r.exit_code == 0, r.stderr
            assert (
                r.stdout.splitlines()[0]
                == "Creating new branch 'two' to track 'origin/two'..."
            )
            assert (
                CommitWithReference.resolve(repo, "HEAD").commit.hex == two.commit.hex
            )

            r = cli_runner.invoke(["branch"])
            assert r.exit_code == 0, r.stderr
            assert r.stdout.splitlines() == ["  four", "  one", "* two"]


def test_reset(data_working_copy, cli_runner, edit_points):
    with data_working_copy("points") as (repo_path, wc):
        repo = KartRepo(repo_path)
        with repo.working_copy.tabular.session() as sess:
            edit_points(sess)

        r = cli_runner.invoke(["diff", "--exit-code"])
        assert r.exit_code == 1
        r = cli_runner.invoke(
            ["log", "--output-format=text:oneline", "--decorate=short"]
        )
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            f"{H.POINTS.HEAD_SHA} (HEAD -> main) Improve naming on Coromandel East coast",
            f"{H.POINTS.HEAD1_SHA} Import from nz-pa-points-topo-150k.gpkg",
        ]

        r = cli_runner.invoke(["reset", "HEAD^"])
        assert r.exit_code == UNCOMMITTED_CHANGES
        assert "You have uncommitted changes in your working copy." in r.stderr

        r = cli_runner.invoke(["reset", "HEAD^", "--discard-changes"])
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["diff", "--exit-code"])
        assert r.exit_code == 0
        r = cli_runner.invoke(
            ["log", "--output-format=text:oneline", "--decorate=short"]
        )
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            f"{H.POINTS.HEAD1_SHA} (HEAD -> main) Import from nz-pa-points-topo-150k.gpkg",
        ]


def _check_workingcopy_contains_tables(repo, expected_tables):
    with repo.working_copy.tabular.session() as sess:
        r = sess.execute("""SELECT name FROM sqlite_master SM WHERE type='table';""")
        sqlite_table_names = set(row[0] for row in r)

        census_tables = set(t for t in sqlite_table_names if t.startswith("census"))
        assert census_tables == expected_tables

        r = sess.execute("""SELECT table_name FROM gpkg_contents;""")
        gpkg_contents_table_names = set(row[0] for row in r)
        assert gpkg_contents_table_names == expected_tables


def test_non_checkout_datasets(data_working_copy, cli_runner):
    with data_working_copy("au-census") as (repo_path, wc):
        repo = KartRepo(repo_path)
        _check_workingcopy_contains_tables(
            repo, {"census2016_sdhca_ot_sos_short", "census2016_sdhca_ot_ra_short"}
        )

        r = cli_runner.invoke(
            ["checkout", "--not-dataset=census2016-sdhca-ot-sos-short"]
        )
        assert r.exit_code == 2
        assert "No dataset census2016-sdhca-ot-sos-short" in r.stderr

        r = cli_runner.invoke(
            ["checkout", "--not-dataset=census2016_sdhca_ot_sos_short"]
        )
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            "On branch branch1",
            "",
            "User configuration prevents the following datasets from being checked out",
            "  (to overturn, use `kart checkout --dataset=DATASET`):",
            "census2016_sdhca_ot_sos_short",
            "",
            "Nothing to commit, working copy clean",
        ]

        _check_workingcopy_contains_tables(repo, {"census2016_sdhca_ot_ra_short"})

        # No WC changes are returned.
        r = cli_runner.invoke(["diff", "--exit-code"])
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(
            ["checkout", "main", "--dataset=census2016_sdhca_ot_sos_short"]
        )
        assert r.exit_code == 0

        _check_workingcopy_contains_tables(
            repo, {"census2016_sdhca_ot_sos_short", "census2016_sdhca_ot_ra_short"}
        )
