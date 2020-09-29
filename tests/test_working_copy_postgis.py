import os
import pytest

from sno.sno_repo import SnoRepo

from sno.working_copy import WorkingCopy


H = pytest.helpers.helpers()


@pytest.mark.parametrize(
    "archive,table,commit_sha",
    [
        pytest.param("points", H.POINTS.LAYER, H.POINTS.HEAD_SHA, id="points"),
        pytest.param(
            "polygons", H.POLYGONS.LAYER, H.POLYGONS.HEAD_SHA, id="polygons-pk"
        ),
        pytest.param("table", H.TABLE.LAYER, H.TABLE.HEAD_SHA, id="table"),
    ],
)
@pytest.mark.parametrize("version", ["1", "2"])
def test_checkout_workingcopy(
    version, archive, table, commit_sha, data_archive, cli_runner, postgis_db
):
    """ Checkout a working copy to edit """
    postgres_url = os.environ["SNO_POSTGRES_URL"]

    with data_archive(archive) as repo_path:
        H.clear_working_copy()

        repo = SnoRepo(repo_path)
        repo.config["sno.workingcopy.path"] = postgres_url
        r = cli_runner.invoke(["checkout"])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines() == [f"Creating working copy at {postgres_url} ..."]

        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines() == [
            "On branch master",
            "",
            "Nothing to commit, working copy clean",
        ]

        # FIXME -
        # Make some edits to show that diffs actually work.
        # Modify data editing fixtures eg insert(), edit() to work on postgres too.

        wc = WorkingCopy.get(repo)
        assert wc.is_created()
        wc.delete()
