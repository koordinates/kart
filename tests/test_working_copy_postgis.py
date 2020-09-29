import os
import pytest

from sno.sno_repo import SnoRepo
from sno.working_copy.postgis import WorkingCopy_Postgis


H = pytest.helpers.helpers()


def test_checkout_workingcopy(data_archive, postgis_db):
    with data_archive("points2") as repo_path:
        H.clear_working_copy()

        repo = SnoRepo(repo_path)
        wc = WorkingCopy_Postgis(repo, os.environ["SNO_POSTGRES_URL"])
        wc.create()
