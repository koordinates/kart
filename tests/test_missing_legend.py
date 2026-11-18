import json
from pathlib import Path

import pytest

from kart.repo import KartRepo
from kart.schema import Schema

DS = "nz_pa_points_topo_150k"
NEW_COLUMN = {
    "id": "5f3b2a1c-0d4e-4f6a-9b7c-8d9e0f1a2b3c",
    "name": "log_url",
    "dataType": "text",
}


def _schema_with_new_column(repo):
    return [*repo.datasets()[DS].get_meta_item("schema.json"), NEW_COLUMN]


def _legend_hashes(repo):
    return {blob.name for blob in repo.datasets()[DS].meta_tree / "legend"}


@pytest.mark.parametrize(
    "archive,dataset_dirname",
    [
        pytest.param("points", ".table-dataset", id="v3"),
        pytest.param(
            Path("upgrade") / "v2.kart" / "points.tgz", ".sno-dataset", id="v2"
        ),
    ],
)
def test_commit_files_schema_json_writes_legend(
    archive, dataset_dirname, data_archive, cli_runner
):
    with data_archive(archive) as repo_path:
        new_schema = _schema_with_new_column(KartRepo(repo_path))
        legend_hash = Schema(new_schema).legend.hexhash()

        r = cli_runner.invoke(
            [
                "commit-files",
                "-m",
                "Add a column",
                f"{DS}/{dataset_dirname}/meta/schema.json={json.dumps(new_schema)}",
            ]
        )
        assert r.exit_code == 0, r.stderr

        assert legend_hash in _legend_hashes(KartRepo(repo_path))
