import json

import pytest

from kart.object_builder import ObjectBuilder
from kart.repo import KartRepo
from kart.schema import Schema
from kart.serialise_util import msg_pack


H = pytest.helpers.helpers()


def test_fsck(data_working_copy, cli_runner):
    with data_working_copy("points") as (repo_path, wc):
        repo = KartRepo(repo_path)

        r = cli_runner.invoke(["fsck"])
        assert r.exit_code == 0, r.stdout

        with repo.working_copy.tabular.session() as sess:
            assert H.row_count(sess, H.POINTS.LAYER) == H.POINTS.ROWCOUNT
            assert H.row_count(sess, "gpkg_kart_track") == 0

            # introduce a feature mismatch
            sess.execute(f"UPDATE {H.POINTS.LAYER} SET name='fred' WHERE fid=1;")
            sess.execute("""DELETE FROM "gpkg_kart_track" WHERE pk='1';""")

        r = cli_runner.invoke(["fsck"])
        assert r.exit_code == 1, r

        r = cli_runner.invoke(["fsck", "--reset-dataset=nz_pa_points_topo_150k"])
        assert r.exit_code == 0, r

        with repo.working_copy.tabular.session() as sess:
            assert H.row_count(sess, H.POINTS.LAYER) == H.POINTS.ROWCOUNT
            assert H.row_count(sess, "gpkg_kart_track") == 0

        r = cli_runner.invoke(["fsck"])
        assert r.exit_code == 0, r


def _commit_raw(repo, message, changes):
    """Commits the given {path: bytes-or-None} changes directly, bypassing Kart's dataset logic."""
    object_builder = ObjectBuilder(repo, repo.head_tree)
    for path, data in changes.items():
        object_builder.insert(path, data)
    sig = repo.author_signature()
    object_builder.commit("HEAD", sig, sig, message, [repo.head.target])


def _without_working_copy(repo_path):
    repo = KartRepo(repo_path)
    del repo.config["kart.workingcopy.location"]
    return repo


def test_fsck_checks_features_without_working_copy(data_archive, cli_runner):
    with data_archive("points") as repo_path:
        _without_working_copy(repo_path)

        r = cli_runner.invoke(["fsck"])
        assert r.exit_code == 0, r.stdout
        assert f"{H.POINTS.ROWCOUNT} features checked" in r.stdout


def test_fsck_features_with_missing_legend(data_archive, cli_runner):
    with data_archive("points") as repo_path:
        repo = _without_working_copy(repo_path)
        legend_hash = repo.datasets()[H.POINTS.LAYER].schema.legend.hexhash()
        _commit_raw(
            repo,
            "Remove legend",
            {f"{H.POINTS.LAYER}/.table-dataset/meta/legend/{legend_hash}": None},
        )

        r = cli_runner.invoke(["fsck"])
        assert r.exit_code == 1, r.stdout
        assert f"Dataset: '{H.POINTS.LAYER}/'" in r.stdout
        assert (
            f"{H.POINTS.ROWCOUNT} features refer to missing legend {legend_hash}"
            in r.stdout
        )


def test_fsck_schema_with_missing_legend(data_archive, cli_runner):
    with data_archive("points") as repo_path:
        repo = _without_working_copy(repo_path)
        new_schema = [
            *repo.datasets()[H.POINTS.LAYER].get_meta_item("schema.json"),
            {
                "id": "5f3b2a1c-0d4e-4f6a-9b7c-8d9e0f1a2b3c",
                "name": "log_url",
                "dataType": "text",
            },
        ]
        legend_hash = Schema(new_schema).legend.hexhash()
        _commit_raw(
            repo,
            "Schema without legend",
            {
                f"{H.POINTS.LAYER}/.table-dataset/meta/schema.json": json.dumps(
                    new_schema
                ).encode()
            },
        )

        r = cli_runner.invoke(["fsck"])
        assert r.exit_code == 1, r.stdout
        assert f"Legend {legend_hash} of the current schema is missing" in r.stdout
        # The features themselves still use the old legend, which is present.
        assert f"{H.POINTS.ROWCOUNT} features checked" in r.stdout


def test_fsck_stops_when_git_fsck_fails(data_archive, cli_runner):
    with data_archive("points") as repo_path:
        _without_working_copy(repo_path)

        r = cli_runner.invoke(["fsck", "--not-a-git-fsck-option"])
        assert r.exit_code == 1, r.stdout
        assert "features checked" not in r.stdout


def test_fsck_many_refs(data_archive, cli_runner, monkeypatch):
    # More heads than fit in one `git fsck` invocation.
    monkeypatch.setattr("kart.fsck.MAX_HEADS_PER_GIT_FSCK", 1)
    with data_archive("points") as repo_path:
        repo = _without_working_copy(repo_path)
        repo.references.create("refs/heads/older", repo.head_commit.parents[0].id)

        r = cli_runner.invoke(["fsck"])
        assert r.exit_code == 0, r.stdout
        assert f"{H.POINTS.ROWCOUNT} features checked" in r.stdout


def test_fsck_legend_is_not_a_blob(data_archive, cli_runner):
    with data_archive("points") as repo_path:
        repo = _without_working_copy(repo_path)
        legend_hash = repo.datasets()[H.POINTS.LAYER].schema.legend.hexhash()
        legend_path = f"{H.POINTS.LAYER}/.table-dataset/meta/legend/{legend_hash}"
        _commit_raw(repo, "Remove legend", {legend_path: None})
        _commit_raw(repo, "Add a tree in its place", {f"{legend_path}/x": b"y"})

        r = cli_runner.invoke(["fsck"])
        assert r.exit_code == 1, r.stdout
        assert f"Legend {legend_hash} of the current schema is missing" in r.stdout
        assert (
            f"{H.POINTS.ROWCOUNT} features refer to missing legend {legend_hash}"
            in r.stdout
        )


def test_fsck_spatially_filtered_clone(data_archive, cli_runner):
    with data_archive("points-spatial-filtered") as repo_path:
        _without_working_copy(repo_path)

        r = cli_runner.invoke(["fsck"])
        assert r.exit_code == 0, r.stdout
        assert "1326 features not present locally were skipped" in r.stdout
        assert "817 features checked" in r.stdout


def test_fsck_feature_with_non_string_legend(data_archive, cli_runner):
    with data_archive("points") as repo_path:
        repo = _without_working_copy(repo_path)
        feature_path = repo.datasets()[H.POINTS.LAYER].encode_1pk_to_path(1)
        _commit_raw(repo, "Malformed feature", {feature_path: msg_pack([123, []])})

        r = cli_runner.invoke(["fsck"])
        assert r.exit_code == 1, r.stdout
        assert "1 features could not be decoded" in r.stdout
        assert "missing legend" not in r.stdout
