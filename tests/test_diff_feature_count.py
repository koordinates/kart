import json

import pytest

from kart.repo import KartRepo

H = pytest.helpers.helpers()


@pytest.mark.parametrize(
    "head_sha,head1_sha",
    [
        pytest.param(H.POINTS.HEAD_SHA, H.POINTS.HEAD1_SHA, id="commit_hash"),
        pytest.param(H.POINTS.HEAD_TREE_SHA, H.POINTS.HEAD1_TREE_SHA, id="tree_hash"),
    ],
)
@pytest.mark.parametrize(
    "accuracy",
    ["exact", "fast"],
)
def test_feature_count_noops(head_sha, head1_sha, accuracy, data_archive, cli_runner):
    NOOP_SPECS = (
        f"{head_sha[:6]}...{head_sha[:6]}",
        f"{head_sha}...{head_sha}",
        f"{head1_sha}...{head1_sha}",
        "HEAD^1...HEAD^1",
        f"{head_sha}...",
        f"...{head_sha}",
    )

    with data_archive("points"):
        for spec in NOOP_SPECS:
            print(f"noop: {spec}")
            r = cli_runner.invoke(["diff", f"--only-feature-count={accuracy}", spec])
            assert r.exit_code == 0, r


def test_feature_count_commits_exact(data_archive, cli_runner):
    with data_archive("points"):
        r = cli_runner.invoke(["diff", "--only-feature-count=exact", "HEAD^...HEAD"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            "nz_pa_points_topo_150k:",
            "\t5 features changed",
        ]


def test_feature_count_commits_json_output(data_archive, cli_runner):
    with data_archive("points"):
        r = cli_runner.invoke(
            ["diff", "--only-feature-count=exact", "HEAD^...HEAD", "-o", "json"]
        )
        assert r.exit_code == 0, r.stderr
        assert json.loads(r.stdout) == {"nz_pa_points_topo_150k": 5}


def test_feature_count_commits_veryfast(data_archive, cli_runner):
    with data_archive("points"):
        r = cli_runner.invoke(["diff", "--only-feature-count=veryfast", "HEAD^...HEAD"])
        assert r.exit_code == 0, r.stderr

        assert r.stdout.splitlines() == [
            "nz_pa_points_topo_150k:",
            "\t5 features changed",
        ]


def test_feature_count_commits_fast(data_archive, cli_runner):
    with data_archive("points"):
        r = cli_runner.invoke(["diff", "--only-feature-count=fast", "HEAD^...HEAD"])
        assert r.exit_code == 0, r.stderr

        assert r.stdout.splitlines() == [
            "nz_pa_points_topo_150k:",
            "\t5 features changed",
        ]


def test_feature_count_fast_for_string_pks(data_archive, cli_runner):
    with data_archive("string-pks"):
        r = cli_runner.invoke(["diff", "--only-feature-count=fast", "HEAD^?...HEAD"])
        assert r.exit_code == 0, r.stderr

        assert r.stdout.splitlines() == [
            "nz_waca_adjustments:",
            # there's actually 228
            "\t213 features changed",
        ]


def test_feature_count_good_for_string_pks(data_archive, cli_runner):
    with data_archive("string-pks"):
        r = cli_runner.invoke(["diff", "--only-feature-count=good", "HEAD^?...HEAD"])
        assert r.exit_code == 0, r.stderr

        assert r.stdout.splitlines() == [
            "nz_waca_adjustments:",
            # there's actually 228
            "\t229 features changed",
        ]


def test_feature_count_no_working_copy(data_archive, cli_runner):
    with data_archive("points"):
        r = cli_runner.invoke(["diff", "--only-feature-count=fast", "HEAD^"])
        # No working copy
        assert r.exit_code == 45, r.stderr


def test_feature_count_with_working_copy(data_working_copy, cli_runner):
    with data_working_copy("points") as (repo_path, wc):
        # empty
        r = cli_runner.invoke(["diff", "--only-feature-count=exact", "HEAD"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            "0 features changed",
        ]

        # make some changes
        repo = KartRepo(repo_path)
        with repo.working_copy.tabular.session() as sess:
            # this actually undoes a change from the HEAD commit
            r = sess.execute(
                f"UPDATE {H.POINTS.LAYER} SET name_ascii=NULL, name=NULL WHERE fid = 1166;"
            )
            assert r.rowcount == 1

        r = cli_runner.invoke(["diff", "--only-feature-count=exact", "HEAD"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            "nz_pa_points_topo_150k:",
            "\t1 features changed",
        ]

        # 'exact' diff has 4 features changed - HEAD had 5 but one change was undone by the working copy
        r = cli_runner.invoke(["diff", "--only-feature-count=exact", "HEAD^"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            "nz_pa_points_topo_150k:",
            "\t4 features changed",
        ]

        # other accuracy settings just add the WC count to the committed diff count,
        # so we get 6 here instead of 4.
        r = cli_runner.invoke(["diff", "--only-feature-count=good", "HEAD^"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            "nz_pa_points_topo_150k:",
            "\t6 features changed",
        ]
