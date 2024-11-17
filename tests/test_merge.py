import io
import json
import pytest
from unittest.mock import ANY

import pygit2

from kart.apply import apply_patch
from kart.exceptions import SUCCESS, INVALID_OPERATION, NO_CONFLICT, MERGE_CONFLICT
from kart.merge_util import (
    MergedIndex,
    CommitWithReference,
    MERGE_HEAD,
    MERGE_BRANCH,
    MERGE_MSG,
    ALL_MERGE_FILES,
)
from kart.repo import KartRepo, KartRepoState


H = pytest.helpers.helpers()


@pytest.mark.parametrize(
    "data",
    [
        pytest.param(H.POINTS, id="points"),
        pytest.param(H.POLYGONS, id="polygons"),
        pytest.param(H.TABLE, id="table"),
    ],
)
def test_merge_fastforward(data, data_working_copy, cli_runner, insert, request):
    with data_working_copy(data.ARCHIVE) as (repo_path, wc):
        repo = KartRepo(repo_path)
        # new branch
        r = cli_runner.invoke(["checkout", "-b", "changes"])
        assert r.exit_code == 0, r
        assert repo.head.name == "refs/heads/changes"

        h = repo.head.target.hex

        # make some changes
        with repo.working_copy.tabular.session() as sess:
            insert(sess)
            insert(sess)
            commit_id = insert(sess)

        H.git_graph(request, "pre-merge")
        assert repo.head.target.hex == commit_id

        r = cli_runner.invoke(["checkout", "main"])
        assert r.exit_code == 0, r
        assert repo.head.target.hex != commit_id

        r = cli_runner.invoke(["merge", "--ff-only", "changes"])
        assert r.exit_code == 0, r

        H.git_graph(request, "post-merge")

        assert repo.head.name == "refs/heads/main"
        assert repo.head.target.hex == commit_id
        c = repo.head_commit
        assert len(c.parents) == 1
        assert c.parents[0].parents[0].parents[0].hex == h


@pytest.mark.parametrize(
    "data",
    [
        pytest.param(H.POINTS, id="points"),
        pytest.param(H.POLYGONS, id="polygons"),
        pytest.param(H.TABLE, id="table"),
    ],
)
@pytest.mark.parametrize("no_ff_method", ["--no-ff", "--message=custom message"])
def test_merge_fastforward_noff(
    no_ff_method,
    data,
    data_working_copy,
    cli_runner,
    insert,
    request,
    disable_editor,
):
    with data_working_copy(data.ARCHIVE) as (repo_path, wc):
        repo = KartRepo(repo_path)
        # new branch
        r = cli_runner.invoke(["checkout", "-b", "changes"])
        assert r.exit_code == 0, r
        assert repo.head.name == "refs/heads/changes"

        h = repo.head.target.hex

        # make some changes
        with repo.working_copy.tabular.session() as sess:
            insert(sess)
            insert(sess)
            commit_id = insert(sess)

        H.git_graph(request, "pre-merge")
        assert repo.head.target.hex == commit_id

        r = cli_runner.invoke(["checkout", "main"])
        assert r.exit_code == 0, r
        assert repo.head.target.hex != commit_id

        # force creation of a merge commit
        r = cli_runner.invoke(["merge", "changes", no_ff_method, "-o", "json"])
        assert r.exit_code == 0, r

        H.git_graph(request, "post-merge")

        merge_commit_id = json.loads(r.stdout)["kart.merge/v1"]["commit"]

        assert repo.head.name == "refs/heads/main"
        assert repo.head.target.hex == merge_commit_id
        c = repo.head_commit
        assert len(c.parents) == 2
        assert c.parents[0].hex == h
        assert c.parents[1].hex == commit_id
        if no_ff_method == "--no-ff":
            assert c.message == 'Merge branch "changes" into main'
        else:
            assert c.message == "custom message"


@pytest.mark.parametrize(
    "data",
    [
        pytest.param(
            H.POINTS,
            id="points",
        ),
        pytest.param(
            H.POLYGONS,
            id="polygons",
        ),
        pytest.param(H.TABLE, id="table"),
    ],
)
def test_merge_true(
    data,
    data_working_copy,
    cli_runner,
    insert,
    request,
    disable_editor,
):
    with data_working_copy(data.ARCHIVE) as (repo_path, wc_path):
        repo = KartRepo(repo_path)
        table_wc = repo.working_copy.tabular
        # new branch
        r = cli_runner.invoke(["checkout", "-b", "changes"])
        assert r.exit_code == 0, r
        assert repo.head.name == "refs/heads/changes"

        h = repo.head.target.hex

        # make some changes
        with table_wc.session() as sess:
            insert(sess)
            insert(sess)
            b_commit_id = insert(sess)
            assert repo.head.target.hex == b_commit_id

        r = cli_runner.invoke(["checkout", "main"])
        assert r.exit_code == 0, r
        assert repo.head.target.hex != b_commit_id

        with table_wc.session() as sess:
            m_commit_id = insert(sess)
        H.git_graph(request, "pre-merge-main")

        # fastforward merge should fail
        r = cli_runner.invoke(["merge", "--ff-only", "changes"])
        assert r.exit_code == INVALID_OPERATION, r
        assert (
            "Can't resolve as a fast-forward merge and --ff-only specified" in r.stderr
        )

        r = cli_runner.invoke(["merge", "changes", "--ff", "-o", "json"])
        assert r.exit_code == 0, r
        H.git_graph(request, "post-merge")

        merge_commit_id = json.loads(r.stdout)["kart.merge/v1"]["commit"]

        assert repo.head.name == "refs/heads/main"
        assert repo.head.target.hex == merge_commit_id
        c = repo.head_commit
        assert len(c.parents) == 2
        assert c.parents[0].hex == m_commit_id
        assert c.parents[1].hex == b_commit_id
        assert c.parents[0].parents[0].hex == h
        assert c.message == 'Merge branch "changes" into main'

        # check the database state
        num_inserts = len(insert.inserted_fids)

        with table_wc.session() as sess:
            rowcount = 0
            for pk in insert.inserted_fids:
                rowcount += sess.execute(
                    f"SELECT COUNT(*) FROM {data.LAYER} WHERE {data.LAYER_PK} = :pk",
                    {"pk": pk},
                ).fetchone()[0]
            assert rowcount == num_inserts


def test_merge_shallow_clone(data_archive, tmp_path, cli_runner):
    with data_archive("points") as repo1_path:
        repo1_url = f"file://{repo1_path.resolve()}"
        repo2_path = tmp_path / "repo2"

        # Clone only 1 commit from repo with more than one commit:
        r = cli_runner.invoke(["clone", repo1_url, repo2_path, "--depth=1"])
        assert r.exit_code == 0, r.stderr

        # Make a simple commit with that one commit as a parent.
        r = cli_runner.invoke(["-C", repo2_path, "commit-files", "-m", "A1", "a=1"])
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["-C", repo2_path, "checkout", "HEAD^"])
        assert r.exit_code == 0, r.stderr

        # Make another simple commit that clearly doesn't conflict, with the same parent.
        r = cli_runner.invoke(["-C", repo2_path, "commit-files", "-m", "B2", "b=2"])
        assert r.exit_code == 0, r.stderr

        # Merge them together.
        r = cli_runner.invoke(["-C", repo2_path, "merge", "main", "-m", "merged"])
        assert r.exit_code == 0, r.stderr


@pytest.mark.parametrize(
    "data",
    [
        pytest.param(H.POINTS, id="points"),
        pytest.param(H.POLYGONS, id="polygons"),
        pytest.param(H.TABLE, id="table"),
    ],
)
@pytest.mark.parametrize(
    "output_format",
    ["text", "json"],
)
@pytest.mark.parametrize(
    "dry_run",
    [pytest.param(False, id=""), pytest.param(True, id="dryrun")],
)
def test_merge_conflicts(
    data,
    output_format,
    dry_run,
    data_archive,
    cli_runner,
):
    with data_archive(f"conflicts/{data.ARCHIVE}.tgz") as repo_path:
        repo = KartRepo(repo_path)
        ancestor = CommitWithReference.resolve(repo, "ancestor_branch")
        ours = CommitWithReference.resolve(repo, "ours_branch")
        theirs = CommitWithReference.resolve(repo, "theirs_branch")

        cmd = ["merge", "theirs_branch", f"--output-format={output_format}"]
        if dry_run:
            cmd += ["--dry-run"]

        r = cli_runner.invoke(cmd)
        assert r.exit_code == 0, r

        if output_format == "text":
            merging_state_message = (
                ["(Not actually merging due to --dry-run)", ""]
                if dry_run
                else [
                    'Repository is now in "merging" state.',
                    "View conflicts with `kart conflicts` and resolve them with `kart resolve`.",
                    "Once no conflicts remain, complete this merge with `kart merge --continue`.",
                    "Or use `kart merge --abort` to return to the previous state.",
                    "",
                ]
            )

            assert (
                r.stdout.split("\n")
                == [
                    'Merging branch "theirs_branch" into ours_branch',
                    "Conflicts found:",
                    "",
                    f"{data.LAYER}:",
                    f"    {data.LAYER}:feature: 4 conflicts",
                    "",
                ]
                + merging_state_message
            )

        else:
            jdict = json.loads(r.stdout)
            assert jdict == {
                "kart.merge/v1": {
                    "branch": "ours_branch",
                    "commit": ours.id.hex,
                    "merging": {
                        "ancestor": {
                            "commit": ancestor.id.hex,
                            "abbrevCommit": ancestor.short_id,
                        },
                        "ours": {
                            "branch": "ours_branch",
                            "commit": ours.id.hex,
                            "abbrevCommit": ours.short_id,
                        },
                        "theirs": {
                            "branch": "theirs_branch",
                            "commit": theirs.id.hex,
                            "abbrevCommit": theirs.short_id,
                        },
                    },
                    "dryRun": dry_run,
                    "message": 'Merge branch "theirs_branch" into ours_branch',
                    "conflicts": {data.LAYER: {"feature": 4}},
                    "state": "merging",
                },
            }

        if not dry_run:
            assert repo.read_gitdir_file(MERGE_HEAD).strip() == theirs.id.hex
            assert repo.read_gitdir_file(MERGE_BRANCH).strip() == "theirs_branch"
            assert (
                repo.read_gitdir_file(MERGE_MSG)
                == 'Merge branch "theirs_branch" into ours_branch\n'
            )

            merged_index = MergedIndex.read_from_repo(repo)
            assert len(merged_index.conflicts) == 4
            cli_runner.invoke(["merge", "--abort"])

        for filename in ALL_MERGE_FILES:
            assert not repo.gitdir_file(filename).exists()


def test_merge_state_lock(data_archive, cli_runner):
    with data_archive("conflicts/points.tgz") as repo_path:
        repo = KartRepo(repo_path)
        # Repo state: normal
        # kart checkout works, but kart conflicts and kart resolve do not.
        assert repo.state == KartRepoState.NORMAL

        r = cli_runner.invoke(["checkout", "ours_branch"])
        assert r.exit_code == SUCCESS
        r = cli_runner.invoke(["conflicts"])
        assert r.exit_code == INVALID_OPERATION
        r = cli_runner.invoke(["resolve", "dummy_conflict", "--with=delete"])
        assert r.exit_code == INVALID_OPERATION

        r = cli_runner.invoke(["merge", "theirs_branch"])
        assert r.exit_code == SUCCESS

        # Repo state: merging
        assert repo.state == KartRepoState.MERGING

        # kart checkout is locked, but kart conflicts and kart resolve work.
        r = cli_runner.invoke(["checkout", "ours_branch"])
        assert r.exit_code == INVALID_OPERATION
        r = cli_runner.invoke(["conflicts"])
        assert r.exit_code == SUCCESS
        r = cli_runner.invoke(["resolve", "dummy_conflict", "--with=delete"])
        assert r.exit_code == NO_CONFLICT  # "dummy_conflict" is not a real conflict


def test_merge_into_branch(data_archive, tmp_path, cli_runner):
    with data_archive("points") as repo_path:
        # create two branches and put a commit on each
        r = cli_runner.invoke(["branch", "b1", "main"])
        assert r.exit_code == 0, r.stderr
        r = cli_runner.invoke(
            ["commit-files", f"--ref=refs/heads/b1", "-m", "B1", "a=1"]
        )
        assert r.exit_code == 0, r.stderr
        r = cli_runner.invoke(["branch", "b2", "main"])
        assert r.exit_code == 0, r.stderr
        r = cli_runner.invoke(
            ["commit-files", f"--ref=refs/heads/b2", "-m", "B2", "b=1"]
        )
        assert r.exit_code == 0, r.stderr

        # Merge b1 into b2, even though main is still checked out
        r = cli_runner.invoke(["merge", "--into=b2", "b1", "-m", "merged"])
        assert r.exit_code == 0, r.stderr

        repo = KartRepo(repo_path)
        # HEAD is unchanged
        assert repo.head.name == "refs/heads/main"
        head_commit = repo.head_commit
        assert head_commit.message == "Improve naming on Coromandel East coast"

        # b2 ref has a merge commit on it
        b2_commit = repo.references["refs/heads/b2"].peel(pygit2.Commit)
        assert len(b2_commit.parents) == 2
        assert b2_commit.message == "merged"


def test_merge_into_branch_fastforward(data_archive, tmp_path, cli_runner):
    with data_archive("points") as repo_path:
        # create two branches and put a commit on one of them
        r = cli_runner.invoke(["branch", "b1", "main"])
        assert r.exit_code == 0, r.stderr
        r = cli_runner.invoke(
            ["commit-files", f"--ref=refs/heads/b1", "-m", "B1", "a=1"]
        )
        assert r.exit_code == 0, r.stderr
        r = cli_runner.invoke(["branch", "b2", "main"])
        assert r.exit_code == 0, r.stderr

        # Merge b1 into b2, even though main is still checked out
        r = cli_runner.invoke(["merge", "--into=b2", "b1"])
        assert r.exit_code == 0, r.stderr

        repo = KartRepo(repo_path)
        b1_commit = repo.references["refs/heads/b1"].peel(pygit2.Commit)

        # HEAD is unchanged
        assert repo.head.name == "refs/heads/main"
        head_commit = repo.head_commit
        assert head_commit.message == "Improve naming on Coromandel East coast"

        # b2 ref is now the same as b1 (because it was fastforwarded)
        b2_commit = repo.references["refs/heads/b2"].peel(pygit2.Commit)
        assert b2_commit.hex == b1_commit.hex


def _apply_features(repo, features, ref):
    patch = {
        "kart.diff/v1+hexwkb": {
            "nz_pa_points_topo_150k": {
                "feature": features,
            }
        },
        "kart.patch/v1": {"message": "m", "base": repo.references[ref].target.hex},
    }
    patch_file = io.StringIO()
    json.dump(patch, patch_file)
    patch_file.seek(0)
    apply_patch(
        repo=repo,
        ref=ref,
        do_commit=True,
        patch_file=patch_file,
        allow_empty=False,
    )


def test_merge_into_branch_with_conflict(data_archive, tmp_path, cli_runner):
    with data_archive("points") as repo_path:
        # create two branches
        r = cli_runner.invoke(["branch", "b1", "main"])
        assert r.exit_code == 0, r.stderr
        r = cli_runner.invoke(["branch", "b2", "main"])
        assert r.exit_code == 0, r.stderr

        # Apply conflicting patches to two branches
        FEATURE = {
            "fid": 1168,
            "geom": "0101000000FFA26275E7FA65405CAC5D37987E42C0",
            "t50_fid": 2427412,
            "name_ascii": "Tairua",
            "macronated": "N",
            "name": "Tairua",
        }
        repo = KartRepo(repo_path)
        b1_commit = _apply_features(
            repo,
            ref="refs/heads/b1",
            features=[{"-": FEATURE, "+": {**FEATURE, "name": "b1"}}],
        )
        b2_commit = _apply_features(
            repo,
            ref="refs/heads/b2",
            features=[{"-": FEATURE, "+": {**FEATURE, "name": "b2"}}],
        )

        # Merge b1 into b2, even though main is still checked out
        r = cli_runner.invoke(
            [
                "merge",
                "--into=b2",
                "b1",
                "-m",
                "merged",
                "--output-format=json",
                "--fail-on-conflict",
            ]
        )
        assert r.exit_code == MERGE_CONFLICT, r.stderr
        assert r.stderr == "Error: Merge failed due to conflicts\n"
        jdict = json.loads(r.stdout)["kart.merge/v1"]
        jdict.pop("merging")
        assert jdict == {
            "commit": ANY,
            "branch": "b2",
            "message": "merged",
            "conflicts": {"nz_pa_points_topo_150k": {"feature": 1}},
            "dryRun": False,
        }
