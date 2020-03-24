import pytest

import pygit2


H = pytest.helpers.helpers()


@pytest.mark.parametrize(
    "archive",
    [
        pytest.param("points", id="points"),
        pytest.param("polygons", id="polygons-pk"),
        pytest.param("table", id="table"),
    ],
)
def test_merge_fastforward(
    archive, data_working_copy, geopackage, cli_runner, insert, request
):
    with data_working_copy(archive) as (repo_path, wc):
        repo = pygit2.Repository(str(repo_path))
        # new branch
        r = cli_runner.invoke(["checkout", "-b", "changes"])
        assert r.exit_code == 0, r
        assert repo.head.name == "refs/heads/changes"

        h = repo.head.target.hex

        # make some changes
        db = geopackage(wc)
        insert(db)
        insert(db)
        commit_id = insert(db)

        H.git_graph(request, "pre-merge")
        assert repo.head.target.hex == commit_id

        r = cli_runner.invoke(["checkout", "master"])
        assert r.exit_code == 0, r
        assert repo.head.target.hex != commit_id

        r = cli_runner.invoke(["merge", "--ff-only", "changes"])
        assert r.exit_code == 0, r

        H.git_graph(request, "post-merge")

        assert repo.head.name == "refs/heads/master"
        assert repo.head.target.hex == commit_id
        c = repo.head.peel(pygit2.Commit)
        assert len(c.parents) == 1
        assert c.parents[0].parents[0].parents[0].hex == h


@pytest.mark.parametrize(
    "archive",
    [
        pytest.param("points", id="points"),
        pytest.param("polygons", id="polygons-pk"),
        pytest.param("table", id="table"),
    ],
)
def test_merge_fastforward_noff(
    archive, data_working_copy, geopackage, cli_runner, insert, request
):
    with data_working_copy(archive) as (repo_path, wc):
        repo = pygit2.Repository(str(repo_path))
        # new branch
        r = cli_runner.invoke(["checkout", "-b", "changes"])
        assert r.exit_code == 0, r
        assert repo.head.name == "refs/heads/changes"

        h = repo.head.target.hex

        # make some changes
        db = geopackage(wc)
        insert(db)
        insert(db)
        commit_id = insert(db)

        H.git_graph(request, "pre-merge")
        assert repo.head.target.hex == commit_id

        r = cli_runner.invoke(["checkout", "master"])
        assert r.exit_code == 0, r
        assert repo.head.target.hex != commit_id

        # force creation of a merge commit
        r = cli_runner.invoke(["merge", "--no-ff", "changes"])
        assert r.exit_code == 0, r

        H.git_graph(request, "post-merge")

        merge_commit_id = r.stdout.splitlines()[-2].split(": ")[1]

        assert repo.head.name == "refs/heads/master"
        assert repo.head.target.hex == merge_commit_id
        c = repo.head.peel(pygit2.Commit)
        assert len(c.parents) == 2
        assert c.parents[0].hex == h
        assert c.parents[1].hex == commit_id
        assert c.message == "Merge 'changes'"


@pytest.mark.parametrize(
    "archive,layer,pk_field",
    [
        pytest.param("points", H.POINTS_LAYER, H.POINTS_LAYER_PK, id="points"),
        pytest.param(
            "polygons", H.POLYGONS_LAYER, H.POLYGONS_LAYER_PK, id="polygons-pk"
        ),
        pytest.param("table", H.TABLE_LAYER, H.TABLE_LAYER_PK, id="table"),
    ],
)
def test_merge_true(
    archive, layer, pk_field, data_working_copy, geopackage, cli_runner, insert, request
):
    with data_working_copy(archive) as (repo_path, wc):
        repo = pygit2.Repository(str(repo_path))
        # new branch
        r = cli_runner.invoke(["checkout", "-b", "changes"])
        assert r.exit_code == 0, r
        assert repo.head.name == "refs/heads/changes"

        h = repo.head.target.hex

        # make some changes
        db = geopackage(wc)
        dbcur = db.cursor()
        insert(db)
        insert(db)
        b_commit_id = insert(db)
        assert repo.head.target.hex == b_commit_id

        r = cli_runner.invoke(["checkout", "master"])
        assert r.exit_code == 0, r
        assert repo.head.target.hex != b_commit_id
        m_commit_id = insert(db)
        H.git_graph(request, "pre-merge-master")

        # fastforward merge should fail
        r = cli_runner.invoke(["merge", "--ff-only", "changes"])
        assert r.exit_code == 1, r
        assert (
            r.stdout.splitlines()[-1]
            == "Can't resolve as a fast-forward merge and --ff-only specified"
        )

        r = cli_runner.invoke(["merge", "--ff", "changes"])
        assert r.exit_code == 0, r
        H.git_graph(request, "post-merge")

        merge_commit_id = r.stdout.splitlines()[-2].split(": ")[1]

        assert repo.head.name == "refs/heads/master"
        assert repo.head.target.hex == merge_commit_id
        c = repo.head.peel(pygit2.Commit)
        assert len(c.parents) == 2
        assert c.parents[0].hex == m_commit_id
        assert c.parents[1].hex == b_commit_id
        assert c.parents[0].parents[0].hex == h
        assert c.message == "Merge 'changes'"

        # check the database state
        num_inserts = len(insert.inserted_fids)
        dbcur.execute(
            f"SELECT COUNT(*) FROM {layer} WHERE {pk_field} IN ({','.join(['?']*num_inserts)});",
            insert.inserted_fids,
        )
        assert dbcur.fetchone()[0] == num_inserts
