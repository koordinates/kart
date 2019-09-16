import pytest

import pygit2

from snowdrop.structure import RepositoryStructure


H = pytest.helpers.helpers()


def edit_points(dbcur):
    dbcur.execute(H.POINTS_INSERT, H.POINTS_RECORD)
    assert dbcur.rowcount == 1
    dbcur.execute(f"UPDATE {H.POINTS_LAYER} SET fid=9998 WHERE fid=1;")
    assert dbcur.rowcount == 1
    dbcur.execute(f"UPDATE {H.POINTS_LAYER} SET name='test' WHERE fid=2;")
    assert dbcur.rowcount == 1
    dbcur.execute(f"DELETE FROM {H.POINTS_LAYER} WHERE fid IN (3,30,31,32,33);")
    assert dbcur.rowcount == 5
    pk_del = 3
    return pk_del


def edit_polygons_pk(dbcur):
    dbcur.execute(H.POLYGONS_INSERT, H.POLYGONS_RECORD)
    assert dbcur.rowcount == 1
    dbcur.execute(f"UPDATE {H.POLYGONS_LAYER} SET id=9998 WHERE id=1424927;")
    assert dbcur.rowcount == 1
    dbcur.execute(f"UPDATE {H.POLYGONS_LAYER} SET survey_reference='test' WHERE id=1443053;")
    assert dbcur.rowcount == 1
    dbcur.execute(f"DELETE FROM {H.POLYGONS_LAYER} WHERE id IN (1452332, 1456853, 1456912, 1457297, 1457355);")
    assert dbcur.rowcount == 5
    pk_del = 1452332
    return pk_del


def edit_table(dbcur):
    dbcur.execute(H.TABLE_INSERT, H.TABLE_RECORD)
    assert dbcur.rowcount == 1
    dbcur.execute(f"UPDATE {H.TABLE_LAYER} SET OBJECTID=9998 WHERE OBJECTID=1;")
    assert dbcur.rowcount == 1
    dbcur.execute(f"UPDATE {H.TABLE_LAYER} SET name='test' WHERE OBJECTID=2;")
    assert dbcur.rowcount == 1
    dbcur.execute(f"DELETE FROM {H.TABLE_LAYER} WHERE OBJECTID IN (3,30,31,32,33);")
    assert dbcur.rowcount == 5
    pk_del = 3
    return pk_del


@pytest.mark.parametrize("archive,layer", [
    pytest.param('points.snow', H.POINTS_LAYER, id='points'),
    pytest.param('polygons.snow', H.POLYGONS_LAYER, id='polygons_pk'),
    pytest.param('table.snow', H.TABLE_LAYER, id='table'),
])
def test_commit(archive, layer, data_working_copy, geopackage, cli_runner, request):
    """ commit outstanding changes from the working copy """
    param_ids = H.parameter_ids(request)

    with data_working_copy(archive) as (repo, wc):
        # empty
        r = cli_runner.invoke(["commit", "-m", "test-commit-0"])
        assert r.exit_code == 1, r
        assert r.stdout.splitlines() == ['Error: No changes to commit']

        # make some changes
        db = geopackage(wc)
        with db:
            cur = db.cursor()
            try:
                edit_func = globals()[f"edit_{param_ids[0]}"]
                pk_del = edit_func(cur)
            except KeyError:
                raise NotImplementedError(f"layer={layer}")

        fk_del = cur.execute(
            f"SELECT feature_key FROM __kxg_map WHERE table_name=? AND feature_id=?;",
            [layer, pk_del]
        ).fetchone()[0]
        print(f"deleted fid={pk_del}, feature_key={fk_del}")

        r = cli_runner.invoke(["commit", "-m", "test-commit-1"])
        assert r.exit_code == 0, r
        commit_id = r.stdout.splitlines()[-1].split(": ")[1]
        print("commit:", commit_id)

        r = pygit2.Repository(str(repo))
        assert str(r.head.target) == commit_id

        tree = r.head.peel(pygit2.Tree)
        assert f"{layer}/features/{fk_del[:4]}/{fk_del}/geom" not in tree

        change_count = cur.execute(
            "SELECT COUNT(*) FROM __kxg_map WHERE table_name=? AND state!=0;",
            [layer]
        ).fetchone()[0]
        assert change_count == 0, "Changes still listed in __kxg_map"

        del_map_record = cur.execute(
            "SELECT 1 FROM __kxg_map WHERE table_name=? AND feature_key=?;",
            [layer, fk_del]
        ).fetchone()
        assert del_map_record is None, "Deleted feature still in __kxg_map"

        map_count, feature_count = cur.execute(
            f"""
                SELECT
                    (SELECT COUNT(*) FROM __kxg_map WHERE table_name=?) AS map_count,
                    (SELECT COUNT(*) FROM {layer}) AS feature_count;
            """,
            [layer]
        ).fetchone()
        print("map_count=", map_count, "feature_count=", feature_count)
        assert map_count == feature_count

        r = cli_runner.invoke(["diff"])
        assert r.exit_code == 0, r
        assert r.stdout == ''


@pytest.mark.parametrize("archive,layer", [
    pytest.param('points2', H.POINTS_LAYER, id='points'),
])
def test_commit2(archive, layer, data_working_copy, geopackage, cli_runner, request):
    """ commit outstanding changes from the working copy """
    param_ids = H.parameter_ids(request)

    with data_working_copy(archive) as (repo_dir, wc_path):
        # empty
        r = cli_runner.invoke(["commit", "-m", "test-commit-0"])
        assert r.exit_code == 1, r
        assert r.stdout.splitlines() == ['Error: No changes to commit']

        # make some changes
        db = geopackage(wc_path)
        with db:
            cur = db.cursor()
            try:
                edit_func = globals()[f"edit_{param_ids[0]}"]
                pk_del = edit_func(cur)
            except KeyError:
                raise NotImplementedError(f"layer={layer}")

        print(f"deleted fid={pk_del}")

        r = cli_runner.invoke(["commit", "-m", "test-commit-1"])
        assert r.exit_code == 0, r
        commit_id = r.stdout.splitlines()[-1].split(": ")[1]
        print("commit:", commit_id)

        repo = pygit2.Repository(str(repo_dir))
        assert str(repo.head.target) == commit_id

        rs = RepositoryStructure(repo)
        wc = rs.working_copy
        dataset = rs[layer]

        tree = repo.head.peel(pygit2.Tree)
        assert dataset.get_feature_path(pk_del) not in tree

        change_count = cur.execute(
            f"SELECT COUNT(*) FROM {wc.TRACKING_TABLE} WHERE table_name=?;",
            [layer]
        ).fetchone()[0]
        assert change_count == 0, f"Changes still listed in {dataset.TRACKING_TABLE}"

        r = cli_runner.invoke(["diff"])
        assert r.exit_code == 0, r
        assert r.stdout == ''


def test_tag(data_working_copy, cli_runner):
    """ review commit history """
    with data_working_copy("points.snow") as (repo_dir, wc):
        # create a tag
        r = cli_runner.invoke(["tag", "version1"])
        assert r.exit_code == 0, r

        repo = pygit2.Repository(str(repo_dir))
        assert 'refs/tags/version1' in repo.references
        ref = repo.lookup_reference_dwim('version1')
        assert ref.target.hex == H.POINTS_HEAD_SHA
