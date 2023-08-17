import json
from pathlib import Path
import pytest
import re
import subprocess

import sqlalchemy

from kart.exceptions import INVALID_ARGUMENT, INVALID_OPERATION, UNCOMMITTED_CHANGES
from kart.repo import KartRepo
from kart.sqlalchemy.adapter.gpkg import KartAdapter_GPKG
from kart.tabular.working_copy.base import TableWorkingCopy
from test_working_copy import compute_approximated_types


H = pytest.helpers.helpers()


@pytest.mark.parametrize(
    "archive,table,commit_sha",
    [
        pytest.param("points", H.POINTS.LAYER, H.POINTS.HEAD_SHA, id="points"),
        pytest.param("polygons", H.POLYGONS.LAYER, H.POLYGONS.HEAD_SHA, id="polygons"),
        pytest.param("table", H.TABLE.LAYER, H.TABLE.HEAD_SHA, id="table"),
    ],
)
def test_checkout_workingcopy(
    archive, table, commit_sha, data_archive, tmp_path, cli_runner
):
    """Checkout a working copy to edit"""
    with data_archive(archive) as repo_path:
        H.clear_working_copy()

        repo = KartRepo(repo_path)
        dataset = repo.datasets()[table]
        geom_cols = dataset.schema.geometry_columns

        r = cli_runner.invoke(["checkout"])
        wc_path = Path(repo.config["kart.workingcopy.location"])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines() == [f"Creating GPKG working copy at {wc_path} ..."]
        assert wc_path.exists()
        table_wc = repo.working_copy.tabular

        assert repo.head.name == "refs/heads/main"
        assert repo.head.shorthand == "main"
        assert table_wc.get_tree_id() == repo.head_tree.hex

        if geom_cols:
            with table_wc.session() as sess:
                spatial_index_count = sess.execute(
                    f"""SELECT COUNT(*) FROM "rtree_{table}_{geom_cols[0].name}";"""
                ).scalar()
                assert spatial_index_count == dataset.feature_count

        table_spec = KartAdapter_GPKG.v2_schema_to_sql_spec(dataset.schema)
        expected_col_spec = f"{KartAdapter_GPKG.quote(dataset.primary_key)} INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL"
        assert expected_col_spec in table_spec


def test_checkout_detached(data_working_copy, cli_runner):
    """Checkout a working copy to edit"""
    with data_working_copy("points") as (repo_dir, wc):
        repo = KartRepo(repo_dir)
        with repo.working_copy.tabular.session() as sess:
            assert H.last_change_time(sess) == "2019-06-20T14:28:33.000000Z"

        # checkout the previous commit
        r = cli_runner.invoke(["checkout", H.POINTS.HEAD1_SHA[:8]])
        assert r.exit_code == 0, r

        with repo.working_copy.tabular.session() as sess:
            assert H.last_change_time(sess) == "2019-06-11T11:03:58.000000Z"

        assert repo.head.target.hex == H.POINTS.HEAD1_SHA
        assert repo.head_is_detached
        assert repo.head.name == "HEAD"


def test_checkout_references(data_working_copy, cli_runner, tmp_path):
    with data_working_copy("points") as (repo_dir, wc_path):
        repo = KartRepo(repo_dir)
        table_wc = repo.working_copy.tabular

        # create a tag
        repo.create_reference("refs/tags/version1", repo.head.target)

        subprocess.run(["git", "init", "--bare", str(tmp_path)], check=True)

        r = cli_runner.invoke(["remote", "add", "myremote", tmp_path])
        assert r.exit_code == 0, r

        r = cli_runner.invoke(["push", "myremote", "main"])
        assert r.exit_code == 0, r

        def r_head():
            return (repo.head.name, repo.head.target.hex)

        # checkout the HEAD commit
        r = cli_runner.invoke(["checkout", "HEAD"])
        assert r.exit_code == 0, r
        assert r_head() == ("refs/heads/main", H.POINTS.HEAD_SHA)
        assert not repo.head_is_detached
        with table_wc.session() as sess:
            assert H.last_change_time(sess) == "2019-06-20T14:28:33.000000Z"

        # checkout the HEAD-but-1 commit
        r = cli_runner.invoke(["checkout", "HEAD~1"])
        assert r.exit_code == 0, r
        assert r_head() == ("HEAD", H.POINTS.HEAD1_SHA)
        assert repo.head_is_detached
        with table_wc.session() as sess:
            assert H.last_change_time(sess) == "2019-06-11T11:03:58.000000Z"

        # checkout the main HEAD via branch-name
        r = cli_runner.invoke(["checkout", "main"])
        assert r.exit_code == 0, r
        assert r_head() == ("refs/heads/main", H.POINTS.HEAD_SHA)
        assert not repo.head_is_detached
        with table_wc.session() as sess:
            assert H.last_change_time(sess) == "2019-06-20T14:28:33.000000Z"

        # checkout a short-sha commit
        r = cli_runner.invoke(["checkout", H.POINTS.HEAD1_SHA[:8]])
        assert r.exit_code == 0, r
        assert r_head() == ("HEAD", H.POINTS.HEAD1_SHA)
        assert repo.head_is_detached
        with table_wc.session() as sess:
            assert H.last_change_time(sess) == "2019-06-11T11:03:58.000000Z"

        # checkout the main HEAD via refspec
        r = cli_runner.invoke(["checkout", "refs/heads/main"])
        assert r.exit_code == 0, r
        assert r_head() == ("refs/heads/main", H.POINTS.HEAD_SHA)
        assert not repo.head_is_detached
        with table_wc.session() as sess:
            assert H.last_change_time(sess) == "2019-06-20T14:28:33.000000Z"

        # checkout the tag
        r = cli_runner.invoke(["checkout", "version1"])
        assert r.exit_code == 0, r
        assert r_head() == ("HEAD", H.POINTS.HEAD_SHA)
        assert repo.head_is_detached
        with table_wc.session() as sess:
            assert H.last_change_time(sess) == "2019-06-20T14:28:33.000000Z"

        # checkout the remote branch
        r = cli_runner.invoke(["checkout", "myremote/main"])
        assert r.exit_code == 0, r
        assert r_head() == ("HEAD", H.POINTS.HEAD_SHA)
        assert repo.head_is_detached
        with table_wc.session() as sess:
            assert H.last_change_time(sess) == "2019-06-20T14:28:33.000000Z"


def test_checkout_branch(data_working_copy, cli_runner, tmp_path):
    with data_working_copy("points") as (repo_path, wc):
        # creating a new branch with existing name errors
        r = cli_runner.invoke(["checkout", "-b", "main"])
        assert r.exit_code == INVALID_ARGUMENT, r
        assert r.stderr.splitlines()[-1].endswith(
            "A branch named 'main' already exists."
        )

        subprocess.run(["git", "init", "--bare", str(tmp_path)], check=True)
        r = cli_runner.invoke(["remote", "add", "myremote", tmp_path])
        assert r.exit_code == 0, r

        r = cli_runner.invoke(["push", "--set-upstream", "myremote", "main"])
        assert r.exit_code == 0, r

        # new branch
        r = cli_runner.invoke(["checkout", "-b", "foo"])
        assert r.exit_code == 0, r

        repo = KartRepo(repo_path)
        assert repo.head.name == "refs/heads/foo"
        assert "foo" in repo.branches
        assert repo.head_commit.hex == H.POINTS.HEAD_SHA

        # make some changes
        with repo.working_copy.tabular.session() as sess:
            r = sess.execute(H.POINTS.INSERT, H.POINTS.RECORD)
            assert r.rowcount == 1

        r = cli_runner.invoke(["commit", "-m", "test1"])
        assert r.exit_code == 0, r

        assert repo.head_commit.hex != H.POINTS.HEAD_SHA

        r = cli_runner.invoke(["checkout", "main"])
        assert r.exit_code == 0, r

        assert repo.head.name == "refs/heads/main"
        assert repo.head_commit.hex == H.POINTS.HEAD_SHA

        # new branch from remote
        r = cli_runner.invoke(["checkout", "-b", "test99", "myremote/main"])
        assert r.exit_code == 0, r
        assert repo.head.name == "refs/heads/test99"
        assert "test99" in repo.branches
        assert repo.head_commit.hex == H.POINTS.HEAD_SHA
        branch = repo.branches["test99"]
        assert branch.upstream_name == "refs/remotes/myremote/main"


def test_switch_branch(data_working_copy, cli_runner, tmp_path):
    with data_working_copy("points") as (repo_path, wc):
        # creating a new branch with existing name errors
        r = cli_runner.invoke(["switch", "-c", "main"])
        assert r.exit_code == INVALID_ARGUMENT
        assert r.stderr.splitlines()[-1].endswith(
            "A branch named 'main' already exists."
        )

        subprocess.run(["git", "init", "--bare", str(tmp_path)], check=True)
        r = cli_runner.invoke(["remote", "add", "myremote", tmp_path])
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["push", "--set-upstream", "myremote", "main"])
        assert r.exit_code == 0, r.stderr

        # new branch
        r = cli_runner.invoke(["switch", "-c", "foo"])
        assert r.exit_code == 0, r.stderr

        repo = KartRepo(repo_path)
        table_wc = repo.working_copy.tabular
        assert repo.head.name == "refs/heads/foo"
        assert "foo" in repo.branches
        assert repo.head_commit.hex == H.POINTS.HEAD_SHA

        # make some changes
        with table_wc.session() as sess:
            r = sess.execute(H.POINTS.INSERT, H.POINTS.RECORD)
            assert r.rowcount == 1

            r = sess.execute(f"UPDATE {H.POINTS.LAYER} SET fid=30000 WHERE fid=3;")
            assert r.rowcount == 1

        r = cli_runner.invoke(["commit", "-m", "test1"])
        assert r.exit_code == 0, r

        new_commit = repo.head_commit.hex
        assert new_commit != H.POINTS.HEAD_SHA

        r = cli_runner.invoke(["switch", "main"])
        assert r.exit_code == 0, r

        with table_wc.session() as sess:
            assert H.row_count(sess, H.POINTS.LAYER) == H.POINTS.ROWCOUNT

        assert repo.head.name == "refs/heads/main"
        assert repo.head_commit.hex == H.POINTS.HEAD_SHA

        # make some changes
        with table_wc.session() as sess:
            r = sess.execute(H.POINTS.INSERT, H.POINTS.RECORD)
            assert r.rowcount == 1

            r = sess.execute(f"UPDATE {H.POINTS.LAYER} SET fid=40000 WHERE fid=4;")
            assert r.rowcount == 1

        r = cli_runner.invoke(["switch", "foo"])
        assert r.exit_code == UNCOMMITTED_CHANGES
        assert "Error: You have uncommitted changes in your working copy." in r.stderr

        r = cli_runner.invoke(["switch", "foo", "--discard-changes"])
        assert r.exit_code == 0, r.stderr

        with table_wc.session() as sess:
            assert H.row_count(sess, H.POINTS.LAYER) == H.POINTS.ROWCOUNT + 1

        assert repo.head.name == "refs/heads/foo"
        assert repo.head_commit.hex == new_commit

        # new branch from remote
        r = cli_runner.invoke(["switch", "-c", "test99", "myremote/main"])
        assert r.exit_code == 0, r.stderr
        assert repo.head.name == "refs/heads/test99"
        assert "test99" in repo.branches
        assert repo.head_commit.hex == H.POINTS.HEAD_SHA
        branch = repo.branches["test99"]
        assert branch.upstream_name == "refs/remotes/myremote/main"

        with table_wc.session() as sess:
            assert H.row_count(sess, H.POINTS.LAYER) == H.POINTS.ROWCOUNT


@pytest.mark.parametrize(
    "archive,layer",
    [
        pytest.param("points", H.POINTS.LAYER, id="points"),
        pytest.param("polygons", H.POLYGONS.LAYER, id="polygons"),
        pytest.param("table", H.TABLE.LAYER, id="table"),
    ],
)
@pytest.mark.parametrize(
    "via",
    [
        pytest.param("restore", id="via-restore"),
        pytest.param("checkout", id="via-checkout"),
        pytest.param("reset", id="via-reset"),
    ],
)
def test_working_copy_discard_changes(
    archive, layer, via, data_working_copy, cli_runner
):
    """
    Check that we reset any working-copy changes correctly before doing any new checkout

    We can do this via `kart restore` or `kart checkout --discard-changes HEAD`
    """
    if layer == H.POINTS.LAYER:
        pk_field = H.POINTS.LAYER_PK
        rec = H.POINTS.RECORD
        sql = H.POINTS.INSERT
        del_pk = 5
        upd_field = "t50_fid"
        upd_field_value = 888_888
        upd_pk_range = (10, 15)
        id_chg_pk = 20
    elif layer == H.POLYGONS.LAYER:
        pk_field = H.POLYGONS.LAYER_PK
        rec = H.POLYGONS.RECORD
        sql = H.POLYGONS.INSERT
        del_pk = 1_456_912
        upd_field = "survey_reference"
        upd_field_value = "test"
        upd_pk_range = (1_459_750, 1_460_312)
        id_chg_pk = 1_460_583
    elif layer == H.TABLE.LAYER:
        pk_field = H.TABLE.LAYER_PK
        rec = H.TABLE.RECORD
        sql = H.TABLE.INSERT
        del_pk = 5
        upd_field = "name"
        upd_field_value = "test"
        upd_pk_range = (10, 15)
        id_chg_pk = 20
    else:
        raise NotImplementedError(f"layer={layer}")

    with data_working_copy(archive, force_new=True) as (repo_path, wc_path):
        repo = KartRepo(repo_path)
        table_wc = repo.working_copy.tabular

        with table_wc.session() as sess:
            h_before = H.db_table_hash(sess, layer, pk_field)
            r = sess.execute(sql, rec)
            assert r.rowcount == 1

            r = sess.execute(f"DELETE FROM {layer} WHERE {pk_field} < {del_pk};")
            assert r.rowcount == 4
            r = sess.execute(
                f"UPDATE {layer} SET {upd_field} = :value WHERE {pk_field}>=:low AND {pk_field}<:high;",
                {
                    "value": upd_field_value,
                    "low": upd_pk_range[0],
                    "high": upd_pk_range[1],
                },
            )
            assert r.rowcount == 5
            r = sess.execute(
                f"UPDATE {layer} SET {pk_field}=:new_pk WHERE {pk_field}=:old_pk;",
                {"new_pk": 9998, "old_pk": id_chg_pk},
            )
            assert r.rowcount == 1

            change_count = sess.scalar("""SELECT COUNT(*) FROM "gpkg_kart_track";""")
            assert change_count == (1 + 4 + 5 + 2)

        if via == "restore":
            # using `kart restore`
            r = cli_runner.invoke(["restore"])
            assert r.exit_code == 0, r

        elif via == "reset":
            # using `kart reset --discard-changes`
            r = cli_runner.invoke(["reset", "--discard-changes"])
            assert r.exit_code == 0, r

        elif via == "checkout":
            # using `kart checkout HEAD --discard-changes`

            # kart checkout HEAD does nothing if you don't --discard-changes:
            r = cli_runner.invoke(["checkout", "HEAD"])
            assert r.exit_code == 0, r.stderr
            assert table_wc.tracking_changes_count() == (1 + 4 + 5 + 2)

            # do again with --discard-changes
            r = cli_runner.invoke(["checkout", "--discard-changes", "HEAD"])
            assert r.exit_code == 0, r.stderr
        else:
            raise NotImplementedError(f"via={via}")

        assert table_wc.tracking_changes_count() == 0

        with table_wc.session() as sess:
            h_after = H.db_table_hash(sess, layer, pk_field)
            if h_before != h_after:
                # Inserted row should disappear again:
                assert not sess.execute(
                    f"SELECT {pk_field} FROM {layer} WHERE {pk_field}=:pk;",
                    {"pk": rec[pk_field]},
                ).fetchone()
                # Deleted rows should be back:
                assert (
                    sess.scalar(
                        f"SELECT COUNT(*) FROM {layer} WHERE {pk_field} < :pk;",
                        {"pk": del_pk},
                    )
                    == 4
                )
                # Updated row should be back to old value.
                assert (
                    sess.scalar(
                        f"SELECT COUNT(*) FROM {layer} WHERE {upd_field} = :value;",
                        {"value": upd_field_value},
                    )
                    == 0
                )
                # Renamed row should be back where it was
                assert not sess.execute(
                    f"SELECT {pk_field} FROM {layer} WHERE {pk_field} = 9998;"
                ).fetchone()
                assert sess.execute(
                    f"SELECT {pk_field} FROM {layer} WHERE {pk_field} = :pk;",
                    {"pk": id_chg_pk},
                ).fetchone()

            assert h_before == h_after


def test_switch_with_meta_items(data_working_copy, cli_runner):
    with data_working_copy("points") as (repo_path, wc_path):
        table_wc = KartRepo(repo_path).working_copy.tabular
        with table_wc.session() as sess:
            sess.execute(
                """UPDATE gpkg_contents SET identifier = 'new identifier', description='new description'"""
            )

        r = cli_runner.invoke(["commit", "-m", "change identifier and description"])
        assert r.exit_code == 0, r.stderr
        r = cli_runner.invoke(["checkout", "HEAD^"])
        assert r.exit_code == 0, r.stderr

        with table_wc.session() as sess:
            r = sess.execute("""SELECT identifier, description FROM gpkg_contents""")
            identifier, description = r.fetchone()
            assert identifier == "NZ Pa Points (Topo, 1:50k)"
            assert description.startswith("Defensive earthworks")

        r = cli_runner.invoke(["checkout", "main"])
        assert r.exit_code == 0, r.stderr

        with table_wc.session() as sess:
            r = sess.execute("""SELECT identifier, description FROM gpkg_contents""")
            identifier, description = r.fetchone()
            assert identifier == "new identifier"
            assert description == "new description"


def test_switch_with_trivial_schema_change(data_working_copy, cli_runner):
    # Column renames are one of the only schema changes we can do without having to recreate the whole table.
    with data_working_copy("points") as (repo_path, wc_path):
        table_wc = KartRepo(repo_path).working_copy.tabular
        with table_wc.session() as sess:
            sess.execute(
                f"""ALTER TABLE "{H.POINTS.LAYER}" RENAME "name_ascii" TO "name_latin1";"""
            )

        r = cli_runner.invoke(["commit", "-m", "change schema"])
        assert r.exit_code == 0, r.stderr
        r = cli_runner.invoke(["checkout", "HEAD^"])
        assert r.exit_code == 0, r.stderr
        with table_wc.session() as sess:
            name = sess.scalar(
                f"""SELECT name FROM pragma_table_info('{H.POINTS.LAYER}') WHERE cid = 3;"""
            )
            assert name == "name_ascii"

        r = cli_runner.invoke(["checkout", "main"])
        assert r.exit_code == 0, r.stderr
        with table_wc.session() as sess:
            name = sess.scalar(
                f"""SELECT name FROM pragma_table_info('{H.POINTS.LAYER}') WHERE cid = 3;"""
            )
            assert name == "name_latin1"


def test_switch_with_schema_change(data_working_copy, cli_runner):
    with data_working_copy("polygons") as (repo_path, wc_path):
        table_wc = KartRepo(repo_path).working_copy.tabular
        with table_wc.session() as sess:
            sess.execute(
                f"""ALTER TABLE "{H.POLYGONS.LAYER}" ADD COLUMN "colour" TEXT;"""
            )
        r = cli_runner.invoke(["commit", "-m", "change schema"])
        assert r.exit_code == 0, r.stderr
        r = cli_runner.invoke(["checkout", "HEAD^"])
        assert r.exit_code == 0, r.stderr
        with table_wc.session() as sess:
            r = sess.execute(
                f"""SELECT name, type FROM pragma_table_info('{H.POLYGONS.LAYER}');"""
            )
            result = list(r)
            assert result == [
                ("id", "INTEGER"),
                ("geom", "MULTIPOLYGON"),
                ("date_adjusted", "DATETIME"),
                ("survey_reference", "TEXT(50)"),
                ("adjusted_nodes", "MEDIUMINT"),
            ]

        r = cli_runner.invoke(["checkout", "main"])
        assert r.exit_code == 0, r.stderr
        with table_wc.session() as sess:
            r = sess.execute(
                f"""SELECT name, type FROM pragma_table_info('{H.POLYGONS.LAYER}');"""
            )
            result = list(r)
            assert result == [
                ("id", "INTEGER"),
                ("geom", "MULTIPOLYGON"),
                ("date_adjusted", "DATETIME"),
                ("survey_reference", "TEXT(50)"),
                ("adjusted_nodes", "MEDIUMINT"),
                ("colour", "TEXT"),
            ]


def test_switch_pre_import_post_import(
    data_working_copy, data_archive_readonly, cli_runner
):
    with data_archive_readonly("gpkg-au-census") as data:
        with data_working_copy("polygons") as (repo_path, wc_path):
            table_wc = KartRepo(repo_path).working_copy.tabular

            r = cli_runner.invoke(
                [
                    "import",
                    data / "census2016_sdhca_ot_short.gpkg",
                    "census2016_sdhca_ot_ced_short",
                ]
            )
            assert r.exit_code == 0, r.stderr
            r = cli_runner.invoke(["checkout", "HEAD^"])
            assert r.exit_code == 0, r.stderr

            with table_wc.session() as sess:
                count = sess.scalar(
                    f"""SELECT COUNT(name) FROM sqlite_master where type='table' AND name='census2016_sdhca_ot_ced_short';"""
                )
                assert count == 0

            r = cli_runner.invoke(["checkout", "main"])
            assert r.exit_code == 0, r.stderr

            with table_wc.session() as sess:
                count = sess.scalar(
                    f"""SELECT COUNT(name) FROM sqlite_master where type='table' AND name='census2016_sdhca_ot_ced_short';"""
                )
                assert count == 1


def test_switch_xml_metadata_added(data_working_copy, cli_runner):
    with data_working_copy("table") as (repo_path, wc_path):
        table_wc = KartRepo(repo_path).working_copy.tabular
        with table_wc.session() as sess:
            # Update title
            sess.execute(
                """
                UPDATE gpkg_contents SET identifier='new title' WHERE table_name = 'countiestbl';
                """
            )
            # Add metadata.
            sess.execute(
                """
                INSERT INTO gpkg_metadata (id, md_scope, md_standard_uri, mime_type, metadata)
                VALUES (1, "dataset", "http://www.isotc211.org/2005/gmd", "text/xml", "<test metadata>");
                """
            )
            sess.execute(
                """
                INSERT INTO gpkg_metadata_reference (reference_scope, table_name, md_file_id)
                VALUES ("table", "countiestbl", 1);
                """
            )

        r = cli_runner.invoke(["commit", "-m", "Update title"])
        assert r.exit_code == 0, r.stderr
        r = cli_runner.invoke(["checkout", "HEAD^"])
        assert r.exit_code == 0, r.stderr

        with table_wc.session() as sess:
            xml_metadata = sess.execute(
                """
                SELECT m.metadata
                FROM gpkg_metadata m JOIN gpkg_metadata_reference r
                ON m.id = r.md_file_id
                WHERE r.table_name = 'countiestbl'
                """
            ).scalar()
            # XML metadata isn't committed, stays the same.
            assert xml_metadata == "<test metadata>"

        r = cli_runner.invoke(["checkout", "main"])
        assert r.exit_code == 0, r.stderr

        with table_wc.session() as sess:
            xml_metadata = sess.execute(
                """
                SELECT m.metadata
                FROM gpkg_metadata m JOIN gpkg_metadata_reference r
                ON m.id = r.md_file_id
                WHERE r.table_name = 'countiestbl'
                """
            ).scalar()
            # XML metadata isn't committed, stays the same.
            assert xml_metadata == "<test metadata>"


def test_geopackage_locking_edit(data_working_copy, cli_runner, monkeypatch):
    with data_working_copy("points") as (repo_path, wc_path):
        table_wc = KartRepo(repo_path).working_copy.tabular

        is_checked = False
        orig_func = TableWorkingCopy._write_features_from_dataset

        def _wrap(*args, **kwargs):
            nonlocal is_checked
            if not is_checked:
                with pytest.raises(
                    sqlalchemy.exc.OperationalError, match=r"database is locked"
                ):
                    with table_wc.session() as sess:
                        sess.execute("UPDATE gpkg_contents SET table_name=table_name;")
                is_checked = True

            return orig_func(*args, **kwargs)

        monkeypatch.setattr(TableWorkingCopy, "_write_features_from_dataset", _wrap)

        r = cli_runner.invoke(["checkout", H.POINTS.HEAD1_SHA])
        assert r.exit_code == 0, r
        assert is_checked

        with table_wc.session() as sess:
            assert H.last_change_time(sess) == "2019-06-11T11:03:58.000000Z"


def test_create_workingcopy(data_working_copy, cli_runner, tmp_path):
    with data_working_copy("points") as (repo_path, _):
        repo = KartRepo(repo_path)

        r = cli_runner.invoke(["create-workingcopy", ".", "--delete-existing"])
        assert r.exit_code == INVALID_ARGUMENT, r.stderr

        # relative path 1
        new_thingz = Path("new-thingz.gpkg")
        assert not new_thingz.exists()
        r = cli_runner.invoke(
            ["create-workingcopy", str(new_thingz), "--delete-existing"]
        )
        assert r.exit_code == 0, r.stderr
        assert new_thingz.exists()
        assert repo.config["kart.workingcopy.location"] == str(new_thingz)

        r = cli_runner.invoke(
            ["create-workingcopy", str(new_thingz), "--delete-existing"]
        )
        assert r.exit_code == 0, r.stderr

        # relative path 2
        other_thingz = Path("other-thingz.gpkg")
        assert not other_thingz.exists()
        r = cli_runner.invoke(
            ["create-workingcopy", "../points/other-thingz.gpkg", "--delete-existing"]
        )
        assert r.exit_code == 0, r.stderr
        assert not new_thingz.exists()
        assert other_thingz.exists()
        assert repo.config["kart.workingcopy.location"] == str(other_thingz)

        # abs path
        abs_thingz = tmp_path / "abs_thingz.gpkg"
        assert not abs_thingz.exists()
        r = cli_runner.invoke(
            ["create-workingcopy", str(abs_thingz), "--delete-existing"]
        )
        assert r.exit_code == 0, r.stderr
        assert not other_thingz.exists()
        assert abs_thingz.exists()

        assert repo.config["kart.workingcopy.location"] == str(abs_thingz)


@pytest.mark.parametrize(
    "source",
    [
        pytest.param([], id="head"),
        pytest.param(["-s", H.POINTS.HEAD_SHA], id="prev"),
    ],
)
@pytest.mark.parametrize(
    "filters",
    [
        pytest.param([], id="all"),
        pytest.param(["bob"], id="exclude"),
    ],
)
def test_restore(source, filters, data_working_copy, cli_runner):
    with data_working_copy("points", force_new=True) as (repo_path, wc_path):
        layer = H.POINTS.LAYER
        pk_field = H.POINTS.LAYER_PK
        rec = H.POINTS.RECORD
        sql = H.POINTS.INSERT
        del_pk = 5
        upd_field = "t50_fid"
        upd_field_value = 888_888
        upd_pk_range = (10, 15)
        id_chg_pk = 20

        repo = KartRepo(repo_path)
        table_wc = repo.working_copy.tabular

        with table_wc.session() as sess:
            r = sess.execute(f"UPDATE {H.POINTS.LAYER} SET fid=30000 WHERE fid=300;")
            assert r.rowcount == 1

        r = cli_runner.invoke(["commit", "-m", "test1"])
        assert r.exit_code == 0, r.stderr

        new_commit = repo.head_commit.hex
        assert new_commit != H.POINTS.HEAD_SHA
        print(f"Original commit={H.POINTS.HEAD_SHA} New commit={new_commit}")
        new_commit_tree = repo[new_commit].tree.hex

        with table_wc.session() as sess:
            r = sess.execute(sql, rec)
            assert r.rowcount == 1

            r = sess.execute(f"DELETE FROM {layer} WHERE {pk_field} < {del_pk};")
            assert r.rowcount == 4
            r = sess.execute(
                f"UPDATE {layer} SET {upd_field} = :value WHERE {pk_field}>=:low AND {pk_field}<:high;",
                {
                    "value": upd_field_value,
                    "low": upd_pk_range[0],
                    "high": upd_pk_range[1],
                },
            )
            assert r.rowcount == 5
            r = sess.execute(
                f"UPDATE {layer} SET {pk_field}=:new_pk WHERE {pk_field}=:old_pk;",
                {"new_pk": 9998, "old_pk": id_chg_pk},
            )
            assert r.rowcount == 1

            changes_pre = [
                r[0]
                for r in sess.execute(
                    'SELECT pk FROM "gpkg_kart_track" ORDER BY CAST(pk AS INTEGER);'
                )
            ]
            # gpkg_kart_track stores pk as strings
            assert changes_pre == [
                "1",
                "2",
                "3",
                "4",
                "10",
                "11",
                "12",
                "13",
                "14",
                "20",
                "9998",
                "9999",
            ]

        assert table_wc.get_tree_id() == new_commit_tree

        # using `kart restore
        r = cli_runner.invoke(["restore"] + source + filters)
        assert r.exit_code == 0, r

        # A restore doesn't change which tree the WC is based on.
        assert table_wc.get_tree_id() == new_commit_tree

        with table_wc.session() as sess:
            changes_post = [
                r[0]
                for r in sess.execute(
                    'SELECT pk FROM "gpkg_kart_track" ORDER BY CAST(pk AS INTEGER);'
                )
            ]

            if filters:
                # we restore'd paths other than our test dataset, so all the changes should still be there
                assert changes_post == changes_pre
                return

            if source:
                assert changes_post == ["300", "30000"]

                # Feature rename that was committed has been restored from previous commit:
                assert not sess.execute(
                    f"SELECT {pk_field} FROM {layer} WHERE {pk_field} = 30000;"
                ).fetchone()
                assert sess.execute(
                    f"SELECT {pk_field} FROM {layer} WHERE {pk_field} = 300;"
                ).fetchone()
                return

            assert changes_post == []

            r = sess.execute(
                f"SELECT {pk_field} FROM {layer} WHERE {pk_field}=:pk;",
                {"pk": rec[pk_field]},
            )
            assert (
                not r.fetchone()
            ), f"Newly inserted row is still there ({pk_field}={rec[pk_field]})"

            count = sess.scalar(
                f"SELECT COUNT(*) FROM {layer} WHERE {pk_field} < :pk;", {"pk": del_pk}
            )
            assert count == 4, f"Deleted rows {pk_field}<{del_pk} still missing"

            count = sess.scalar(
                f"SELECT COUNT(*) FROM {layer} WHERE {upd_field} = :value;",
                {"value": upd_field_value},
            )
            assert count == 0, "Updated rows not reset"

            # Feature rename that was not yet committed has been restored from HEAD:
            assert not sess.execute(
                f"SELECT {pk_field} FROM {layer} WHERE {pk_field} = 9998;"
            ).fetchone()
            assert sess.execute(
                f"SELECT {pk_field} FROM {layer} WHERE {pk_field} = :pk;",
                {"pk": id_chg_pk},
            ).fetchone()

            # Feature rename that was committed is still present:
            assert sess.execute(
                f"SELECT {pk_field} FROM {layer} WHERE {pk_field} = 30000;"
            ).fetchone()
            assert not sess.execute(
                f"SELECT {pk_field} FROM {layer} WHERE {pk_field} = 300;"
            ).fetchone()


def test_delete_branch(data_working_copy, cli_runner):
    with data_working_copy("points") as (repo_path, wc):
        # prevent deleting the current branch
        r = cli_runner.invoke(["branch", "-d", "main"])
        assert r.exit_code == INVALID_OPERATION, r
        assert "Cannot delete" in r.stderr

        r = cli_runner.invoke(["checkout", "-b", "test"])
        assert r.exit_code == 0, r

        r = cli_runner.invoke(["branch", "-d", "test"])
        assert r.exit_code == INVALID_OPERATION, r

        r = cli_runner.invoke(["checkout", "main"])
        assert r.exit_code == 0, r

        r = cli_runner.invoke(["branch", "-d", "test"])
        assert r.exit_code == 0, r


def test_auto_increment_pk(data_working_copy):
    with data_working_copy("polygons") as (repo_path, wc):
        repo = KartRepo(repo_path)
        with repo.working_copy.tabular.session() as sess:
            count = sess.scalar(
                f"SELECT COUNT(*) FROM {H.POLYGONS.LAYER} WHERE id = {H.POLYGONS.NEXT_UNASSIGNED_PK};"
            )
            assert count == 0
            sess.execute(f"INSERT INTO {H.POLYGONS.LAYER} (geom) VALUES (NULL);")
            count = sess.scalar(
                f"SELECT COUNT(*) FROM {H.POLYGONS.LAYER} WHERE id = {H.POLYGONS.NEXT_UNASSIGNED_PK};"
            )
            assert count == 1


def test_approximated_types():
    assert KartAdapter_GPKG.APPROXIMATED_TYPES == compute_approximated_types(
        KartAdapter_GPKG.V2_TYPE_TO_SQL_TYPE, KartAdapter_GPKG.SQL_TYPE_TO_V2_TYPE
    )


def test_types_roundtrip(data_working_copy, cli_runner):
    # If type-approximation roundtrip code isn't working,
    # we would get spurious diffs on types that GPKG doesn't support.
    with data_working_copy("types") as (repo_path, wc_path):
        r = cli_runner.invoke(["diff", "--exit-code"])
        assert r.exit_code == 0, r.stdout


def test_values_roundtrip(data_working_copy, cli_runner):
    # If values roundtripping code isn't working for certain types,
    # we could get spurious diffs on those values.
    with data_working_copy("types") as (repo_path, wc_path):
        # Geopackage can store any type of numerics it likes since it approximates them as text.
        r = cli_runner.invoke(["checkout", "unconstrained-numerics"])
        repo = KartRepo(repo_path)
        with repo.working_copy.tabular.session() as sess:
            # We don't diff values unless they're marked as dirty in the WC - move the row to make it dirty.
            sess.execute('UPDATE manytypes SET "PK"="PK" + 1000;')
            sess.execute('UPDATE manytypes SET "PK"="PK" - 1000;')
        r = cli_runner.invoke(["diff", "--exit-code"])
        assert r.exit_code == 0, r.stdout


def test_empty_geometry_roundtrip(data_working_copy, cli_runner):
    with data_working_copy("empty-geometry") as (repo_path, wc_path):
        repo = KartRepo(repo_path)
        with repo.working_copy.tabular.session() as sess:
            # We don't diff values unless they're marked as dirty in the WC - move the row to make it dirty.
            sess.execute('UPDATE point_test SET "PK"="PK" + 1000;')
            sess.execute('UPDATE point_test SET "PK"="PK" - 1000;')
            sess.execute('UPDATE polygon_test SET "PK"="PK" + 1000;')
            sess.execute('UPDATE polygon_test SET "PK"="PK" - 1000;')
        r = cli_runner.invoke(["diff", "--exit-code"])
        assert r.exit_code == 0, r.stdout


def test_pk_second_roundtrip(data_working_copy, cli_runner):
    # Make sure we can handle the PK being second without creating an auto_int_pk column or showing spurious diffs.
    with data_working_copy("pk-second") as (repo_path, wc_path):
        repo = KartRepo(repo_path)
        with repo.working_copy.tabular.session() as sess:
            r = sess.execute(
                "SELECT name FROM pragma_table_info('nz_waca_adjustments');"
            )
            col_names = [row[0] for row in r]
            assert col_names == [
                "geom",
                "id",
                "date_adjusted",
                "survey_reference",
                "adjusted_nodes",
            ]

        r = cli_runner.invoke(["diff", "--exit-code"])
        assert r.exit_code == 0, r.stdout


def _edit_string_pk_polygons(conn):
    H = pytest.helpers.helpers()
    layer = H.POLYGONS.LAYER
    insert_record = H.POLYGONS.RECORD.copy()
    insert_record["id"] = "test1234"
    r = conn.execute(H.POLYGONS.INSERT, insert_record)
    assert r.rowcount == 1
    r = conn.execute(f"UPDATE {layer} SET id='POLY9998' WHERE id='POLY1424927';")
    assert r.rowcount == 1
    r = conn.execute(
        f"UPDATE {layer} SET survey_reference='test' WHERE id='POLY1443053';"
    )
    assert r.rowcount == 1
    r = conn.execute(
        f"DELETE FROM {layer} WHERE id IN ('POLY1452332', 'POLY1456853', 'POLY1456912', 'POLY1457297', 'POLY1457355');"
    )
    assert r.rowcount == 5
    pk_del = 1452332
    return pk_del


def test_edit_string_pks(data_working_copy, cli_runner):
    with data_working_copy("string-pks") as (repo_path, wc):
        repo = KartRepo(repo_path)
        with repo.working_copy.tabular.session() as sess:
            _edit_string_pk_polygons(sess)

        r = cli_runner.invoke(["status", "--output-format=json"])
        assert r.exit_code == 0, r
        changes = json.loads(r.stdout)["kart.status/v2"]["workingCopy"]["changes"]
        assert changes == {
            "nz_waca_adjustments": {
                "feature": {"inserts": 1, "updates": 2, "deletes": 5}
            }
        }
        r = cli_runner.invoke(["diff", "--exit-code"])
        assert r.exit_code == 1, r.stderr

        r = cli_runner.invoke(["commit", "-m", "test"])
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["diff", "--exit-code"])
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["show"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines()[12:16] == [
            "--- nz_waca_adjustments:feature:POLY1443053",
            "+++ nz_waca_adjustments:feature:POLY1443053",
            "-                         survey_reference = ␀",
            "+                         survey_reference = test",
        ]


def test_reset_transaction(data_working_copy, cli_runner, edit_points):
    with data_working_copy("points") as (repo_path, wc_path):
        repo = KartRepo(repo_path)
        table_wc = repo.working_copy.tabular
        with table_wc.session() as sess:
            edit_points(sess)

        r = cli_runner.invoke(["status", "--output-format=json"])
        assert r.exit_code == 0, r
        changes = json.loads(r.stdout)["kart.status/v2"]["workingCopy"]["changes"]
        assert changes == {
            H.POINTS.LAYER: {"feature": {"inserts": 1, "updates": 2, "deletes": 5}}
        }

        with table_wc.session() as sess:
            # This modification makes the gpkg_kart_state table work like normal for reading,
            # but writing to it will fail due to the CHECK.
            sess.execute(
                """ALTER TABLE "gpkg_kart_state" RENAME TO "gpkg_kart_state_backup";"""
            )
            value = sess.scalar("SELECT value FROM gpkg_kart_state_backup;")
            sess.execute(
                f"""
                CREATE TABLE "gpkg_kart_state"
                    ("table_name" TEXT NOT NULL, "key" TEXT NOT NULL, "value" TEXT NULL CHECK("value" = '{value}'));
                """
            )
            sess.execute(
                """INSERT INTO "gpkg_kart_state" SELECT * FROM "gpkg_kart_state_backup";"""
            )

        # This should fail and so the entire transaction should be rolled back.
        # Therefore, the GPKG should remain unchanged with 6 uncommitted changes -
        # even though the failed write to gpkg_kart_state happens after the changes
        # are discarded and after working copy is reset to the new commit - all of
        # that will be rolled back.
        with pytest.raises(sqlalchemy.exc.IntegrityError):
            r = cli_runner.invoke(["checkout", "HEAD^", "--discard-changes"])

        with table_wc.session() as sess:
            sess.execute("DROP TABLE IF EXISTS gpkg_kart_state;")
            sess.execute(
                """ALTER TABLE "gpkg_kart_state_backup" RENAME TO "gpkg_kart_state";"""
            )

        # FIXME - although we roll-back the WC transaction on failure, we don't roll back any other WC parts
        # or the change to HEAD, so the Kart repo still ends up in an inconsistent state. Fix that now:
        repo.set_head("refs/heads/main")

        r = cli_runner.invoke(["status", "--output-format=json"])
        assert r.exit_code == 0, r.stderr
        changes = json.loads(r.stdout)["kart.status/v2"]["workingCopy"]["changes"]
        assert changes == {
            H.POINTS.LAYER: {"feature": {"inserts": 1, "updates": 2, "deletes": 5}}
        }


def test_meta_updates(data_working_copy, cli_runner):
    with data_working_copy("meta-updates") as (repo_path, wc_path):
        # These commits have minor schema changes.
        # We try to handle minor schema changes by using ALTER TABLE statements, instead
        # of dropping and recreating the whole table. Make sure those statements are working:

        r = cli_runner.invoke(["checkout", "main~3"])
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["checkout", "main~2"])
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["checkout", "main~1"])
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["checkout", "main"])
        assert r.exit_code == 0, r.stderr


def test_checkout_custom_crs(data_working_copy, cli_runner, dodgy_restore):
    with data_working_copy("custom_crs") as (repo_path, wc_path):
        repo = KartRepo(repo_path)

        # main has a custom CRS at HEAD. A diff here would mean we are not roundtripping it properly:
        r = cli_runner.invoke(["diff", "--exit-code"])
        assert r.exit_code == 0, r.stderr

        table_wc = repo.working_copy.tabular
        with table_wc.session() as sess:
            srs_id = sess.scalar(
                "SELECT srs_id FROM gpkg_contents WHERE table_name = :table_name",
                {"table_name": H.POINTS.LAYER},
            )
            assert srs_id == 100002

        # We should be able to switch to the previous revision, which has a different (standard) CRS.
        r = cli_runner.invoke(["checkout", "epsg-4326"])
        assert r.exit_code == 0, r.stderr

        with table_wc.session() as sess:
            srs_id = sess.scalar(
                "SELECT srs_id FROM gpkg_contents WHERE table_name = :table_name",
                {"table_name": H.POINTS.LAYER},
            )
            assert srs_id == 4326

        # Restore the contents of custom-crs to the WC so we can make sure WC diff is working:
        dodgy_restore(repo, "custom-crs")

        r = cli_runner.invoke(["diff"])
        assert r.stdout.splitlines() == [
            "--- nz_pa_points_topo_150k:meta:crs/EPSG:4326.wkt",
            '- GEOGCS["WGS 84",',
            '-     DATUM["WGS_1984",',
            '-         SPHEROID["WGS 84", 6378137, 298.257223563,',
            '-             AUTHORITY["EPSG", "7030"]],',
            '-         AUTHORITY["EPSG", "6326"]],',
            '-     PRIMEM["Greenwich", 0,',
            '-         AUTHORITY["EPSG", "8901"]],',
            '-     UNIT["degree", 0.0174532925199433,',
            '-         AUTHORITY["EPSG", "9122"]],',
            '-     AUTHORITY["EPSG", "4326"]]',
            "- ",
            "+++ nz_pa_points_topo_150k:meta:crs/koordinates.com:100002.wkt",
            '+ PROJCS["NAD83 / Austin",',
            '+     GEOGCS["NAD83",',
            '+         DATUM["North_American_Datum_1983",',
            '+             SPHEROID["GRS 1980", 6378137.0, 298.257222101],',
            "+             TOWGS84[0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]],",
            '+         PRIMEM["Greenwich", 0.0],',
            '+         UNIT["degree", 0.017453292519943295],',
            '+         AXIS["Lon", EAST],',
            '+         AXIS["Lat", NORTH]],',
            '+     PROJECTION["Lambert_Conformal_Conic_2SP"],',
            '+     PARAMETER["central_meridian", -100.333333333333],',
            '+     PARAMETER["latitude_of_origin", 29.6666666666667],',
            '+     PARAMETER["standard_parallel_1", 31.883333333333297],',
            '+     PARAMETER["false_easting", 2296583.333333],',
            '+     PARAMETER["false_northing", 9842500.0],',
            '+     PARAMETER["standard_parallel_2", 30.1166666666667],',
            '+     UNIT["m", 1.0],',
            '+     AXIS["x", EAST],',
            '+     AXIS["y", NORTH],',
            '+     AUTHORITY["koordinates.com", "100002"]]',
            "+ ",
            "--- nz_pa_points_topo_150k:meta:schema.json",
            "+++ nz_pa_points_topo_150k:meta:schema.json",
            "  [",
            "    {",
            '      "id": "e97b4015-2765-3a33-b174-2ece5c33343b",',
            '      "name": "fid",',
            '      "dataType": "integer",',
            '      "primaryKeyIndex": 0,',
            '      "size": 64',
            "    },",
            "    {",
            '      "id": "f488ae9b-6e15-1fe3-0bda-e0d5d38ea69e",',
            '      "name": "geom",',
            '      "dataType": "geometry",',
            '      "geometryType": "POINT",',
            '-     "geometryCRS": "EPSG:4326",',
            '+     "geometryCRS": "koordinates.com:100002",',
            "    },",
            "    {",
            '      "id": "4a1c7a86-c425-ea77-7f1a-d74321a10edc",',
            '      "name": "t50_fid",',
            '      "dataType": "integer",',
            '      "size": 32',
            "    },",
            "    {",
            '      "id": "d2a62351-a66d-bde2-ce3e-356fec9641e9",',
            '      "name": "name_ascii",',
            '      "dataType": "text",',
            '      "length": 75',
            "    },",
            "    {",
            '      "id": "c3389414-a511-5385-7dcd-891c4ead1663",',
            '      "name": "macronated",',
            '      "dataType": "text",',
            '      "length": 1',
            "    },",
            "    {",
            '      "id": "45b00eaa-5700-662d-8a21-9614e40c437b",',
            '      "name": "name",',
            '      "dataType": "text",',
            '      "length": 75',
            "    },",
            "  ]",
        ]


def test_checkout_with_no_crs(
    data_working_copy,
    tmp_path,
    cli_runner,
):
    with data_working_copy("points-no-crs") as (repo_dir, wc):
        r = cli_runner.invoke(["diff", "--exit-code"])
        assert r.exit_code == 0, r.stderr


def test_working_copy_progress_bar(cli_runner, data_working_copy, monkeypatch):
    with data_working_copy("points") as (repo_path, wc_path):
        r = cli_runner.invoke(["create-workingcopy", "--delete-existing"])
        assert r.exit_code == 0, r.stderr
        progress_output = r.stderr.splitlines()
        assert progress_output == [
            "Writing features for dataset 1 of 1: nz_pa_points_topo_150k"
        ]

        # Since stderr is not atty during testing, we have to force progress to show using KART_SHOW_PROGRESS.
        monkeypatch.setenv("KART_SHOW_PROGRESS", "1")
        r = cli_runner.invoke(["create-workingcopy", "--delete-existing"])
        assert r.exit_code == 0, r.stderr
        progress_output = r.stderr.splitlines()
        assert (
            progress_output[0]
            == "Writing features for dataset 1 of 1: nz_pa_points_topo_150k"
        )
        assert re.fullmatch(
            r"nz_pa_points_topo_150k: 100%\|█+\| 2143/2143 \[[0-9:<]+, [0-9\.]+F/s\]",
            progress_output[-1],
        )


def test_global_mapper_compatibility(data_working_copy):
    # See https://github.com/koordinates/kart/issues/899
    with data_working_copy("points") as (repo_path, wc_path):
        repo = KartRepo(repo_path)
        table_wc = repo.working_copy.tabular
        with table_wc.session() as sess:
            r = sess.execute("SELECT sql FROM sqlite_schema;")
            schemas = [row[0] for row in r if row[0]]
            # Global Mapper does understand this SQL snippet: VALUES (...) and we expect some in the triggers
            assert any(re.search(r"VALUES\s*\([^();]*\)", s) for s in schemas)
            # But it does not understand this SQL snippet: VALUES (...), (...)
            # Make sure that doesn't occur in our GPKG working copy.
            assert all(not re.search(r"VALUES\s*\([^();]*\)\s*,", s) for s in schemas)
