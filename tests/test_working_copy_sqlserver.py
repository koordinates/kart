import pytest

import pygit2
from sqlalchemy.exc import IntegrityError

from sno.exceptions import NotFound
from sno.repo import SnoRepo
from sno.sqlalchemy.create_engine import get_odbc_drivers, get_sqlserver_driver
from sno.working_copy import sqlserver_adapter
from sno.working_copy.base import WorkingCopyStatus
from sno.working_copy.db_server import DatabaseServer_WorkingCopy
from test_working_copy import compute_approximated_types


H = pytest.helpers.helpers()


def test_no_odbc():
    # if unixODBC is installed or we're on Windows we can't test the not-installed message
    try:
        import pyodbc
    except ImportError:
        pass
    else:
        pytest.skip("ODBC is installed")

    with pytest.raises(
        NotFound, match=r"^ODBC support for SQL Server is required but was not found."
    ) as e:
        get_odbc_drivers()


def test_odbc_drivers():
    # if unixODBC isn't installed we can't test this
    # use a try/except so we get a better message than via pytest.importorskip
    try:
        import pyodbc
    except ImportError:
        pytest.skip("Can't import pyodbc — unixODBC likely isn't installed.")

    num_drivers = len(get_odbc_drivers())
    # Eventually we should assert that we have useful drivers - eg MSSQL.
    # But for now, we are asserting that we were able to load pyodbc and it seems to be working.
    assert num_drivers >= 0
    print(f"Found {num_drivers} ODBC drivers")


@pytest.mark.xfail(
    reason="MSSQL driver is not included in the build - SQL Server tests will fail unless it is installed manually."
)
def test_sqlserver_driver():
    assert get_sqlserver_driver() is not None


# All of the following tests will also fail unless a MSSQL driver has been installed manually.
# However, they are not marked as xfail, since they do not run unless SNO_SQLSERVER_URL is set
# (the tests require a running SQL Server instance). This URL should not be set unless the driver
# has also been installed, otherwise the tests will all fail.


@pytest.mark.parametrize(
    "existing_schema",
    [
        pytest.param(True, id="existing-schema"),
        pytest.param(False, id="brand-new-schema"),
    ],
)
@pytest.mark.parametrize(
    "archive,table,commit_sha",
    [
        pytest.param("points", H.POINTS.LAYER, H.POINTS.HEAD_SHA, id="points"),
        pytest.param("polygons", H.POLYGONS.LAYER, H.POLYGONS.HEAD_SHA, id="polygons"),
        pytest.param("table", H.TABLE.LAYER, H.TABLE.HEAD_SHA, id="table"),
    ],
)
def test_checkout_workingcopy(
    archive,
    table,
    commit_sha,
    existing_schema,
    data_archive,
    cli_runner,
    new_sqlserver_db_schema,
):
    """ Checkout a working copy """
    with data_archive(archive) as repo_path:
        repo = SnoRepo(repo_path)
        H.clear_working_copy()

        with new_sqlserver_db_schema(create=existing_schema) as (
            sqlserver_url,
            sqlserver_schema,
        ):
            r = cli_runner.invoke(["create-workingcopy", sqlserver_url])
            assert r.exit_code == 0, r.stderr
            assert (
                r.stdout.splitlines()[-1]
                == f"Creating working copy at {DatabaseServer_WorkingCopy.strip_password(sqlserver_url)} ..."
            )

            r = cli_runner.invoke(["status"])
            assert r.exit_code == 0, r.stderr
            assert r.stdout.splitlines() == [
                "On branch main",
                "",
                "Nothing to commit, working copy clean",
            ]

            wc = repo.working_copy
            assert wc.status() & WorkingCopyStatus.INITIALISED
            assert wc.status() & WorkingCopyStatus.HAS_DATA

            head_tree_id = repo.head_tree.hex
            assert wc.assert_db_tree_match(head_tree_id)


@pytest.mark.parametrize(
    "existing_schema",
    [
        pytest.param(True, id="existing-schema"),
        pytest.param(False, id="brand-new-schema"),
    ],
)
def test_init_import(
    existing_schema,
    new_sqlserver_db_schema,
    data_archive,
    tmp_path,
    cli_runner,
):
    """ Import the GeoPackage (eg. `kx-foo-layer.gpkg`) into a Kart repository. """
    repo_path = tmp_path / "repo"
    repo_path.mkdir()

    with data_archive("gpkg-points") as data:
        with new_sqlserver_db_schema(create=existing_schema) as (
            sqlserver_url,
            sqlserver_schema,
        ):
            r = cli_runner.invoke(
                [
                    "init",
                    "--import",
                    f"gpkg:{data / 'nz-pa-points-topo-150k.gpkg'}",
                    str(repo_path),
                    f"--workingcopy-path={sqlserver_url}",
                ]
            )
            assert r.exit_code == 0, r.stderr
            assert (repo_path / ".kart" / "HEAD").exists()

            repo = SnoRepo(repo_path)
            wc = repo.working_copy
            assert wc.status() & WorkingCopyStatus.INITIALISED
            assert wc.status() & WorkingCopyStatus.HAS_DATA

            assert wc.location == sqlserver_url


@pytest.mark.parametrize(
    "archive,table,commit_sha",
    [
        pytest.param("points", H.POINTS.LAYER, H.POINTS.HEAD_SHA, id="points"),
        pytest.param("polygons", H.POLYGONS.LAYER, H.POLYGONS.HEAD_SHA, id="polygons"),
        pytest.param("table", H.TABLE.LAYER, H.TABLE.HEAD_SHA, id="table"),
    ],
)
def test_commit_edits(
    archive,
    table,
    commit_sha,
    data_archive,
    cli_runner,
    new_sqlserver_db_schema,
    edit_points,
    edit_polygons,
    edit_table,
):
    """ Checkout a working copy and make some edits """
    with data_archive(archive) as repo_path:
        repo = SnoRepo(repo_path)
        H.clear_working_copy()

        with new_sqlserver_db_schema() as (sqlserver_url, sqlserver_schema):
            r = cli_runner.invoke(["create-workingcopy", sqlserver_url])
            assert r.exit_code == 0, r.stderr

            r = cli_runner.invoke(["status"])
            assert r.exit_code == 0, r.stderr
            assert r.stdout.splitlines() == [
                "On branch main",
                "",
                "Nothing to commit, working copy clean",
            ]

            wc = repo.working_copy
            assert wc.status() & WorkingCopyStatus.INITIALISED
            assert wc.status() & WorkingCopyStatus.HAS_DATA

            with wc.session() as sess:
                if archive == "points":
                    edit_points(sess, repo.datasets()[H.POINTS.LAYER], wc)
                elif archive == "polygons":
                    edit_polygons(sess, repo.datasets()[H.POLYGONS.LAYER], wc)
                elif archive == "table":
                    edit_table(sess, repo.datasets()[H.TABLE.LAYER], wc)

            r = cli_runner.invoke(["status"])
            assert r.exit_code == 0, r.stderr
            assert r.stdout.splitlines() == [
                "On branch main",
                "",
                "Changes in working copy:",
                '  (use "kart commit" to commit)',
                '  (use "kart reset" to discard changes)',
                "",
                f"  {table}:",
                "    feature:",
                "      1 inserts",
                "      2 updates",
                "      5 deletes",
            ]
            orig_head = repo.head.peel(pygit2.Commit).hex

            r = cli_runner.invoke(["commit", "-m", "test_commit"])
            assert r.exit_code == 0, r.stderr

            r = cli_runner.invoke(["status"])
            assert r.exit_code == 0, r.stderr
            assert r.stdout.splitlines() == [
                "On branch main",
                "",
                "Nothing to commit, working copy clean",
            ]

            new_head = repo.head.peel(pygit2.Commit).hex
            assert new_head != orig_head

            r = cli_runner.invoke(["checkout", "HEAD^"])

            assert repo.head.peel(pygit2.Commit).hex == orig_head


def test_edit_schema(data_archive, cli_runner, new_sqlserver_db_schema):
    with data_archive("polygons") as repo_path:
        repo = SnoRepo(repo_path)
        H.clear_working_copy()

        with new_sqlserver_db_schema() as (sqlserver_url, sqlserver_schema):
            r = cli_runner.invoke(["create-workingcopy", sqlserver_url])
            assert r.exit_code == 0, r.stderr

            wc = repo.working_copy
            assert wc.status() & WorkingCopyStatus.INITIALISED
            assert wc.status() & WorkingCopyStatus.HAS_DATA

            r = cli_runner.invoke(["diff", "--output-format=quiet"])
            assert r.exit_code == 0, r.stderr

            with wc.session() as sess:
                sess.execute(
                    f"""ALTER TABLE "{sqlserver_schema}"."{H.POLYGONS.LAYER}" ADD colour NVARCHAR(32);"""
                )
                sess.execute(
                    f"""ALTER TABLE "{sqlserver_schema}"."{H.POLYGONS.LAYER}" DROP COLUMN survey_reference;"""
                )

            r = cli_runner.invoke(["diff"])
            assert r.exit_code == 0, r.stderr
            diff = r.stdout.splitlines()

            # New column "colour" has an ID is deterministically generated from the commit hash,
            # but we don't care exactly what it is.
            try:
                colour_id_line = diff[-6]
            except KeyError:
                colour_id_line = ""

            assert diff[-46:] == [
                "--- nz_waca_adjustments:meta:schema.json",
                "+++ nz_waca_adjustments:meta:schema.json",
                "  [",
                "    {",
                '      "id": "79d3c4ca-3abd-0a30-2045-45169357113c",',
                '      "name": "id",',
                '      "dataType": "integer",',
                '      "primaryKeyIndex": 0,',
                '      "size": 64',
                "    },",
                "    {",
                '      "id": "c1d4dea1-c0ad-0255-7857-b5695e3ba2e9",',
                '      "name": "geom",',
                '      "dataType": "geometry",',
                '      "geometryType": "MULTIPOLYGON",',
                '      "geometryCRS": "EPSG:4167"',
                "    },",
                "    {",
                '      "id": "d3d4b64b-d48e-4069-4bb5-dfa943d91e6b",',
                '      "name": "date_adjusted",',
                '      "dataType": "timestamp"',
                "    },",
                "-   {",
                '-     "id": "dff34196-229d-f0b5-7fd4-b14ecf835b2c",',
                '-     "name": "survey_reference",',
                '-     "dataType": "text",',
                '-     "length": 50',
                "-   },",
                "    {",
                '      "id": "13dc4918-974e-978f-05ce-3b4321077c50",',
                '      "name": "adjusted_nodes",',
                '      "dataType": "integer",',
                '      "size": 32',
                "    },",
                "+   {",
                colour_id_line,
                '+     "name": "colour",',
                '+     "dataType": "text",',
                '+     "length": 32',
                "+   },",
                "  ]",
            ]

            orig_head = repo.head.peel(pygit2.Commit).hex

            r = cli_runner.invoke(["commit", "-m", "test_commit"])
            assert r.exit_code == 0, r.stderr

            r = cli_runner.invoke(["status"])
            assert r.exit_code == 0, r.stderr
            assert r.stdout.splitlines() == [
                "On branch main",
                "",
                "Nothing to commit, working copy clean",
            ]

            new_head = repo.head.peel(pygit2.Commit).hex
            assert new_head != orig_head

            r = cli_runner.invoke(["checkout", "HEAD^"])

            assert repo.head.peel(pygit2.Commit).hex == orig_head


def test_approximated_types():
    assert sqlserver_adapter.APPROXIMATED_TYPES == compute_approximated_types(
        sqlserver_adapter.V2_TYPE_TO_MS_TYPE, sqlserver_adapter.MS_TYPE_TO_V2_TYPE
    )


def test_types_roundtrip(data_archive, cli_runner, new_sqlserver_db_schema):
    with data_archive("types") as repo_path:
        repo = SnoRepo(repo_path)
        H.clear_working_copy()

        with new_sqlserver_db_schema() as (sqlserver_url, sqlserver_schema):
            repo.config["sno.workingcopy.path"] = sqlserver_url
            r = cli_runner.invoke(["checkout"])

            # If type-approximation roundtrip code isn't working,
            # we would get spurious diffs on types that SQL server doesn't support.
            r = cli_runner.invoke(["diff", "--exit-code"])
            assert r.exit_code == 0, r.stdout


def test_geometry_constraints(
    data_archive,
    cli_runner,
    new_sqlserver_db_schema,
    edit_points,
    edit_polygons,
    edit_table,
):
    """ Checkout a working copy and make some edits """
    with data_archive("points") as repo_path:
        repo = SnoRepo(repo_path)
        H.clear_working_copy()

        with new_sqlserver_db_schema() as (sqlserver_url, sqlserver_schema):
            r = cli_runner.invoke(["create-workingcopy", sqlserver_url])
            assert r.exit_code == 0, r.stderr

            r = cli_runner.invoke(["diff", "--exit-code"])
            assert r.exit_code == 0

            wc = repo.working_copy
            assert wc.status() & WorkingCopyStatus.INITIALISED
            assert wc.status() & WorkingCopyStatus.HAS_DATA

            with wc.session() as sess:
                sess.execute(
                    f"UPDATE {sqlserver_schema}.{H.POINTS.LAYER} "
                    "SET geom=geometry::STGeomFromText('POINT(0 0)', 4326) WHERE fid=1;"
                )
                # Allowed - Geometry type and CRS ID match the points schema.

            with wc.session() as sess:
                sess.execute(
                    f"UPDATE {sqlserver_schema}.{H.POINTS.LAYER} "
                    "SET geom=NULL WHERE fid=2;"
                )
                # Allowed - NULLs are also allowed.

            with pytest.raises(IntegrityError):
                with wc.session() as sess:
                    sess.execute(
                        f"UPDATE {sqlserver_schema}.{H.POINTS.LAYER} "
                        "SET geom=geometry::STGeomFromText('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))', 4326) WHERE fid=3;"
                    )
                # Not allowed - wrong geometry type

            with pytest.raises(IntegrityError):
                with wc.session() as sess:
                    sess.execute(
                        f"UPDATE {sqlserver_schema}.{H.POINTS.LAYER} "
                        "SET geom=geometry::STGeomFromText('POINT(0 0)', 4327) WHERE fid=4;"
                    )
                # Not allowed - wrong CRS ID
