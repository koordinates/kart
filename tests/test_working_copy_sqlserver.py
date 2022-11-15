import pytest
import pygit2
from sqlalchemy.exc import IntegrityError

from kart.exceptions import NotFound
from kart.repo import KartRepo
from kart.sqlalchemy import strip_password
from kart.sqlalchemy.sqlserver import Db_SqlServer
from kart.sqlalchemy.adapter.sqlserver import KartAdapter_SqlServer

from kart.tabular.working_copy.base import TableWorkingCopyStatus
from test_working_copy import compute_approximated_types


pytestmark = pytest.mark.xdist_group(name="mssql")

H = pytest.helpers.helpers()


def test_no_odbc():
    # if unixODBC is installed or we're on Windows we can't test the not-installed message
    try:
        import pyodbc  # noqa
    except ImportError:
        pass
    else:
        pytest.skip("ODBC is available, so we can't test the no-ODBC error message")

    with pytest.raises(
        NotFound, match=r"^ODBC support for SQL Server is required but was not found."
    ):
        Db_SqlServer.get_odbc_drivers()


def test_odbc_drivers():
    # if unixODBC isn't installed we can't test this
    # use a try/except so we get a better message than via pytest.importorskip
    try:
        import pyodbc  # noqa

        has_odbc = True
    except ImportError:
        has_odbc = False

    pytest.helpers.feature_assert_or_skip("ODBC support", "KART_EXPECT_ODBC", has_odbc)

    num_drivers = len(Db_SqlServer.get_odbc_drivers())
    # Eventually we should assert that we have useful drivers - eg MSSQL.
    # But for now, we are asserting that we were able to load pyodbc and it seems to be working.
    assert num_drivers >= 0
    print(f"Found {num_drivers} ODBC drivers")


def test_sqlserver_driver():
    try:
        assert Db_SqlServer.get_sqlserver_driver() is not None
        has_mssql_driver = True
    except NotFound:
        has_mssql_driver = False

    pytest.helpers.feature_assert_or_skip(
        "MSSQL driver", "KART_EXPECT_MSSQLDRIVER", has_mssql_driver
    )


# All of the following tests will also fail unless a MSSQL driver has been installed manually.
# However, they are not marked as xfail, since they do not run unless KART_SQLSERVER_URL is set
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
    """Checkout a working copy"""
    with data_archive(archive) as repo_path:
        repo = KartRepo(repo_path)
        H.clear_working_copy()

        with new_sqlserver_db_schema(create=existing_schema) as (
            sqlserver_url,
            sqlserver_schema,
        ):
            r = cli_runner.invoke(["create-workingcopy", sqlserver_url])
            assert r.exit_code == 0, r.stderr
            assert (
                r.stdout.splitlines()[-1]
                == f"Creating SQL Server working copy at {strip_password(sqlserver_url)} ..."
            )

            r = cli_runner.invoke(["status"])
            assert r.exit_code == 0, r.stderr
            assert r.stdout.splitlines() == [
                "On branch main",
                "",
                "Nothing to commit, working copy clean",
            ]

            table_wc = repo.working_copy.tabular
            assert table_wc.status() & TableWorkingCopyStatus.INITIALISED
            assert table_wc.status() & TableWorkingCopyStatus.HAS_DATA
            table_wc.assert_matches_head_tree()

            # Also test the importer by making sure we can import this from the WC:
            r = cli_runner.invoke(["import", sqlserver_url, f"{table}:{table}_2"])
            assert r.exit_code == 0, r.stderr


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
    """Import the GeoPackage (eg. `kx-foo-layer.gpkg`) into a Kart repository."""
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

            repo = KartRepo(repo_path)
            table_wc = repo.working_copy.tabular
            assert table_wc.status() & TableWorkingCopyStatus.INITIALISED
            assert table_wc.status() & TableWorkingCopyStatus.HAS_DATA

            assert table_wc.location == sqlserver_url


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
    """Checkout a working copy and make some edits"""
    with data_archive(archive) as repo_path:
        repo = KartRepo(repo_path)
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

            table_wc = repo.working_copy.tabular
            assert table_wc.status() & TableWorkingCopyStatus.INITIALISED
            assert table_wc.status() & TableWorkingCopyStatus.HAS_DATA

            with table_wc.session() as sess:
                if archive == "points":
                    edit_points(sess, repo.datasets()[H.POINTS.LAYER], table_wc)
                elif archive == "polygons":
                    edit_polygons(sess, repo.datasets()[H.POLYGONS.LAYER], table_wc)
                elif archive == "table":
                    edit_table(sess, repo.datasets()[H.TABLE.LAYER], table_wc)

            r = cli_runner.invoke(["status"])
            assert r.exit_code == 0, r.stderr
            assert r.stdout.splitlines() == [
                "On branch main",
                "",
                "Changes in working copy:",
                '  (use "kart commit" to commit)',
                '  (use "kart restore" to discard changes)',
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
        repo = KartRepo(repo_path)
        H.clear_working_copy()

        with new_sqlserver_db_schema() as (sqlserver_url, sqlserver_schema):
            r = cli_runner.invoke(["create-workingcopy", sqlserver_url])
            assert r.exit_code == 0, r.stderr

            table_wc = repo.working_copy.tabular
            assert table_wc.status() & TableWorkingCopyStatus.INITIALISED
            assert table_wc.status() & TableWorkingCopyStatus.HAS_DATA

            r = cli_runner.invoke(["diff", "--output-format=quiet"])
            assert r.exit_code == 0, r.stderr

            with table_wc.session() as sess:
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

            assert diff[-47:] == [
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
                '      "dataType": "timestamp",',
                '      "timezone": "UTC"',
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


def test_auto_increment_pk(data_archive, cli_runner, new_sqlserver_db_schema):
    with data_archive("polygons") as repo_path:
        H.clear_working_copy()
        with new_sqlserver_db_schema() as (sqlserver_url, sqlserver_schema):
            r = cli_runner.invoke(["create-workingcopy", sqlserver_url])
            assert r.exit_code == 0, r.stderr

            repo = KartRepo(repo_path)
            with repo.working_copy.tabular.session() as sess:
                t = f"{sqlserver_schema}.{H.POLYGONS.LAYER}"
                count = sess.scalar(
                    f"SELECT COUNT(*) FROM {t} WHERE id = {H.POLYGONS.NEXT_UNASSIGNED_PK};"
                )
                assert count == 0
                sess.execute(f"INSERT INTO {t} (geom) VALUES (NULL);")
                count = sess.scalar(
                    f"SELECT COUNT(*) FROM {t} WHERE id = {H.POLYGONS.NEXT_UNASSIGNED_PK};"
                )
                assert count == 1


def test_approximated_types():
    assert KartAdapter_SqlServer.APPROXIMATED_TYPES == compute_approximated_types(
        KartAdapter_SqlServer.V2_TYPE_TO_SQL_TYPE,
        KartAdapter_SqlServer.SQL_TYPE_TO_V2_TYPE,
    )


def test_types_roundtrip(data_archive, cli_runner, new_sqlserver_db_schema):
    with data_archive("types") as repo_path:
        repo = KartRepo(repo_path)
        H.clear_working_copy()

        with new_sqlserver_db_schema() as (sqlserver_url, sqlserver_schema):
            repo.config["kart.workingcopy.location"] = sqlserver_url
            r = cli_runner.invoke(["checkout"])

            # If type-approximation roundtrip code isn't working,
            # we would get spurious diffs on types that SQL server doesn't support.
            r = cli_runner.invoke(["diff", "--exit-code"])
            assert r.exit_code == 0, r.stdout


def test_values_roundtrip(data_archive, cli_runner, new_sqlserver_db_schema):
    with data_archive("types") as repo_path:
        repo = KartRepo(repo_path)
        H.clear_working_copy()

        with new_sqlserver_db_schema() as (sqlserver_url, sqlserver_schema):
            repo.config["kart.workingcopy.location"] = sqlserver_url
            # TODO - fix SQL server to roundtrip 3D and 4D geometries.
            r = cli_runner.invoke(["checkout"])

            with repo.working_copy.tabular.session() as sess:
                # We don't diff values unless they're marked as dirty in the WC - move the row to make it dirty.
                sess.execute(
                    f'UPDATE {sqlserver_schema}.manytypes SET "PK"="PK" + 1000;'
                )
                sess.execute(
                    f'UPDATE {sqlserver_schema}.manytypes SET "PK"="PK" - 1000;'
                )

            # If values roundtripping code isn't working for certain types,
            # we could get spurious diffs on those values.
            r = cli_runner.invoke(["diff", "--exit-code"])
            assert r.exit_code == 0, r.stdout


def test_empty_geometry_roundtrip(data_archive, cli_runner, new_sqlserver_db_schema):
    with data_archive("empty-geometry") as repo_path:
        repo = KartRepo(repo_path)
        H.clear_working_copy()

        with new_sqlserver_db_schema() as (sqlserver_url, sqlserver_schema):
            repo.config["kart.workingcopy.location"] = sqlserver_url
            r = cli_runner.invoke(["checkout"])

            with repo.working_copy.tabular.session() as sess:
                # We don't diff values unless they're marked as dirty in the WC - move the row to make it dirty.
                sess.execute(
                    f'UPDATE {sqlserver_schema}.point_test SET "PK"="PK" + 1000;'
                )
                sess.execute(
                    f'UPDATE {sqlserver_schema}.point_test SET "PK"="PK" - 1000;'
                )
                sess.execute(
                    f'UPDATE {sqlserver_schema}.polygon_test SET "PK"="PK" + 1000;'
                )
                sess.execute(
                    f'UPDATE {sqlserver_schema}.polygon_test SET "PK"="PK" - 1000;'
                )

            # If values roundtripping code isn't working for certain types,
            # we could get spurious diffs on those values.
            r = cli_runner.invoke(["diff", "--exit-code"])
            assert r.exit_code == 0, r.stdout


def test_meta_updates(data_archive, cli_runner, new_sqlserver_db_schema):
    with data_archive("meta-updates"):
        H.clear_working_copy()
        with new_sqlserver_db_schema() as (sqlserver_url, sqlserver_schema):
            r = cli_runner.invoke(["create-workingcopy", sqlserver_url])
            assert r.exit_code == 0, r.stderr

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


def test_geometry_constraints(
    data_archive,
    cli_runner,
    new_sqlserver_db_schema,
    edit_points,
    edit_polygons,
    edit_table,
):
    """Checkout a working copy and make some edits"""
    with data_archive("points") as repo_path:
        repo = KartRepo(repo_path)
        H.clear_working_copy()

        with new_sqlserver_db_schema() as (sqlserver_url, sqlserver_schema):
            r = cli_runner.invoke(["create-workingcopy", sqlserver_url])
            assert r.exit_code == 0, r.stderr

            r = cli_runner.invoke(["diff", "--exit-code"])
            assert r.exit_code == 0

            table_wc = repo.working_copy.tabular
            assert table_wc.status() & TableWorkingCopyStatus.INITIALISED
            assert table_wc.status() & TableWorkingCopyStatus.HAS_DATA

            with table_wc.session() as sess:
                sess.execute(
                    f"UPDATE {sqlserver_schema}.{H.POINTS.LAYER} "
                    "SET geom=geometry::STGeomFromText('POINT(0 0)', 4326) WHERE fid=1;"
                )
                # Allowed - Geometry type and CRS ID match the points schema.

            with table_wc.session() as sess:
                sess.execute(
                    f"UPDATE {sqlserver_schema}.{H.POINTS.LAYER} "
                    "SET geom=NULL WHERE fid=2;"
                )
                # Allowed - NULLs are also allowed.

            with pytest.raises(IntegrityError):
                with table_wc.session() as sess:
                    sess.execute(
                        f"UPDATE {sqlserver_schema}.{H.POINTS.LAYER} "
                        "SET geom=geometry::STGeomFromText('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))', 4326) WHERE fid=3;"
                    )
                # Not allowed - wrong geometry type

            with pytest.raises(IntegrityError):
                with table_wc.session() as sess:
                    sess.execute(
                        f"UPDATE {sqlserver_schema}.{H.POINTS.LAYER} "
                        "SET geom=geometry::STGeomFromText('POINT(0 0)', 4327) WHERE fid=4;"
                    )
                # Not allowed - wrong CRS ID


def test_checkout_custom_crs(
    data_archive, cli_runner, new_sqlserver_db_schema, dodgy_restore
):
    with data_archive("custom_crs") as repo_path:
        repo = KartRepo(repo_path)
        H.clear_working_copy()

        with new_sqlserver_db_schema() as (sqlserver_url, sqlserver_schema):
            repo.config["kart.workingcopy.location"] = sqlserver_url
            r = cli_runner.invoke(["checkout", "custom-crs"])

            # main branch has a custom CRS at HEAD. A diff here would mean we are not roundtripping it properly.
            # In fact we *cannot* roundtrip it properly since MSSQL cannot store custom CRS, but we should at least not
            # get a spurious diff when the user has not made any edits - the diff should be hidden.
            r = cli_runner.invoke(["diff", "--exit-code"])
            assert r.exit_code == 0, r.stdout

            # Even though SQL Server cannot store the custom CRS, it can still store the CRS ID in the geometries:
            table_wc = repo.working_copy.tabular
            with table_wc.session() as sess:
                srid = sess.scalar(
                    f"SELECT TOP 1 geom.STSrid FROM {sqlserver_schema}.{H.POINTS.LAYER};"
                )
                assert srid == 100002

            # We should be able to checkout the previous revision, which has a different (standard) CRS.
            r = cli_runner.invoke(["checkout", "epsg-4326"])
            assert r.exit_code == 0, r.stdout

            table_wc = repo.working_copy.tabular
            with table_wc.session() as sess:
                srid = sess.scalar(
                    f"SELECT TOP 1 geom.STSrid FROM {sqlserver_schema}.{H.POINTS.LAYER};"
                )
                assert srid == 4326

            # Restore the contents of custom-crs to the WC so we can make sure WC diff is working:
            dodgy_restore(repo, "custom-crs")

            # We can detect that the CRS ID has changed to 100002, but SQL server can't actually store
            # the custom definition, so we don't know what it is.
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
                '+     "geometryCRS": "CUSTOM:100002",',
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


def test_checkout_and_status_with_no_crs(
    new_sqlserver_db_schema,
    data_archive,
    tmp_path,
    cli_runner,
):
    repo_path = tmp_path / "repo"
    repo_path.mkdir()

    with data_archive("points-no-crs") as repo_path:
        with new_sqlserver_db_schema() as (sqlserver_url, sqlserver_schema):
            r = cli_runner.invoke(["create-workingcopy", sqlserver_url])
            r = cli_runner.invoke(
                [
                    "-C",
                    str(repo_path),
                    "status",
                ]
            )
            assert r.exit_code == 0, r.stderr


def test_checkout_large_geometry(
    new_sqlserver_db_schema,
    data_archive,
    tmp_path,
    cli_runner,
):
    repo_path = tmp_path / "repo"
    repo_path.mkdir()

    with data_archive("large-geometry") as repo_path:
        with new_sqlserver_db_schema() as (sqlserver_url, sqlserver_schema):
            r = cli_runner.invoke(["create-workingcopy", sqlserver_url])
            r = cli_runner.invoke(
                [
                    "-C",
                    str(repo_path),
                    "status",
                ]
            )
            assert r.exit_code == 0, r.stderr
