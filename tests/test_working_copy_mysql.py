import pytest

import pygit2

from kart.repo import KartRepo
from kart.working_copy import sqlserver_adapter
from kart.working_copy.base import WorkingCopyStatus
from kart.working_copy.db_server import DatabaseServer_WorkingCopy
from test_working_copy import compute_approximated_types


H = pytest.helpers.helpers()


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
    new_mysql_db_schema,
):
    """ Checkout a working copy """
    with data_archive(archive) as repo_path:
        repo = KartRepo(repo_path)
        H.clear_working_copy()

        with new_mysql_db_schema(create=existing_schema) as (
            mysql_url,
            mysql_schema,
        ):
            r = cli_runner.invoke(["create-workingcopy", mysql_url])
            assert r.exit_code == 0, r.stderr
            assert (
                r.stdout.splitlines()[-1]
                == f"Creating working copy at {DatabaseServer_WorkingCopy.strip_password(mysql_url)} ..."
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
    new_mysql_db_schema,
    data_archive,
    tmp_path,
    cli_runner,
):
    """ Import the GeoPackage (eg. `kx-foo-layer.gpkg`) into a Kart repository. """
    repo_path = tmp_path / "repo"
    repo_path.mkdir()

    with data_archive("gpkg-points") as data:
        with new_mysql_db_schema(create=existing_schema) as (
            mysql_url,
            mysql_schema,
        ):
            r = cli_runner.invoke(
                [
                    "init",
                    "--import",
                    f"gpkg:{data / 'nz-pa-points-topo-150k.gpkg'}",
                    str(repo_path),
                    f"--workingcopy-path={mysql_url}",
                ]
            )
            assert r.exit_code == 0, r.stderr
            assert (repo_path / ".kart" / "HEAD").exists()

            repo = KartRepo(repo_path)
            wc = repo.working_copy
            assert wc.status() & WorkingCopyStatus.INITIALISED
            assert wc.status() & WorkingCopyStatus.HAS_DATA

            assert wc.location == mysql_url


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
    new_mysql_db_schema,
    edit_points,
    edit_polygons,
    edit_table,
):
    """ Checkout a working copy and make some edits """
    with data_archive(archive) as repo_path:
        repo = KartRepo(repo_path)
        H.clear_working_copy()

        with new_mysql_db_schema() as (mysql_url, mysql_schema):
            r = cli_runner.invoke(["create-workingcopy", mysql_url])
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


def test_edit_schema(data_archive, cli_runner, new_mysql_db_schema):
    with data_archive("polygons") as repo_path:
        repo = KartRepo(repo_path)
        H.clear_working_copy()

        with new_mysql_db_schema() as (mysql_url, mysql_schema):
            r = cli_runner.invoke(["create-workingcopy", mysql_url])
            assert r.exit_code == 0, r.stderr

            wc = repo.working_copy
            assert wc.status() & WorkingCopyStatus.INITIALISED
            assert wc.status() & WorkingCopyStatus.HAS_DATA

            r = cli_runner.invoke(["diff", "--output-format=quiet"])
            assert r.exit_code == 0, r.stderr

            with wc.session() as sess:
                sess.execute(
                    f"""ALTER TABLE "{mysql_schema}"."{H.POLYGONS.LAYER}" ADD colour NVARCHAR(32);"""
                )
                sess.execute(
                    f"""ALTER TABLE "{mysql_schema}"."{H.POLYGONS.LAYER}" DROP COLUMN survey_reference;"""
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


def test_types_roundtrip(data_archive, cli_runner, new_mysql_db_schema):
    with data_archive("types") as repo_path:
        repo = KartRepo(repo_path)
        H.clear_working_copy()

        with new_mysql_db_schema() as (mysql_url, mysql_schema):
            repo.config["kart.workingcopy.location"] = mysql_url
            r = cli_runner.invoke(["checkout", "2d-geometry-only"])

            # If type-approximation roundtrip code isn't working,
            # we would get spurious diffs on types that SQL server doesn't support.
            r = cli_runner.invoke(["diff", "--exit-code"])
            assert r.exit_code == 0, r.stdout


def test_meta_updates(data_archive, cli_runner, new_mysql_db_schema):
    with data_archive("meta-updates"):
        H.clear_working_copy()
        with new_mysql_db_schema() as (mysql_url, mysql_schema):
            r = cli_runner.invoke(["create-workingcopy", mysql_url])
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


def test_checkout_custom_crs(data_archive, cli_runner, new_mysql_db_schema):
    with data_archive("custom_crs") as repo_path:
        repo = KartRepo(repo_path)
        H.clear_working_copy()

        with new_mysql_db_schema() as (mysql_url, mysql_schema):
            repo.config["kart.workingcopy.location"] = mysql_url
            r = cli_runner.invoke(["checkout", "custom-crs"])
            # main has a custom CRS at HEAD. A diff here would mean we are not roundtripping it properly:
            r = cli_runner.invoke(["diff", "--exit-code"])
            assert r.exit_code == 0, r.stdout

            wc = repo.working_copy
            with wc.session() as sess:
                srs_id = sess.scalar(
                    """
                    SELECT srs_id FROM information_schema.st_geometry_columns
                    WHERE table_schema=:table_schema AND table_name=:table_name;
                    """,
                    {"table_schema": mysql_schema, "table_name": H.POINTS.LAYER},
                )
                assert srs_id == 100002

            # Since MySQL has some non-standard CRS requirements, make sure it can also handle these two variants:
            r = cli_runner.invoke(["checkout", "custom-crs-no-axes"])
            r = cli_runner.invoke(["diff", "--exit-code"])
            assert r.exit_code == 0, r.stdout

            r = cli_runner.invoke(["checkout", "custom-crs-axes-last"])
            r = cli_runner.invoke(["diff", "--exit-code"])
            assert r.exit_code == 0, r.stdout

            # We should be able to switch to the previous revision, which has a different (standard) CRS.
            r = cli_runner.invoke(["checkout", "epsg-4326"])
            assert r.exit_code == 0, r.stderr

            with wc.session() as sess:
                srs_id = sess.scalar(
                    """
                    SELECT srs_id FROM information_schema.st_geometry_columns
                    WHERE table_schema=:table_schema AND table_name=:table_name;
                    """,
                    {"table_schema": mysql_schema, "table_name": H.POINTS.LAYER},
                )
                assert srs_id == 4326

            # Checkout main to the WC, then set HEAD back to main^ without updating the WC.
            # (This is just a way to use Kart to simulate the user manually changing the CRS in the WC.)
            # Make sure we can see this rev<>WC change in kart diff.
            head_commit = repo.head_commit.hex
            head_tree = repo.head_tree.hex
            r = cli_runner.invoke(["checkout", "custom-crs"])
            assert r.exit_code == 0, r.stderr
            repo.write_gitdir_file("HEAD", head_commit)
            repo.working_copy.update_state_table_tree(head_tree)

            r = cli_runner.invoke(["diff"])
            assert r.stdout.splitlines() == [
                '--- nz_pa_points_topo_150k:meta:crs/EPSG:4326.wkt',
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
                '- ',
                '+++ nz_pa_points_topo_150k:meta:crs/koordinates.com:100002.wkt',
                '+ PROJCS["NAD83 / Austin",',
                '+     GEOGCS["NAD83",',
                '+         DATUM["North_American_Datum_1983",',
                '+             SPHEROID["GRS 1980", 6378137.0, 298.257222101],',
                '+             TOWGS84[0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]],',
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
                '+ ',
                '--- nz_pa_points_topo_150k:meta:schema.json',
                '+++ nz_pa_points_topo_150k:meta:schema.json',
                '  [',
                '    {',
                '      "id": "e97b4015-2765-3a33-b174-2ece5c33343b",',
                '      "name": "fid",',
                '      "dataType": "integer",',
                '      "primaryKeyIndex": 0,',
                '      "size": 64',
                '    },',
                '    {',
                '      "id": "f488ae9b-6e15-1fe3-0bda-e0d5d38ea69e",',
                '      "name": "geom",',
                '      "dataType": "geometry",',
                '      "geometryType": "POINT",',
                '-     "geometryCRS": "EPSG:4326",',
                '+     "geometryCRS": "koordinates.com:100002",',
                '    },',
                '    {',
                '      "id": "4a1c7a86-c425-ea77-7f1a-d74321a10edc",',
                '      "name": "t50_fid",',
                '      "dataType": "integer",',
                '      "size": 32',
                '    },',
                '    {',
                '      "id": "d2a62351-a66d-bde2-ce3e-356fec9641e9",',
                '      "name": "name_ascii",',
                '      "dataType": "text",',
                '      "length": 75',
                '    },',
                '    {',
                '      "id": "c3389414-a511-5385-7dcd-891c4ead1663",',
                '      "name": "macronated",',
                '      "dataType": "text",',
                '      "length": 1',
                '    },',
                '    {',
                '      "id": "45b00eaa-5700-662d-8a21-9614e40c437b",',
                '      "name": "name",',
                '      "dataType": "text",',
                '      "length": 75',
                '    },',
                '  ]',
            ]
