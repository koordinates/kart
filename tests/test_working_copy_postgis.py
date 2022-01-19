import pytest

import pygit2

from sqlalchemy import inspect

from kart.repo import KartRepo

from kart.working_copy.base import WorkingCopyStatus
from kart.sqlalchemy import strip_password
from kart.sqlalchemy.adapter.postgis import KartAdapter_Postgis
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
    new_postgis_db_schema,
):
    """ Checkout a working copy """
    with data_archive(archive) as repo_path:
        repo = KartRepo(repo_path)
        H.clear_working_copy()

        with new_postgis_db_schema(create=existing_schema) as (
            postgres_url,
            postgres_schema,
        ):
            r = cli_runner.invoke(["create-workingcopy", postgres_url])
            assert r.exit_code == 0, r.stderr
            assert (
                r.stdout.splitlines()[-1]
                == f"Creating working copy at {strip_password(postgres_url)} ..."
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

            # Also test the importer by making sure we can import this from the WC:
            r = cli_runner.invoke(["import", postgres_url, f"{table}:{table}_2"])
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
    new_postgis_db_schema,
    data_archive,
    tmp_path,
    cli_runner,
):
    """ Import the GeoPackage (eg. `kx-foo-layer.gpkg`) into a Kart repository. """
    repo_path = tmp_path / "repo"
    repo_path.mkdir()

    with data_archive("gpkg-points") as data:
        with new_postgis_db_schema(create=existing_schema) as (
            postgres_url,
            postgres_schema,
        ):
            r = cli_runner.invoke(
                [
                    "init",
                    "--import",
                    f"gpkg:{data / 'nz-pa-points-topo-150k.gpkg'}",
                    str(repo_path),
                    f"--workingcopy-path={postgres_url}",
                ]
            )
            assert r.exit_code == 0, r.stderr
            assert (repo_path / ".kart" / "HEAD").exists()

            repo = KartRepo(repo_path)
            wc = repo.working_copy
            assert wc.status() & WorkingCopyStatus.INITIALISED
            assert wc.status() & WorkingCopyStatus.HAS_DATA

            assert wc.location == postgres_url


def test_checkout_and_status_with_no_crs(
    new_postgis_db_schema,
    data_archive,
    tmp_path,
    cli_runner,
):
    repo_path = tmp_path / "repo"
    repo_path.mkdir()

    with data_archive("points-no-crs") as repo_path:
        with new_postgis_db_schema() as (postgres_url, postgres_schema):
            r = cli_runner.invoke(["create-workingcopy", postgres_url])
            r = cli_runner.invoke(
                [
                    "-C",
                    str(repo_path),
                    "status",
                ]
            )
            assert r.exit_code == 0, r.stderr


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
    new_postgis_db_schema,
    edit_points,
    edit_polygons,
    edit_table,
):
    """ Checkout a working copy and make some edits """
    with data_archive(archive) as repo_path:
        repo = KartRepo(repo_path)
        H.clear_working_copy()

        with new_postgis_db_schema() as (postgres_url, postgres_schema):
            r = cli_runner.invoke(["create-workingcopy", postgres_url])
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


def test_postgis_wc_with_long_index_name(
    data_archive,
    tmp_path,
    cli_runner,
    chdir,
    new_postgis_db_schema,
):
    with data_archive("gpkg-points") as data:
        # list tables
        repo_path = tmp_path / "repo"
        repo_path.mkdir()

        # create a repo
        with new_postgis_db_schema() as (
            postgres_url,
            postgres_schema,
        ):
            r = cli_runner.invoke(
                ["init", f"--workingcopy-path={postgres_url}", str(repo_path)]
            )
            assert r.exit_code == 0, r.stderr

            # import a very long named dataset
            r = cli_runner.invoke(
                [
                    "-C",
                    str(repo_path),
                    "import",
                    f"{data}/nz-pa-points-topo-150k.gpkg",
                    "nz_pa_points_topo_150k:a_really_long_table_name_that_is_really_actually_quite_long_dont_you_think",
                ]
            )
            assert r.exit_code == 0, r.stderr

            repo = KartRepo(repo_path)
            assert not repo.is_bare
            assert not repo.is_empty

            wc = repo.working_copy
            insp = inspect(wc.engine)
            indexes = insp.get_indexes(
                "a_really_long_table_name_that_is_really_actually_quite_long_dont_you_think",
                schema=postgres_schema,
            )
            assert len(indexes) == 1
            assert indexes[0] == {
                "name": "a_really_long_table_na_0ccb228e66871172f030077f3a9974d2b58d1ee5",
                "unique": False,
                "column_names": ["geom"],
                "include_columns": [],
                "dialect_options": {"postgresql_using": "gist"},
            }


def test_edit_schema(data_archive, cli_runner, new_postgis_db_schema):
    with data_archive("polygons") as repo_path:
        repo = KartRepo(repo_path)
        H.clear_working_copy()

        with new_postgis_db_schema() as (postgres_url, postgres_schema):
            r = cli_runner.invoke(["create-workingcopy", postgres_url])
            assert r.exit_code == 0, r.stderr

            wc = repo.working_copy
            assert wc.status() & WorkingCopyStatus.INITIALISED
            assert wc.status() & WorkingCopyStatus.HAS_DATA

            r = cli_runner.invoke(["diff", "--output-format=quiet"])
            assert r.exit_code == 0, r.stderr

            with wc.session() as sess:
                sess.execute(
                    f"""COMMENT ON TABLE "{postgres_schema}"."{H.POLYGONS.LAYER}" IS 'New title';"""
                )
                sess.execute(
                    f"""ALTER TABLE "{postgres_schema}"."{H.POLYGONS.LAYER}" ADD COLUMN colour VARCHAR(32);"""
                )
                sess.execute(
                    f"""ALTER TABLE "{postgres_schema}"."{H.POLYGONS.LAYER}" DROP COLUMN survey_reference;"""
                )
                sess.execute(
                    f"""
                    ALTER TABLE "{postgres_schema}"."{H.POLYGONS.LAYER}" ALTER COLUMN geom
                    TYPE geometry(MULTIPOLYGON, 3857)
                    USING ST_SetSRID(geom, 3857);
                    """
                )

            r = cli_runner.invoke(["diff"])
            assert r.exit_code == 0, r.stderr
            diff = r.stdout.splitlines()
            assert "--- nz_waca_adjustments:meta:crs/EPSG:4167.wkt" in diff
            assert "+++ nz_waca_adjustments:meta:crs/EPSG:3857.wkt" in diff

            # New column "colour" has an ID is deterministically generated from the commit hash,
            # but we don't care exactly what it is.
            try:
                colour_id_line = diff[-10]
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
                '-     "geometryCRS": "EPSG:4167",',
                '+     "geometryCRS": "EPSG:3857",',
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
                "--- nz_waca_adjustments:meta:title",
                "+++ nz_waca_adjustments:meta:title",
                "- NZ WACA Adjustments",
                "+ New title",
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


class SucceedAndRollback(Exception):
    """
    This test passed, but raising an exception will cause the DB transaction to rollback.
    Which is what we want to do, to undo any changes to public.spatial_ref_sys
    """

    pass


def test_edit_crs(data_archive, cli_runner, new_postgis_db_schema):
    with data_archive("points") as repo_path:
        repo = KartRepo(repo_path)
        H.clear_working_copy()

        with new_postgis_db_schema() as (postgres_url, postgres_schema):
            r = cli_runner.invoke(["create-workingcopy", postgres_url])
            assert r.exit_code == 0, r.stderr

            wc = repo.working_copy
            assert wc.status() & WorkingCopyStatus.INITIALISED
            assert wc.status() & WorkingCopyStatus.HAS_DATA
            assert not wc.is_dirty()

            # The test is run inside a single transaction which we always roll back -
            # this is because we are editing the public.spatial_ref_sys table, which is shared by
            # everything in the postgis DB - we don't want these temporary changes to make other
            # tests fail, and we want to roll them immediately whether the test passes or fails.
            with pytest.raises(SucceedAndRollback):
                with wc.session() as sess:

                    crs = sess.scalar(
                        "SELECT srtext FROM public.spatial_ref_sys WHERE srid=4326"
                    )
                    assert crs.startswith('GEOGCS["WGS 84",')
                    assert crs.endswith('AUTHORITY["EPSG","4326"]]')

                    # Make an unimportant, cosmetic change, while keeping the CRS basically EPSG:4326
                    crs = crs.replace('GEOGCS["WGS 84",', 'GEOGCS["WGS 1984",')
                    sess.execute(
                        """UPDATE public.spatial_ref_sys SET srtext=:srtext WHERE srid=4326;""",
                        {"srtext": crs},
                    )

                    # kart diff hides differences between dataset CRS and WC CRS if they are both supposed to be EPSG:4326
                    # (or any other standard CRS). See POSTGIS_WC.md
                    assert not wc.is_dirty()

                    # Change the CRS authority to CUSTOM
                    crs = crs.replace(
                        'AUTHORITY["EPSG","4326"]]', 'AUTHORITY["CUSTOM","4326"]]'
                    )

                    sess.execute(
                        """UPDATE public.spatial_ref_sys SET srtext=:srtext WHERE srid=4326;""",
                        {"srtext": crs},
                    )

                    # Now kart diff should show the change, and it is possible to commit the change.
                    assert wc.is_dirty()

                    commit_id = repo.structure().commit_diff(
                        wc.diff_to_tree(), "Modify CRS"
                    )
                    wc.update_state_table_tree(commit_id.hex)

                    assert not wc.is_dirty()

                    r = cli_runner.invoke(["show"])
                    lines = r.stdout.splitlines()
                    assert "--- nz_pa_points_topo_150k:meta:crs/EPSG:4326.wkt" in lines
                    assert (
                        "+++ nz_pa_points_topo_150k:meta:crs/CUSTOM:4326.wkt" in lines
                    )

                    raise SucceedAndRollback()


def test_auto_increment_pk(data_archive, cli_runner, new_postgis_db_schema):
    with data_archive("polygons") as repo_path:
        H.clear_working_copy()
        with new_postgis_db_schema() as (postgres_url, postgres_schema):
            r = cli_runner.invoke(["create-workingcopy", postgres_url])
            assert r.exit_code == 0, r.stderr

            repo = KartRepo(repo_path)
            with repo.working_copy.session() as sess:
                t = f"{postgres_schema}.{H.POLYGONS.LAYER}"
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
    assert KartAdapter_Postgis.APPROXIMATED_TYPES == compute_approximated_types(
        KartAdapter_Postgis.V2_TYPE_TO_SQL_TYPE, KartAdapter_Postgis.SQL_TYPE_TO_V2_TYPE
    )


def test_types_roundtrip(data_archive, cli_runner, new_postgis_db_schema):
    with data_archive("types") as repo_path:
        repo = KartRepo(repo_path)
        H.clear_working_copy()

        with new_postgis_db_schema() as (postgres_url, postgres_schema):
            repo.config["kart.workingcopy.location"] = postgres_url
            r = cli_runner.invoke(["checkout"])

            # If type-approximation roundtrip code isn't working,
            # we would get spurious diffs on types that PostGIS doesn't support.
            r = cli_runner.invoke(["diff", "--exit-code"])
            assert r.exit_code == 0, r.stdout


def test_values_roundtrip(data_archive, cli_runner, new_postgis_db_schema):
    with data_archive("types") as repo_path:
        repo = KartRepo(repo_path)
        H.clear_working_copy()

        with new_postgis_db_schema() as (postgres_url, postgres_schema):
            repo.config["kart.workingcopy.location"] = postgres_url
            r = cli_runner.invoke(["checkout"])

            with repo.working_copy.session() as sess:
                # We don't diff values unless they're marked as dirty in the WC - move the row to make it dirty.
                sess.execute(
                    f'UPDATE {postgres_schema}.manytypes SET "PK"="PK" + 1000;'
                )
                sess.execute(
                    f'UPDATE {postgres_schema}.manytypes SET "PK"="PK" - 1000;'
                )

            # If values roundtripping code isn't working for certain types,
            # we could get spurious diffs on those values.
            r = cli_runner.invoke(["diff", "--exit-code"])
            assert r.exit_code == 0, r.stdout


def test_empty_geometry_roundtrip(data_archive, cli_runner, new_postgis_db_schema):
    with data_archive("empty-geometry") as repo_path:
        repo = KartRepo(repo_path)
        H.clear_working_copy()

        with new_postgis_db_schema() as (postgres_url, postgres_schema):
            repo.config["kart.workingcopy.location"] = postgres_url
            r = cli_runner.invoke(["checkout"])

            with repo.working_copy.session() as sess:
                # We don't diff values unless they're marked as dirty in the WC - move the row to make it dirty.
                sess.execute(
                    f'UPDATE {postgres_schema}.point_test SET "PK"="PK" + 1000;'
                )
                sess.execute(
                    f'UPDATE {postgres_schema}.point_test SET "PK"="PK" - 1000;'
                )
                sess.execute(
                    f'UPDATE {postgres_schema}.polygon_test SET "PK"="PK" + 1000;'
                )
                sess.execute(
                    f'UPDATE {postgres_schema}.polygon_test SET "PK"="PK" - 1000;'
                )

            # If values roundtripping code isn't working for certain types,
            # we could get spurious diffs on those values.
            r = cli_runner.invoke(["diff", "--exit-code"])
            assert r.exit_code == 0, r.stdout


def test_meta_updates(data_archive, cli_runner, new_postgis_db_schema):
    with data_archive("meta-updates"):
        H.clear_working_copy()
        with new_postgis_db_schema() as (postgres_url, postgres_schema):
            r = cli_runner.invoke(["create-workingcopy", postgres_url])
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


def test_checkout_custom_crs(
    data_archive, cli_runner, new_postgis_db_schema, dodgy_restore
):
    with data_archive("custom_crs") as repo_path:
        repo = KartRepo(repo_path)
        H.clear_working_copy()

        with new_postgis_db_schema() as (postgres_url, postgres_schema):
            repo.config["kart.workingcopy.location"] = postgres_url
            r = cli_runner.invoke(["checkout", "custom-crs"])
            # main has a custom CRS at HEAD. A diff here would mean we are not roundtripping it properly:
            r = cli_runner.invoke(["diff", "--exit-code"])
            assert r.exit_code == 0, r.stderr

            wc = repo.working_copy
            with wc.session() as sess:
                srid = sess.scalar(
                    """
                    SELECT srid FROM geometry_columns WHERE f_table_schema=:table_schema AND f_table_name=:table_name;
                    """,
                    {"table_schema": postgres_schema, "table_name": H.POINTS.LAYER},
                )
                assert srid == 100002

            # We should be able to switch to the previous revision, which has a different (standard) CRS.
            r = cli_runner.invoke(["checkout", "epsg-4326"])
            assert r.exit_code == 0, r.stderr

            with wc.session() as sess:
                srid = sess.scalar(
                    """
                    SELECT srid FROM geometry_columns WHERE f_table_schema=:table_schema AND f_table_name=:table_name;
                    """,
                    {"table_schema": postgres_schema, "table_name": H.POINTS.LAYER},
                )
                assert srid == 4326

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
