import contextlib
import itertools
import os
from pathlib import Path
import re
import subprocess

from osgeo import gdal, ogr

import pygit2
import pytest

from kart import init, fast_import
from kart.dataset3 import Dataset3
from kart.dataset3_paths import IntPathEncoder, MsgpackHashPathEncoder
from kart.exceptions import INVALID_OPERATION
from kart.sqlalchemy.gpkg import Db_GPKG
from kart.schema import Schema
from kart.geometry import ogr_to_gpkg_geom, gpkg_geom_to_ogr
from kart.import_source import ImportSource
from kart.ogr_import_source import postgres_url_to_ogr_conn_str
from kart.pk_generation import PkGeneratingImportSource
from kart.repo import KartRepo


H = pytest.helpers.helpers()

# copied from test_init.py
GPKG_IMPORTS = (
    "archive,source_gpkg,table",
    [
        pytest.param(
            "gpkg-points", "nz-pa-points-topo-150k.gpkg", H.POINTS.LAYER, id="points"
        ),
        pytest.param(
            "gpkg-polygons",
            "nz-waca-adjustments.gpkg",
            H.POLYGONS.LAYER,
            id="polygons",
        ),
        pytest.param(
            "gpkg-au-census",
            "census2016_sdhca_ot_short.gpkg",
            "census2016_sdhca_ot_ra_short",
            id="au-ra-short",
        ),
        pytest.param("gpkg-spec", "sample1_2.gpkg", "counties", id="spec_counties"),
        pytest.param(
            "gpkg-spec", "sample1_2.gpkg", "countiestbl", id="spec_counties_table"
        ),
    ],
)


def _import_check(repo_path, table, source_gpkg):
    repo = KartRepo(repo_path)
    dataset = repo.datasets()[table]
    assert dataset.VERSION == 3

    with Db_GPKG.create_engine(source_gpkg).connect() as conn:
        num_rows = conn.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]

    o = subprocess.check_output(["git", "ls-tree", "-r", "-t", "HEAD", table])
    print("\n".join(l.decode("utf8") for l in o.splitlines()[:20]))

    if dataset.VERSION != 3:
        raise NotImplementedError(dataset.VERSION)

    re_paths = r"^\d{6} blob [0-9a-f]{40}\t%s/.table-dataset/feature/.*$" % table
    git_paths = [m for m in re.findall(re_paths, o.decode("utf-8"), re.MULTILINE)]
    assert len(git_paths) == num_rows

    num_features = dataset.feature_count
    assert num_features == num_rows

    return dataset


def normalise_feature(row):
    row = dict(row)
    # In production code, this is done automatically the type converters in KartAdapter_GPKG.
    # Here we do it crudely "by hand", to make sure it's doing what we expect.
    for field_name in ("geom", "Shape"):
        if field_name in row:
            row[field_name] = ogr_to_gpkg_geom(
                gpkg_geom_to_ogr(row[field_name], parse_crs=True),
            )
    for field_name in ("date_adjusted",):
        if row.get(field_name):
            row[field_name] = row[field_name].rstrip("Z")

    return row


def without_ids(column_dicts):
    # Note that datasets use lru-caches for meta items, so we make sure not to modify the meta items.
    return [col_without_id(c) for c in column_dicts]


def col_without_id(column_dict):
    result = column_dict.copy()
    result.pop("id")
    return result


@pytest.mark.slow
@pytest.mark.parametrize(
    GPKG_IMPORTS[0],
    [
        *GPKG_IMPORTS[1],
        pytest.param(
            "gpkg-points", "nz-pa-points-topo-150k.gpkg", H.POINTS.LAYER, id="empty"
        ),
    ],
)
@pytest.mark.parametrize("profile", ["fast_import", "checkout"])
def test_import(
    profile,
    archive,
    source_gpkg,
    table,
    data_archive,
    tmp_path,
    cli_runner,
    chdir,
    benchmark,
    request,
    monkeypatch,
):
    """ Import the GeoPackage (eg. `kx-foo-layer.gpkg`) into a Kart repository. """
    param_ids = H.parameter_ids(request)

    # wrap the original functions with benchmarking
    orig_import_func = fast_import.fast_import_tables
    orig_checkout_func = init._add_datasets_to_working_copy

    def _benchmark_import(*args, **kwargs):
        # one round/iteration isn't very statistical, but hopefully crude idea
        return benchmark.pedantic(
            orig_import_func, args=args, kwargs=kwargs, rounds=1, iterations=1
        )

    def _benchmark_checkout(*args, **kwargs):
        return benchmark.pedantic(
            orig_checkout_func, args=args, kwargs=kwargs, rounds=1, iterations=1
        )

    if profile == "fast_import":
        monkeypatch.setattr(init, "fast_import_tables", _benchmark_import)
    else:
        monkeypatch.setattr(init, "_add_datasets_to_working_copy", _benchmark_checkout)

    with data_archive(archive) as data:
        # list tables
        repo_path = tmp_path / "repo"
        repo_path.mkdir()

        with Db_GPKG.create_engine(data / source_gpkg).connect() as conn:
            if param_ids[-1] == "empty":
                print(f"emptying table {table}...")
                conn.execute(f"DELETE FROM {table};")

            num_rows = conn.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
        benchmark.group = f"test_import - {param_ids[-1]} (N={num_rows})"

        if param_ids[-1] == "empty":
            assert num_rows == 0

        with chdir(repo_path):
            r = cli_runner.invoke(["init"])
            assert r.exit_code == 0, r

            repo = KartRepo(repo_path)
            assert repo.is_empty

            r = cli_runner.invoke(["import", str(data / source_gpkg), table])
            assert r.exit_code == 0, r

            assert not repo.is_empty
            assert repo.head.name == "refs/heads/main"
            assert repo.head.shorthand == "main"

            # has a single commit
            assert len(list(repo.walk(repo.head.target))) == 1

            dataset = _import_check(repo_path, table, f"{data / source_gpkg}")

            with Db_GPKG.create_engine(data / source_gpkg).connect() as conn:
                pk_field = Db_GPKG.pk_name(conn, table=table)

                if num_rows > 0:
                    # compare the first feature in the repo against the source DB
                    feature = next(dataset.features())

                    row = normalise_feature(
                        conn.execute(
                            f"SELECT * FROM {table} WHERE {pk_field}=?;",
                            [feature[pk_field]],
                        ).fetchone()
                    )
                    feature = normalise_feature(feature)
                    print("First Feature:", feature, row)
                    assert feature == row

                    # compare a source DB feature against the repo feature
                    row = normalise_feature(
                        conn.execute(
                            f"SELECT * FROM {table} ORDER BY {pk_field} LIMIT 1 OFFSET {min(97,num_rows-1)};"
                        ).fetchone()
                    )

                    for feature in dataset.features():
                        if feature[pk_field] == row[pk_field]:
                            feature = normalise_feature(feature)
                            assert feature == row
                            break
                    else:
                        pytest.fail(
                            f"Couldn't find repo feature {pk_field}={row[pk_field]}"
                        )


def _compare_ogr_and_gpkg_meta_items(dataset, gpkg_dataset):
    """
    Compares the meta items from an OGR import, with those from a GPKG import.
    There are all sorts of caveats to the meta item emulation, and this attempts
    to avoid them when comparing.
    """
    ds_schema = without_ids(dataset.get_meta_item("schema.json"))
    gpkg_schema = without_ids(gpkg_dataset.get_meta_item("schema.json"))

    # SHP/TAB always call the primary key "FID"
    for col in gpkg_schema:
        if col["name"] == gpkg_dataset.primary_key:
            col["name"] = "FID"

    # Check the fields are in the right order. Ignore truncation and capitalisation
    ds_names = [col["name"][:8] for col in ds_schema]
    gpkg_names = [col["name"][:8] for col in gpkg_schema]
    assert ds_names == gpkg_names

    # now we've checked order, we can key by name
    ds_schema = {col.pop("name"): col for col in ds_schema}
    gpkg_schema = {col.pop("name"): col for col in gpkg_schema}

    # SHP/TAB field names must be <10 chars
    # When they're truncated, an underscore and integer suffix gets appended.
    # so the fieldname itself is truncated to 8 chars.
    remove_prefixes = set()
    for k in list(gpkg_schema.keys()):
        if k not in gpkg_schema:
            # already removed
            continue
        if len(k) > 10:
            remove_prefixes.add(k[:8])

    remove_prefixes = tuple(remove_prefixes)
    for k in list(gpkg_schema.keys()):
        if k.startswith(remove_prefixes):
            gpkg_schema.pop(k)
    for k in list(ds_schema.keys()):
        if k.startswith(remove_prefixes):
            ds_schema.pop(k)


@pytest.mark.slow
@pytest.mark.parametrize(
    "archive,source_gpkg,table",
    [
        pytest.param(
            "gpkg-points", "nz-pa-points-topo-150k.gpkg", H.POINTS.LAYER, id="points"
        ),
        pytest.param(
            "gpkg-polygons",
            "nz-waca-adjustments.gpkg",
            H.POLYGONS.LAYER,
            id="polygons",
        ),
        pytest.param(
            "gpkg-points", "nz-pa-points-topo-150k.gpkg", H.POINTS.LAYER, id="empty"
        ),
    ],
)
@pytest.mark.parametrize(
    "source_format,source_ogr_driver",
    [
        ("SHP", "ESRI Shapefile"),
        # https://github.com/koordinates/kart/issues/86
        # This test starts by converting a GPKG into a TAB, and then imports then TAB.
        # But the TAB ended up with very broken SRS info, and then during import GDAL
        # failed to find an EPSG code for the projection.
        # We can't currently work around this so we're disabling it.
        # A future release might add handling via an option (--srs=epsg:4167 for example)
        # ('TAB', 'MapInfo File')
    ],
    ids=["SHP"],
)
def test_import_from_non_gpkg(
    archive,
    source_gpkg,
    table,
    data_archive,
    tmp_path,
    cli_runner,
    chdir,
    request,
    source_format,
    source_ogr_driver,
):
    """
    Import something else into a Kart repository.
    """
    param_ids = H.parameter_ids(request)

    with data_archive(archive) as data:
        with Db_GPKG.create_engine(data / source_gpkg).connect() as conn:
            if param_ids[-1] == "empty":
                print(f"emptying table {table}...")
                conn.execute(f"DELETE FROM {table};")

            num_rows = conn.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]

        if param_ids[-1] == "empty":
            assert num_rows == 0

        # First, import the original GPKG to one repo
        gpkg_repo_path = tmp_path / "gpkg"
        gpkg_repo_path.mkdir()
        with chdir(gpkg_repo_path):
            r = cli_runner.invoke(["init"])
            assert r.exit_code == 0, r
            r = cli_runner.invoke(["import", data / source_gpkg, table])
            assert r.exit_code == 0, r

        gpkg_repo = KartRepo(gpkg_repo_path)
        gpkg_dataset = gpkg_repo.datasets()[table]

        # convert to a new format using OGR
        source_filename = tmp_path / f"data.{source_format.lower()}"
        gdal.VectorTranslate(
            str(source_filename),
            gdal.OpenEx(str(data / source_gpkg)),
            format=source_ogr_driver,
            layers=[table],
        )
        repo_path = tmp_path / "non-gpkg"
        repo_path.mkdir()
        with chdir(repo_path):
            r = cli_runner.invoke(["init"])
            assert r.exit_code == 0, r

            repo = KartRepo(repo_path)
            assert repo.is_empty

            # Import from SHP/TAB/something into Kart
            r = cli_runner.invoke(
                [
                    "import",
                    str(source_filename),
                    f"data:{table}",
                ]
            )
            assert r.exit_code == 0, r

            assert not repo.is_empty
            assert repo.head.name == "refs/heads/main"
            assert repo.head.shorthand == "main"

            # has a single commit
            assert len([c for c in repo.walk(repo.head.target)]) == 1

            dataset = _import_check(repo_path, table, f"{data / source_gpkg}")

            # Compare the meta items to the GPKG-imported ones
            repo = KartRepo(repo_path)
            dataset = repo.datasets()[table]

            _compare_ogr_and_gpkg_meta_items(dataset, gpkg_dataset)

            if num_rows > 0:
                # compare the first feature in the repo against the source DB
                got_feature = next(dataset.features())
                pk = got_feature[dataset.primary_key]

                src_ds = ogr.Open(str(source_filename))
                src_layer = src_ds.GetLayer(0)
                assert src_layer.GetFeatureCount() == num_rows

                f = src_layer.GetFeature(pk)
                expected_feature = {
                    f.GetFieldDefnRef(i).GetName(): f.GetField(i)
                    for i in range(f.GetFieldCount())
                }
                if "date_adjus" in expected_feature:
                    expected_feature["date_adjus"] = expected_feature[
                        "date_adjus"
                    ].replace("/", "-")
                expected_feature["FID"] = f.GetFID()
                if src_layer.GetGeomType() != ogr.wkbNone:
                    g = f.GetGeometryRef()
                    if g:
                        g.AssignSpatialReference(src_layer.GetSpatialRef())
                    expected_feature["geom"] = ogr_to_gpkg_geom(g)

                assert normalise_feature(got_feature) == expected_feature


def test_shp_import_meta(
    data_archive,
    tmp_path,
    cli_runner,
    request,
):
    with data_archive("gpkg-polygons") as data:
        # convert to SHP using OGR
        source_filename = tmp_path / "nz_waca_adjustments.shp"
        gdal.VectorTranslate(
            str(source_filename),
            gdal.OpenEx(str(data / "nz-waca-adjustments.gpkg")),
            format="ESRI Shapefile",
            layers=["nz_waca_adjustments"],
        )

        # now import the SHP
        repo_path = tmp_path / "repo"
        r = cli_runner.invoke(["init", "--import", source_filename, str(repo_path)])
        assert r.exit_code == 0, r

        # now check metadata
        path = "nz_waca_adjustments"
        repo = KartRepo(repo_path)
        dataset = repo.datasets()[path]

        meta_items = dict(dataset.meta_items())
        assert set(meta_items) == {
            "schema.json",
            "crs/EPSG:4167.wkt",
        }
        schema = without_ids(dataset.get_meta_item("schema.json"))
        assert schema == [
            {"name": "FID", "dataType": "integer", "primaryKeyIndex": 0, "size": 64},
            {
                "name": "geom",
                "dataType": "geometry",
                "geometryType": "POLYGON",
                "geometryCRS": "EPSG:4167",
            },
            {"name": "date_adjus", "dataType": "date"},
            {"name": "survey_ref", "dataType": "text", "length": 50},
            {
                "name": "adjusted_n",
                "dataType": "integer",
                "size": 32,
            },
        ]


def quote_ident(part):
    """
    SQL92 conformant identifier quoting, for use with OGR-dialect SQL
    (and most other dialects)
    """
    part = part.replace('"', '""')
    return f'"{part}"'


@pytest.fixture()
def postgis_layer(postgis_db, data_archive):
    postgres_conn_str = postgres_url_to_ogr_conn_str(os.environ["KART_POSTGRES_URL"])

    @contextlib.contextmanager
    def _postgis_layer(archive_name, gpkg_name, table):
        with data_archive(archive_name) as data:
            src_ds = gdal.OpenEx(str(data / gpkg_name), gdal.OF_VERBOSE_ERROR)
            dest_ds = gdal.OpenEx(
                postgres_conn_str,
                gdal.OF_VERBOSE_ERROR | gdal.OF_UPDATE,
                ["PostgreSQL"],
            )

            gdal.VectorTranslate(
                dest_ds,
                src_ds,
                format="PostgreSQL",
                accessMode="overwrite",
                layerCreationOptions=["LAUNDER=NO"],
                layers=[table],
            )
        yield
        with postgis_db.connect() as conn:
            conn.execute(f"""DROP TABLE IF EXISTS {quote_ident(table)} CASCADE;""")

    return _postgis_layer


def test_postgres_preserves_float_precision(postgis_db):
    with postgis_db.connect() as conn:
        val = conn.scalar("SHOW extra_float_digits")
        assert val == "3"
        val = conn.scalar("SELECT 1060116.12::real")
        assert val == 1060116.1


def _test_postgis_import(
    repo_path,
    cli_runner,
    chdir,
    *,
    table_name,
    pk_name="id",
    pk_size=64,
    import_args=(),
):
    r = cli_runner.invoke(["init", repo_path])
    assert r.exit_code == 0, r
    with chdir(repo_path):
        r = cli_runner.invoke(
            [
                "import",
                os.environ["KART_POSTGRES_URL"],
                table_name,
                *import_args,
            ]
        )
        assert r.exit_code == 0, r
    # now check metadata
    repo = KartRepo(repo_path)
    dataset = repo.datasets()[table_name]

    meta_items = dict(dataset.meta_items())
    meta_item_keys = set(meta_items.keys())
    assert "schema.json" in meta_item_keys
    crs_keys = meta_item_keys - {"title", "description", "schema.json"}
    assert len(crs_keys) == 1
    crs_key = next(iter(crs_keys))
    assert crs_key.startswith("crs/EPSG:") and crs_key.endswith(".wkt")


def test_postgis_import(
    postgis_layer,
    data_archive,
    tmp_path,
    cli_runner,
    request,
    chdir,
):
    with postgis_layer(
        "gpkg-polygons", "nz-waca-adjustments.gpkg", "nz_waca_adjustments"
    ):
        _test_postgis_import(
            tmp_path / "repo", cli_runner, chdir, table_name="nz_waca_adjustments"
        )


def test_postgis_import_from_view(
    postgis_db,
    postgis_layer,
    data_archive,
    tmp_path,
    cli_runner,
    request,
    chdir,
):
    with postgis_layer(
        "gpkg-polygons", "nz-waca-adjustments.gpkg", "nz_waca_adjustments"
    ):
        with postgis_db.connect() as conn:
            conn.execute(
                """
                CREATE OR REPLACE VIEW nz_waca_adjustments_view AS (
                    SELECT * FROM nz_waca_adjustments
                );
                """
            )
        _test_postgis_import(
            tmp_path / "repo",
            cli_runner,
            chdir,
            table_name="nz_waca_adjustments_view",
            pk_name="id",
            pk_size=32,
            import_args=["--primary-key=id"],
        )


def test_postgis_import_from_view_with_ogc_fid(
    postgis_db,
    postgis_layer,
    data_archive,
    tmp_path,
    cli_runner,
    request,
    chdir,
):
    with postgis_layer(
        "gpkg-polygons", "nz-waca-adjustments.gpkg", "nz_waca_adjustments"
    ):
        with postgis_db.connect() as conn:
            conn.execute(
                """
                CREATE OR REPLACE VIEW nz_waca_adjustments_view AS (
                    SELECT id AS ogc_fid, date_adjusted, survey_reference, adjusted_nodes, geom
                    FROM nz_waca_adjustments
                );
                """
            )
        _test_postgis_import(
            tmp_path / "repo",
            cli_runner,
            chdir,
            table_name="nz_waca_adjustments_view",
            pk_name="ogc_fid",
            import_args=["--primary-key=ogc_fid"],
        )


def test_postgis_import_from_view_no_pk(
    postgis_db,
    postgis_layer,
    data_archive,
    tmp_path,
    cli_runner,
    request,
    chdir,
):
    repo_path = tmp_path / "repo"
    with postgis_layer(
        "gpkg-points", "nz-pa-points-topo-150k.gpkg", "nz_pa_points_topo_150k"
    ):
        with postgis_db.connect() as conn:
            conn.execute(
                """
                CREATE OR REPLACE VIEW nz_pa_points_view AS (
                    SELECT geom, t50_fid, name_ascii, macronated, name
                    FROM nz_pa_points_topo_150k
                    WHERE fid %% 3 != 0
                );
                """
            )
        _test_postgis_import(
            repo_path,
            cli_runner,
            chdir,
            table_name="nz_pa_points_view",
            pk_name="auto_pk",
        )

        repo = KartRepo(repo_path)
        dataset = repo.datasets()["nz_pa_points_view"]
        initial_pks = [f["auto_pk"] for f in dataset.features()]
        assert len(initial_pks) == 1429
        assert max(initial_pks) == 1429
        assert sorted(initial_pks) == list(range(1, 1429 + 1))

        with postgis_db.connect() as conn:
            conn.execute("DROP VIEW IF EXISTS nz_pa_points_view;")
            conn.execute(
                """
                CREATE OR REPLACE VIEW nz_pa_points_view AS (
                    SELECT geom, t50_fid, name_ascii, macronated, name
                    FROM nz_pa_points_topo_150k
                    WHERE fid %% 3 != 1
                );
                """
            )

        r = cli_runner.invoke(
            [
                "--repo",
                str(repo_path.resolve()),
                "import",
                os.environ["KART_POSTGRES_URL"],
                "nz_pa_points_view",
                "--replace-existing",
            ]
        )
        assert r.exit_code == 0, r.stderr
        repo = KartRepo(repo_path)
        dataset = repo.datasets()["nz_pa_points_view"]
        new_pks = [f["auto_pk"] for f in dataset.features()]

        assert len(new_pks) == 1428
        assert max(new_pks) == 2143
        assert len(set(initial_pks) & set(new_pks)) == 714
        # 2143 features total - but 1429 are in the first group and 1428 are in the second group
        # Means 714 features are in both, and should be imported with the same PK both times
        # 1429 + 1428 is 2857, which is 714 more features than the actual total of 2143

        with postgis_db.connect() as conn:
            # This is similar enough to be detected as an edit - only one field is different.
            conn.execute(
                "UPDATE nz_pa_points_topo_150k SET name_ascii='foo' WHERE fid=3;"
            )
            # This is similar enough to be detected as an edit - only one field is different.
            conn.execute("UPDATE nz_pa_points_topo_150k SET name='qux' WHERE fid=6;")
            # This will not be detected as an edit - two fields are different,
            # so it looks like one feature is deleted and a different one is inserted.
            conn.execute(
                "UPDATE nz_pa_points_topo_150k SET name_ascii='bar', name='baz' WHERE fid=9;"
            )
            conn.execute("DROP VIEW IF EXISTS nz_pa_points_view;")
            conn.execute(
                """
                CREATE OR REPLACE VIEW nz_pa_points_view AS (
                    SELECT geom, t50_fid, name_ascii, macronated, name
                    FROM nz_pa_points_topo_150k
                    WHERE fid %% 3 != 2
                );
                """
            )

        r = cli_runner.invoke(
            [
                "--repo",
                str(repo_path.resolve()),
                "import",
                os.environ["KART_POSTGRES_URL"],
                "nz_pa_points_view",
                "--replace-existing",
            ]
        )
        assert r.exit_code == 0, r.stderr
        r = cli_runner.invoke(["--repo", str(repo_path.resolve()), "show"])
        assert r.exit_code == 0, r.stderr

        output = r.stdout.splitlines()
        # Huge amount of adds and deletes caused by changing which features are included in the view again:
        assert len(output) == 10031

        # But, we still are able to recognise the edits we made as edits.
        # (For happy mathematical reasons, these diffs end up at the end of the output)
        assert output[-22:] == [
            # Edit: name_ascii changed to foo
            "--- nz_pa_points_view:feature:1430",
            "+++ nz_pa_points_view:feature:1430",
            "-                               name_ascii = Tauwhare Pa",
            "+                               name_ascii = foo",
            # Edit: name changed to qux
            "--- nz_pa_points_view:feature:1431",
            "+++ nz_pa_points_view:feature:1431",
            "-                                     name = ␀",
            "+                                     name = qux",
            # Not considered an edit - both name_ascii and name changed
            # So, left as a delete + insert, and assigned a new PK
            "--- nz_pa_points_view:feature:1432",
            "-                                  auto_pk = 1432",
            "-                                     geom = POINT(...)",
            "-                                  t50_fid = 2426279",
            "-                               name_ascii = ␀",
            "-                               macronated = N",
            "-                                     name = ␀",
            "+++ nz_pa_points_view:feature:2144",
            "+                                  auto_pk = 2144",
            "+                                     geom = POINT(...)",
            "+                                  t50_fid = 2426279",
            "+                               name_ascii = bar",
            "+                               macronated = N",
            "+                                     name = baz",
        ]


def test_import_no_pk_performance(data_archive_readonly, benchmark):
    with data_archive_readonly("points") as repo_path:
        repo = KartRepo(repo_path)
        dataset = repo.datasets()["nz_pa_points_topo_150k"]
        dataset.meta_overrides = {}

        features = list(dataset.features())
        assert len(features) == 2143
        old_features = features[0:1000]
        new_features = features[1000:2143]

        pkis = PkGeneratingImportSource(dataset, repo)
        pkis.prev_dest_schema = dataset.schema
        pkis.primary_key = dataset.primary_key

        def _match_features_benchmark():
            # Exhaust generator:
            for _ in pkis._match_similar_features_and_remove(
                old_features, new_features
            ):
                pass

        benchmark(_match_features_benchmark)


def test_postgis_import_replace_no_ids(
    postgis_db,
    postgis_layer,
    data_archive,
    tmp_path,
    cli_runner,
    request,
    chdir,
):
    repo_path = tmp_path / "repo"
    with postgis_layer(
        "gpkg-polygons", "nz-waca-adjustments.gpkg", "nz_waca_adjustments"
    ):
        with postgis_db.connect() as conn:
            conn.execute(
                """
                CREATE OR REPLACE VIEW nz_waca_adjustments_view AS (
                    SELECT date_adjusted, survey_reference, adjusted_nodes, geom
                    FROM nz_waca_adjustments
                    WHERE id %% 3 != 0
                );
                """
            )
        _test_postgis_import(
            repo_path,
            cli_runner,
            chdir,
            table_name="nz_waca_adjustments_view",
            pk_name="auto_pk",
        )

        r = cli_runner.invoke(
            [
                "--repo",
                str(repo_path.resolve()),
                "import",
                os.environ["KART_POSTGRES_URL"],
                "nz_waca_adjustments_view",
                "--replace-ids=",
            ]
        )
        assert r.exit_code == 44, r.stderr
        r = cli_runner.invoke(
            [
                "--repo",
                str(repo_path.resolve()),
                "import",
                os.environ["KART_POSTGRES_URL"],
                "nz_waca_adjustments_view",
                "--replace-ids=",
                # add some meta info so it's not a complete noop
                '--table-info={"nz_waca_adjustments_view": {"title": "New title"}}',
            ]
        )
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["--repo", str(repo_path.resolve()), "show"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines()[-2:] == [
            "+++ nz_waca_adjustments_view:meta:title",
            "+ New title",
        ]


def test_pk_encoder_legacy_hashed(data_archive_readonly):
    archive_path = Path("upgrade") / "v2.kart" / "points.tgz"
    with data_archive_readonly(archive_path) as repo_path:
        repo = KartRepo(repo_path)
        ds = repo.datasets()["nz_pa_points_topo_150k"]
        e = ds.feature_path_encoder
        assert isinstance(e, MsgpackHashPathEncoder)
        assert e.encoding == "hex"
        assert e.branches == 256
        assert e.levels == 2
        assert (
            ds.encode_1pk_to_path(1181)
            == "nz_pa_points_topo_150k/.sno-dataset/feature/7b/36/kc0EnQ=="
        )
        assert (
            ds.encode_1pk_to_path("Dave")
            == "nz_pa_points_topo_150k/.sno-dataset/feature/b2/fe/kaREYXZl"
        )


def test_pk_encoder_string_pk():
    schema = Schema.from_column_dicts(
        [{"name": "mypk", "dataType": "text", "id": "abc123"}]
    )
    ds = Dataset3.new_dataset_for_writing("mytable", schema)
    e = ds.feature_path_encoder
    assert isinstance(e, MsgpackHashPathEncoder)
    assert e.encoding == "base64"
    assert e.branches == 64
    assert e.levels == 4
    assert ds.encode_1pk_to_path("") == "mytable/.table-dataset/feature/I/6/M/_/kaA="
    assert (
        ds.encode_1pk_to_path("Dave")
        == "mytable/.table-dataset/feature/s/v/7/j/kaREYXZl"
    )


def test_pk_encoder_int_pk():
    schema = Schema.from_column_dicts(
        [
            {
                "name": "mypk",
                "dataType": "integer",
                "size": 64,
                "id": "abc123",
                "primaryKeyIndex": 0,
            }
        ]
    )
    ds = Dataset3.new_dataset_for_writing("mytable", schema)
    e = ds.feature_path_encoder
    assert isinstance(e, IntPathEncoder)
    assert e.encoding == "base64"
    assert e.branches == 64
    assert e.levels == 4

    with pytest.raises(TypeError):
        ds.encode_1pk_to_path("Dave")
    with pytest.raises(TypeError):
        ds.encode_1pk_to_path(0.1)

    assert ds.encode_1pk_to_path(0) == "mytable/.table-dataset/feature/A/A/A/A/kQA="
    assert ds.encode_1pk_to_path(1) == "mytable/.table-dataset/feature/A/A/A/A/kQE="
    assert ds.encode_1pk_to_path(-1) == "mytable/.table-dataset/feature/_/_/_/_/kf8="
    assert (
        ds.encode_1pk_to_path(1181) == "mytable/.table-dataset/feature/A/A/A/S/kc0EnQ=="
    )
    # trees hit wraparound with large PKs, but don't break
    assert (
        ds.encode_1pk_to_path(64 ** 5)
        == "mytable/.table-dataset/feature/A/A/A/A/kc5AAAAA"
    )
    assert (
        ds.encode_1pk_to_path(-(64 ** 5))
        == "mytable/.table-dataset/feature/A/A/A/A/kdLAAAAA"
    )


@pytest.mark.slow
@pytest.mark.parametrize(*GPKG_IMPORTS)
@pytest.mark.parametrize("profile", ["get_feature_by_pk", "get_feature_from_data"])
def test_feature_find_decode_performance(
    profile,
    archive,
    source_gpkg,
    table,
    data_archive,
    data_imported,
    benchmark,
    request,
):
    """ Check single-feature decoding performance """
    param_ids = H.parameter_ids(request)
    benchmark.group = (
        f"test_feature_find_decode_performance - {profile} - {param_ids[-1]}"
    )

    repo_path = data_imported(archive, source_gpkg, table)
    repo = KartRepo(repo_path)
    dataset = repo.datasets()["mytable"]
    inner_tree = dataset.inner_tree

    with data_archive(archive) as data:
        with Db_GPKG.create_engine(data / source_gpkg).connect() as conn:
            num_rows = conn.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
            pk_field = Db_GPKG.pk_name(conn, table=table)
            pk = conn.execute(
                f"SELECT {pk_field} FROM {table} ORDER BY {pk_field} LIMIT 1 OFFSET {min(97,num_rows-1)};"
            ).fetchone()[0]

    if profile == "get_feature_by_pk":
        benchmark(dataset.get_feature, pk)

    elif profile == "get_feature_from_data":
        feature_path = dataset.encode_1pk_to_path(pk, relative=True)
        feature_data = memoryview(inner_tree / feature_path)

        benchmark(dataset.get_feature, path=feature_path, data=feature_data)
    else:
        raise NotImplementedError(f"Unknown profile: {profile}")


@pytest.mark.slow
def test_import_multiple(data_archive, chdir, cli_runner, tmp_path):
    repo_path = tmp_path / "repo"
    repo_path.mkdir()

    with chdir(repo_path):
        r = cli_runner.invoke(["init"])
        assert r.exit_code == 0, r

    repo = KartRepo(repo_path)
    assert repo.is_empty

    LAYERS = (
        ("gpkg-points", "nz-pa-points-topo-150k.gpkg", H.POINTS.LAYER),
        ("gpkg-polygons", "nz-waca-adjustments.gpkg", H.POLYGONS.LAYER),
    )

    datasets = []
    for i, (archive, source_gpkg, table) in enumerate(LAYERS):
        with data_archive(archive) as data:
            with chdir(repo_path):
                r = cli_runner.invoke(["import", f"GPKG:{data / source_gpkg}", table])
                assert r.exit_code == 0, r

                datasets.append(
                    _import_check(
                        repo_path,
                        table,
                        f"{data / source_gpkg}",
                    )
                )

                assert len([c for c in repo.walk(repo.head.target)]) == i + 1

                if i + 1 == len(LAYERS):
                    r = cli_runner.invoke(
                        ["import", f"GPKG:{data / source_gpkg}", table]
                    )
                    assert r.exit_code == INVALID_OPERATION

    # has two commits
    assert len([c for c in repo.walk(repo.head.target)]) == len(LAYERS)

    tree = repo.head_tree

    for i, ds in enumerate(datasets):
        assert ds.path == LAYERS[i][2]

        feature = next(ds.features())
        f_path = ds.encode_1pk_to_path(feature[ds.primary_key])
        assert tree / f_path


def test_import_into_empty_branch(data_archive, cli_runner, chdir, tmp_path):
    repo_path = tmp_path / "repo"
    repo_path.mkdir()

    r = cli_runner.invoke(["init", "--bare", repo_path])
    assert r.exit_code == 0

    with data_archive("gpkg-points") as data:
        with chdir(repo_path):
            r = cli_runner.invoke(["import", data / "nz-pa-points-topo-150k.gpkg"])
            assert r.exit_code == 0, r

            # delete the main branch.
            # HEAD still points to it, but that's okay - this just means
            # the branch is empty.
            # We still need to be able to import from this state.
            repo = KartRepo(repo_path)
            repo.references.delete("refs/heads/main")
            assert repo.head_is_unborn

            r = cli_runner.invoke(["import", data / "nz-pa-points-topo-150k.gpkg"])
            assert r.exit_code == 0, r

            repo = KartRepo(repo_path)
            assert repo.head_commit


@pytest.mark.slow
@pytest.mark.parametrize(*GPKG_IMPORTS)
def test_write_feature_performance(
    archive,
    source_gpkg,
    table,
    data_archive,
    tmp_path,
    cli_runner,
    chdir,
    benchmark,
    request,
):
    """ Per-feature import performance. """
    param_ids = H.parameter_ids(request)

    with data_archive(archive) as data:
        # list tables
        repo_path = tmp_path / "repo"
        repo_path.mkdir()

        benchmark.group = f"test_write_feature_performance - {param_ids[-1]}"

        with chdir(repo_path):
            r = cli_runner.invoke(["init"])
            assert r.exit_code == 0, r

            repo = KartRepo(repo_path)

            source = ImportSource.open(data / source_gpkg, table=table)
            with source:
                dataset = Dataset3.new_dataset_for_writing(table, source.schema)
                feature_iter = itertools.cycle(list(source.features()))

                index = pygit2.Index()

                encode_kwargs = {"schema": source.schema}

                def _write_feature():
                    feature = next(feature_iter)
                    dest_path, dest_data = dataset.encode_feature(
                        feature, **encode_kwargs
                    )
                    blob_id = repo.create_blob(dest_data)
                    entry = pygit2.IndexEntry(
                        f"{dataset.path}/{dest_path}", blob_id, pygit2.GIT_FILEMODE_BLOB
                    )
                    index.add(entry)

                benchmark(_write_feature)


@pytest.mark.slow
def test_fast_import(data_archive, tmp_path, cli_runner, chdir):
    table = H.POINTS.LAYER
    with data_archive("gpkg-points") as data:
        # list tables
        repo_path = tmp_path / "repo"
        repo_path.mkdir()

        with chdir(repo_path):
            r = cli_runner.invoke(["init"])
            assert r.exit_code == 0, r

            repo = KartRepo(repo_path)

            source = ImportSource.open(
                data / "nz-pa-points-topo-150k.gpkg", table=table
            )

            fast_import.fast_import_tables(repo, [source], from_commit=None)

            assert not repo.is_empty
            assert repo.head.name == "refs/heads/main"
            assert repo.head.shorthand == "main"

            dataset = repo.datasets()[table]
            assert dataset.VERSION == 3

            # has a single commit
            assert len([c for c in repo.walk(repo.head.target)]) == 1
            assert list(dataset.meta_items())

            # has the right number of features
            feature_count = sum(1 for f in dataset.features())
            assert feature_count == source.feature_count


def test_postgis_import_with_sampled_geometry_dimension(
    postgis_db,
    data_archive,
    tmp_path,
    cli_runner,
    request,
    chdir,
):
    with postgis_db.connect() as conn:
        conn.execute("""DROP TABLE IF EXISTS points_xyz CASCADE;""")
        conn.execute(
            """CREATE TABLE points_xyz (fid BIGINT PRIMARY KEY, shape GEOMETRY);"""
        )
        conn.execute(
            """INSERT INTO points_xyz (fid, shape) VALUES (1, ST_GeomFromText('POINT(1 2 3)', 4326));"""
        )

        _test_postgis_import(
            tmp_path / "repo",
            cli_runner,
            chdir,
            table_name="points_xyz",
            pk_name="fid",
            pk_size=64,
            import_args=["--primary-key=fid"],
        )

        repo = KartRepo(tmp_path / "repo")
        dataset = repo.datasets()["points_xyz"]
        [geom_col] = dataset.schema.geometry_columns
        assert geom_col.extra_type_info["geometryType"] == "GEOMETRY Z"

        conn.execute("""DROP TABLE IF EXISTS points_xyz CASCADE;""")
