import contextlib
import itertools
import os
import re
import subprocess

from osgeo import gdal, ogr

import psycopg2
import pygit2
import pytest

from sno import gpkg, structure, fast_import
from sno.init import OgrImporter, ImportPostgreSQL
from sno.dataset1 import Dataset1


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
            id="polygons-pk",
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

DATASET_VERSIONS = (
    "import_version",
    structure.DatasetStructure.version_numbers(),
)


def test_dataset_versions():
    assert structure.DatasetStructure.version_numbers() == ("1.0", "2.0")
    klasses = structure.DatasetStructure.all_versions()
    assert set(klass.VERSION_IMPORT for klass in klasses) == set(
        structure.DatasetStructure.version_numbers()
    )


def _import_check(repo_path, table, source_gpkg, geopackage):
    repo = pygit2.Repository(str(repo_path))
    tree = repo.head.peel(pygit2.Tree) / table

    dataset = structure.DatasetStructure.instantiate(tree, table)

    db = geopackage(source_gpkg)
    num_rows = db.cursor().execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]

    o = subprocess.check_output(["git", "ls-tree", "-r", "-t", "HEAD", table])
    print("\n".join(l.decode("utf8") for l in o.splitlines()[:20]))

    if dataset.version.startswith("1."):
        re_paths = (
            r"^\d{6} blob [0-9a-f]{40}\t%s/.sno-table/[0-9a-f]{2}/[0-9a-f]{2}/([^/]+)$"
            % table
        )
    elif dataset.version.startswith("2."):
        re_paths = r"^\d{6} blob [0-9a-f]{40}\t%s/.sno-table/feature/.*$" % table
    else:
        raise NotImplementedError(dataset.version)

    git_paths = [m for m in re.findall(re_paths, o.decode("utf-8"), re.MULTILINE)]
    assert len(git_paths) == num_rows

    num_features = sum(1 for _ in dataset.features())
    assert num_features == num_rows

    return dataset


def normalise_feature(row):
    row = dict(row)
    if 'geom' in row:
        # We import via OGR, which strips envelopes by default
        row['geom'] = gpkg.ogr_to_gpkg_geom(
            gpkg.gpkg_geom_to_ogr(row['geom'], parse_srs=True),
        )
    return row


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
@pytest.mark.parametrize(*DATASET_VERSIONS)
def test_import(
    import_version,
    archive,
    source_gpkg,
    table,
    data_archive,
    tmp_path,
    cli_runner,
    chdir,
    geopackage,
    benchmark,
    request,
    monkeypatch,
):
    """ Import the GeoPackage (eg. `kx-foo-layer.gpkg`) into a Sno repository. """
    param_ids = H.parameter_ids(request)

    # wrap the fast_import_tables function with benchmarking
    orig_import_func = fast_import.fast_import_tables

    def _benchmark_import(*args, **kwargs):
        # one round/iteration isn't very statistical, but hopefully crude idea
        return benchmark.pedantic(
            orig_import_func, args=args, kwargs=kwargs, rounds=1, iterations=1
        )

    monkeypatch.setattr(fast_import, 'fast_import_tables', _benchmark_import)

    with data_archive(archive) as data:
        # list tables
        repo_path = tmp_path / "data.sno"
        repo_path.mkdir()

        db = geopackage(f"{data / source_gpkg}")
        dbcur = db.cursor()
        if param_ids[-1] == "empty":
            with db:
                print(f"emptying table {table}...")
                dbcur.execute(f"DELETE FROM {table};")

        num_rows = dbcur.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
        benchmark.group = f"test_import - {param_ids[-1]} (N={num_rows})"

        if param_ids[-1] == "empty":
            assert num_rows == 0

        with chdir(repo_path):
            r = cli_runner.invoke(["init"])
            assert r.exit_code == 0, r

            repo = pygit2.Repository(str(repo_path))
            assert repo.is_bare
            assert repo.is_empty

            r = cli_runner.invoke(
                [
                    "import",
                    str(data / source_gpkg),
                    f"--version={import_version}",
                    table,
                ]
            )
            assert r.exit_code == 0, r

            assert not repo.is_empty
            assert repo.head.name == "refs/heads/master"
            assert repo.head.shorthand == "master"

            # has a single commit
            assert len(list(repo.walk(repo.head.target))) == 1

            dataset = _import_check(
                repo_path, table, f"{data / source_gpkg}", geopackage
            )

            assert dataset.__class__.__name__ == f"Dataset{import_version[0]}"
            assert dataset.version == import_version

            pk_field = gpkg.pk(db, table)

            # pk_list = sorted([v[pk_field] for k, v in dataset.features()])
            # pk_gaps = sorted(set(range(pk_list[0], pk_list[-1] + 1)).difference(pk_list))
            # print("pk_gaps:", pk_gaps)

            if num_rows > 0:
                # compare the first feature in the repo against the source DB
                key, feature = next(dataset.features())

                row = normalise_feature(
                    dbcur.execute(
                        f"SELECT * FROM {table} WHERE {pk_field}=?;",
                        [feature[pk_field]],
                    ).fetchone()
                )
                feature = normalise_feature(feature)
                print("First Feature:", key, feature, row)
                assert feature == row

                # compare a source DB feature against the repo feature
                row = normalise_feature(
                    dbcur.execute(
                        f"SELECT * FROM {table} ORDER BY {pk_field} LIMIT 1 OFFSET {min(97,num_rows-1)};"
                    ).fetchone()
                )

                for key, feature in dataset.features():
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
    meta_items = dict(dataset.iter_meta_items())
    gpkg_meta_items = dict(gpkg_dataset.iter_meta_items())

    # we don't implement XML metadata for non-gpkg formats
    del gpkg_meta_items['gpkg_metadata']
    del gpkg_meta_items['gpkg_metadata_reference']
    # SHP/TAB always call the primary key "FID"
    gpkg_meta_items[f"fields/{dataset.primary_key}"] = gpkg_meta_items.pop(
        f"fields/{gpkg_dataset.primary_key}"
    )
    gpkg_meta_items["primary_key"] = meta_items["primary_key"]

    # SHP/TAB field names must be <10 chars
    nuke_truncated_fields = set()
    for f in meta_items.keys():
        if f.startswith('fields/') and len(f) == 17:
            # It's likely OGR has truncated this field name.
            # Truncate the gpkg field to match.
            match = re.match(r'^fields/(?P<fieldname>\w+?)(?:_?\d+)?$', f)
            assert match
            nuke_truncated_fields.add(match.groupdict()["fieldname"])

    # nuke difficult field names as mentioned above
    nuke_truncated_fields = tuple(nuke_truncated_fields)
    for d in (meta_items, gpkg_meta_items):
        for k in list(d):
            if k.startswith(tuple(f'fields/{f}' for f in nuke_truncated_fields)):
                d.pop(k)
        for f in d['sqlite_table_info']:
            if f['name'].startswith(nuke_truncated_fields):
                # truncated them here so we can test the rest of the stuff about the field
                f['name'] = f"{f['name'][:7]}[trunc]"

    assert meta_items.keys() == gpkg_meta_items.keys()
    for d in (meta_items, gpkg_meta_items):
        # SHP/TAB can't possibly preserve identifier/description, those are GPKG specific :/
        d['gpkg_contents'].pop('description')
        d['gpkg_contents'].pop('identifier')

        pk_field = [f for f in d['sqlite_table_info'] if f['pk']][0]
        # SHP/TAB always call the primary key "FID"
        pk_field['name'] = dataset.primary_key
        # SHP/TAB don't preserve nullability
        for f in d['sqlite_table_info']:
            del f['notnull']

        # OGR SRS names seem different from GPKG ones, and we don't seem to have descriptions at all.
        # Luckily these don't really matter (do they?)
        for srs in d['gpkg_spatial_ref_sys']:
            del srs['description']
            del srs['srs_name']

            # this one matters, but slight variations may not.
            # OGR adds AXIS definitions in that GPKG doesn't have. meh
            assert srs.pop('definition')

        # SHP/TAB don't preserve the MULTI-ness of their geometry type.
        # also, at least one of our test datasets has gtype=GEOMETRY in GPKG,
        # but when converted to SHP it ends up with POLYGON
        # (because it contains only polygons)
        del d['gpkg_geometry_columns']['geometry_type_name']
        for f in d['sqlite_table_info']:
            if f['type'].startswith('MULTI'):
                f['type'] = f['type'][5:]
            if f['type'] in (
                'POLYGON',
                'POINT',
                'LINESTRING',
                'GEOMETRYCOLLECTION',
                'GEOMETRY',
            ):
                # SHP/TAB formats don't preserve the ordering of the geometry column.
                f['cid'] = -1
            if f['type'] == 'DATETIME':
                # wow, SHP doesn't support datetimes(!)
                # OGR launders these to DATE
                f['type'] = 'DATE'

        # SHP/TAB formats don't preserve the ordering of the geometry column.
        # Above, we set the geometry cid to -1
        # Now we sort by cid, and then remove the cids.
        # This ensures we're testing the ordering of the other columns, but
        # means we don't care what their cids are with respect to the geometry column.
        d['sqlite_table_info'].sort(key=lambda f: f['cid'])
        for f in d['sqlite_table_info']:
            f.pop('cid')

        # the GPKG spec allows for z/m values to be set to 2,
        # which means z/m values are optional.
        # OGR importer doesn't do that, it's 0 or 1
        if d['gpkg_geometry_columns']['m'] == 2:
            d['gpkg_geometry_columns']['m'] = 0
        if d['gpkg_geometry_columns']['z'] == 2:
            d['gpkg_geometry_columns']['z'] = 0

    # That was dramatic. Whatever's left should be identical
    for key in meta_items:
        assert meta_items[key] == gpkg_meta_items[key], key


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
            id="polygons-pk",
        ),
        pytest.param(
            "gpkg-points", "nz-pa-points-topo-150k.gpkg", H.POINTS.LAYER, id="empty"
        ),
    ],
)
@pytest.mark.parametrize(*DATASET_VERSIONS)
@pytest.mark.parametrize(
    'source_format,source_ogr_driver',
    [
        ('SHP', 'ESRI Shapefile'),
        # https://github.com/koordinates/sno/issues/86
        # This test starts by converting a GPKG into a TAB, and then imports then TAB.
        # But the TAB ended up with very broken SRS info, and then during import GDAL
        # failed to find an EPSG code for the projection.
        # We can't currently work around this so we're disabling it.
        # A future release might add handling via an option (--srs=epsg:4167 for example)
        # ('TAB', 'MapInfo File')
    ],
    ids=['SHP'],
)
def test_import_from_non_gpkg(
    import_version,
    archive,
    source_gpkg,
    table,
    data_archive,
    tmp_path,
    cli_runner,
    chdir,
    geopackage,
    request,
    source_format,
    source_ogr_driver,
):
    """
    Import something else into a Sno repository.
    """
    param_ids = H.parameter_ids(request)

    with data_archive(archive) as data:
        db = geopackage(f"{data / source_gpkg}")
        dbcur = db.cursor()
        if param_ids[-1] == "empty":
            with db:
                print(f"emptying table {table}...")
                dbcur.execute(f"DELETE FROM {table};")

        num_rows = dbcur.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]

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

        gpkg_repo = pygit2.Repository(str(gpkg_repo_path))
        gpkg_tree = gpkg_repo.head.peel(pygit2.Tree) / table
        gpkg_dataset = structure.DatasetStructure.instantiate(gpkg_tree, table)

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

            repo = pygit2.Repository(str(repo_path))
            assert repo.is_bare
            assert repo.is_empty

            # Import from SHP/TAB/something into sno
            r = cli_runner.invoke(
                [
                    "import",
                    str(source_filename),
                    f"--version={import_version}",
                    f"data:{table}",
                ]
            )
            assert r.exit_code == 0, r

            assert not repo.is_empty
            assert repo.head.name == "refs/heads/master"
            assert repo.head.shorthand == "master"

            # has a single commit
            assert len([c for c in repo.walk(repo.head.target)]) == 1

            dataset = _import_check(
                repo_path, table, f"{data / source_gpkg}", geopackage
            )

            assert dataset.__class__.__name__ == f"Dataset{import_version[0]}"
            assert dataset.version == import_version

            # Compare the meta items to the GPKG-imported ones
            repo = pygit2.Repository(str(repo_path))
            tree = repo.head.peel(pygit2.Tree) / table
            dataset = structure.DatasetStructure.instantiate(tree, table)

            if import_version == "1.0":
                _compare_ogr_and_gpkg_meta_items(dataset, gpkg_dataset)
            elif import_version == "2.0":
                # TODO: Dataset2 needs to store more metadata.
                pass

            if num_rows > 0:
                # compare the first feature in the repo against the source DB
                key, got_feature = next(dataset.features())
                if import_version == "1.0":
                    fid = dataset.decode_pk(key)
                elif import_version == "2.0":
                    [fid] = dataset.decode_path_to_pk_values(key)

                src_ds = ogr.Open(str(source_filename))
                src_layer = src_ds.GetLayer(0)
                assert src_layer.GetFeatureCount() == num_rows

                f = src_layer.GetFeature(fid)
                expected_feature = {
                    f.GetFieldDefnRef(i).GetName(): f.GetField(i)
                    for i in range(f.GetFieldCount())
                }
                expected_feature['FID'] = f.GetFID()
                if src_layer.GetGeomType() != ogr.wkbNone:
                    g = f.GetGeometryRef()
                    if g:
                        g.AssignSpatialReference(src_layer.GetSpatialRef())
                    expected_feature['geom'] = gpkg.ogr_to_gpkg_geom(g)

                assert normalise_feature(got_feature) == expected_feature


def test_shp_import_meta(
    data_archive, tmp_path, cli_runner, request,
):
    with data_archive('gpkg-polygons') as data:
        # convert to SHP using OGR
        source_filename = tmp_path / "nz_waca_adjustments.shp"
        gdal.VectorTranslate(
            str(source_filename),
            gdal.OpenEx(str(data / 'nz-waca-adjustments.gpkg')),
            format='ESRI Shapefile',
            layers=['nz_waca_adjustments'],
        )

        # now import the SHP
        repo_path = tmp_path / "repo"
        r = cli_runner.invoke(["init", "--import", source_filename, str(repo_path)])
        assert r.exit_code == 0, r

        # now check metadata
        path = "nz_waca_adjustments"
        repo = pygit2.Repository(str(repo_path))
        tree = repo.head.peel(pygit2.Tree) / path
        dataset = structure.DatasetStructure.instantiate(tree, path)

        meta_items = dict(dataset.iter_meta_items())
        assert set(meta_items) == {
            'gpkg_contents',
            'gpkg_geometry_columns',
            'gpkg_spatial_ref_sys',
            'primary_key',
            'sqlite_table_info',
            'version',
            'fields/FID',
            'fields/adjusted_n',
            'fields/date_adjus',
            'fields/geom',
            'fields/survey_ref',
        }
        assert meta_items['sqlite_table_info'] == [
            {
                'cid': 0,
                'name': 'FID',
                'type': 'INTEGER',
                'notnull': 1,
                'dflt_value': None,
                'pk': 1,
            },
            {
                'cid': 1,
                'name': 'geom',
                'type': 'POLYGON',
                'notnull': 0,
                'dflt_value': None,
                'pk': 0,
            },
            {
                'cid': 2,
                'name': 'date_adjus',
                'type': 'DATE',
                'notnull': 0,
                'dflt_value': None,
                'pk': 0,
            },
            {
                'cid': 3,
                'name': 'survey_ref',
                'type': 'TEXT(50)',
                'notnull': 0,
                'dflt_value': None,
                'pk': 0,
            },
            {
                'cid': 4,
                'name': 'adjusted_n',
                'type': 'MEDIUMINT',
                'notnull': 0,
                'dflt_value': None,
                'pk': 0,
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
def postgis_db():
    if 'SNO_POSTGRES_URL' not in os.environ:
        raise pytest.skip('Requires postgres')
    conn = psycopg2.connect(os.environ['SNO_POSTGRES_URL'])
    with conn.cursor() as cur:
        # test connection and postgis support
        try:
            cur.execute("""SELECT postgis_version()""")
        except psycopg2.errors.UndefinedFunction:
            raise pytest.skip('Requires PostGIS')
    yield conn


@pytest.fixture()
def postgis_layer(postgis_db, data_archive):
    postgres_conn_str = ImportPostgreSQL.postgres_url_to_ogr_conn_str(
        os.environ['SNO_POSTGRES_URL']
    )

    @contextlib.contextmanager
    def _postgis_layer(archive_name, gpkg_name, table):
        with data_archive(archive_name) as data:
            gdal.VectorTranslate(
                postgres_conn_str,
                gdal.OpenEx(str(data / gpkg_name)),
                format='PostgreSQL',
                accessMode='overwrite',
                layerCreationOptions=['LAUNDER=NO'],
                layers=[table],
            )
        yield
        c = postgis_db.cursor()
        c.execute(
            f"""
            DROP TABLE IF EXISTS {quote_ident(table)}
            """
        )

    return _postgis_layer


def test_pg_import(
    postgis_layer, data_archive, tmp_path, cli_runner, request, chdir,
):
    with postgis_layer(
        'gpkg-polygons', 'nz-waca-adjustments.gpkg', 'nz_waca_adjustments'
    ):
        repo_path = tmp_path / "repo"
        r = cli_runner.invoke(['init', repo_path])
        assert r.exit_code == 0, r
        with chdir(repo_path):
            r = cli_runner.invoke(
                ['import', os.environ['SNO_POSTGRES_URL'], 'nz_waca_adjustments']
            )
            assert r.exit_code == 0, r
        # now check metadata
        path = "nz_waca_adjustments"
        repo = pygit2.Repository(str(repo_path))
        tree = repo.head.peel(pygit2.Tree) / path
        dataset = structure.DatasetStructure.instantiate(tree, path)

        meta_items = dict(dataset.iter_meta_items())
        assert set(meta_items.keys()) == {
            'fields/geom',
            'version',
            'fields/id',
            'gpkg_geometry_columns',
            'gpkg_spatial_ref_sys',
            'fields/adjusted_nodes',
            'primary_key',
            'gpkg_contents',
            'fields/survey_reference',
            'fields/date_adjusted',
            'sqlite_table_info',
        }
        assert meta_items['sqlite_table_info'] == [
            {
                'cid': 0,
                'name': 'id',
                'type': 'INTEGER',
                'notnull': 1,
                'dflt_value': None,
                'pk': 1,
            },
            {
                'cid': 1,
                'name': 'geom',
                'type': 'MULTIPOLYGON',
                'notnull': 0,
                'dflt_value': None,
                'pk': 0,
            },
            {
                'cid': 2,
                'name': 'date_adjusted',
                'type': 'DATETIME',
                'notnull': 0,
                'dflt_value': None,
                'pk': 0,
            },
            {
                'cid': 3,
                'name': 'survey_reference',
                'type': 'TEXT(50)',
                'notnull': 0,
                'dflt_value': None,
                'pk': 0,
            },
            {
                'cid': 4,
                'name': 'adjusted_nodes',
                'type': 'MEDIUMINT',
                'notnull': 0,
                'dflt_value': None,
                'pk': 0,
            },
        ]
        contents = meta_items['gpkg_contents']
        assert contents == {
            'table_name': 'nz_waca_adjustments',
            'description': '',
            'data_type': 'features',
            'identifier': '',
            'srs_id': 4167,
        }


def test_pk_encoding():
    ds = Dataset1(None, "mytable")

    assert ds.encode_pk(492183) == "zgAHgpc="
    assert ds.decode_pk("zgAHgpc=") == 492183

    enc = [(i, ds.encode_pk(i)) for i in range(-50000, 50000, 23)]
    assert len(set([k for i, k in enc])) == len(enc)

    for i, k in enc:
        assert ds.decode_pk(k) == i

    assert ds.encode_pk("Dave") == "pERhdmU="
    assert ds.decode_pk("pERhdmU=") == "Dave"


@pytest.mark.slow
@pytest.mark.parametrize(*GPKG_IMPORTS)
@pytest.mark.parametrize(*DATASET_VERSIONS)
@pytest.mark.parametrize("profile", ["get_feature", "feature_to_dict"])
def test_feature_find_decode_performance(
    profile,
    import_version,
    archive,
    source_gpkg,
    table,
    data_archive,
    data_imported,
    geopackage,
    benchmark,
    request,
):
    """ Check single-feature decoding performance """
    param_ids = H.parameter_ids(request)
    benchmark.group = (
        f"test_feature_find_decode_performance - {profile} - {param_ids[-1]}"
    )

    repo_path = data_imported(archive, source_gpkg, table, import_version)
    repo = pygit2.Repository(str(repo_path))

    path = "mytable"
    tree = repo.head.peel(pygit2.Tree) / path

    dataset = structure.DatasetStructure.instantiate(tree, path)
    assert dataset.__class__.__name__ == f"Dataset{import_version[0]}"
    assert dataset.version == import_version

    with data_archive(archive) as data:
        db = geopackage(f"{data / source_gpkg}")
        dbcur = db.cursor()
        num_rows = dbcur.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
        pk_field = gpkg.pk(db, table)
        pk = dbcur.execute(
            f"SELECT {pk_field} FROM {table} ORDER BY {pk_field} LIMIT 1 OFFSET {min(97,num_rows-1)};"
        ).fetchone()[0]

    if profile == "get_feature":
        benchmark(dataset.get_feature, pk)

    elif profile == "feature_to_dict":
        # TODO: try to avoid two sets of code for two dataset versions -
        # either by making their interfaces more similar, or by deleting v1
        if import_version == "1.0":
            feature_path = dataset.encode_pk(pk)
            feature_data = (tree / dataset.get_feature_path(pk)).data
            benchmark(dataset.repo_feature_to_dict, feature_path, feature_data)
        elif import_version == "2.0":
            feature_path = dataset.encode_pk_values_to_path(pk)
            feature_data = dataset.get_data_at(feature_path)
            benchmark(dataset.get_feature, path=feature_path, data=feature_data)
    else:
        raise NotImplementedError(f"Unknown profile: {profile}")


@pytest.mark.slow
@pytest.mark.parametrize("import_version", ["1.0"])
def test_import_multiple(
    import_version, data_archive, chdir, cli_runner, tmp_path, geopackage
):
    repo_path = tmp_path / "data.sno"
    repo_path.mkdir()

    with chdir(repo_path):
        r = cli_runner.invoke(["init"])
        assert r.exit_code == 0, r

    repo = pygit2.Repository(str(repo_path))
    assert repo.is_bare
    assert repo.is_empty

    LAYERS = (
        ("gpkg-points", "nz-pa-points-topo-150k.gpkg", H.POINTS.LAYER),
        ("gpkg-polygons", "nz-waca-adjustments.gpkg", H.POLYGONS.LAYER),
    )

    datasets = []
    for i, (archive, source_gpkg, table) in enumerate(LAYERS):
        with data_archive(archive) as data:
            with chdir(repo_path):
                r = cli_runner.invoke(
                    [
                        "import",
                        f"GPKG:{data / source_gpkg}",
                        f"--version={import_version}",
                        table,
                    ]
                )
                assert r.exit_code == 0, r

                datasets.append(
                    _import_check(repo_path, table, f"{data / source_gpkg}", geopackage)
                )

                assert len([c for c in repo.walk(repo.head.target)]) == i + 1

                if i + 1 == len(LAYERS):
                    # importing to an existing path/layer should fail
                    with pytest.raises(ValueError, match=f"{table}/ already exists"):
                        r = cli_runner.invoke(
                            [
                                "import",
                                f"GPKG:{data / source_gpkg}",
                                f"--version={import_version}",
                                table,
                            ]
                        )

    # has two commits
    assert len([c for c in repo.walk(repo.head.target)]) == len(LAYERS)

    tree = repo.head.peel(pygit2.Tree)

    for i, ds in enumerate(datasets):
        assert ds.path == LAYERS[i][2]

        pk_enc, feature = next(ds.features())
        f_path = ds.get_feature_path(feature[ds.primary_key])
        assert tree / ds.path / f_path


@pytest.mark.slow
@pytest.mark.parametrize(*GPKG_IMPORTS)
@pytest.mark.parametrize(*DATASET_VERSIONS)
def test_write_feature_performance(
    import_version,
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
        repo_path = tmp_path / "data.sno"
        repo_path.mkdir()

        benchmark.group = f"test_write_feature_performance - {param_ids[-1]}"

        with chdir(repo_path):
            r = cli_runner.invoke(["init"])
            assert r.exit_code == 0, r

            repo = pygit2.Repository(str(repo_path))

            source = OgrImporter.open(data / source_gpkg, table=table)
            with source:
                dataset = structure.DatasetStructure.for_version(import_version)(
                    None, table
                )
                feature_iter = itertools.cycle(source.iter_features())

                index = pygit2.Index()

                if import_version == "1.0":
                    kwargs = {
                        "geom_cols": source.geom_cols,
                        "field_cid_map": source.field_cid_map,
                        "primary_key": source.primary_key,
                        "cast_primary_key": False,
                    }
                else:
                    kwargs = {"schema": source.schema}

                def _write_feature():
                    feature = next(feature_iter)
                    dest_path, dest_data = dataset.encode_feature(feature, **kwargs)
                    blob_id = repo.create_blob(dest_data)
                    entry = pygit2.IndexEntry(
                        f"{dataset.path}/{dest_path}", blob_id, pygit2.GIT_FILEMODE_BLOB
                    )
                    index.add(entry)

                benchmark(_write_feature)


@pytest.mark.slow
@pytest.mark.parametrize(*DATASET_VERSIONS)
def test_fast_import(import_version, data_archive, tmp_path, cli_runner, chdir):
    table = H.POINTS.LAYER
    with data_archive("gpkg-points") as data:
        # list tables
        repo_path = tmp_path / "data.sno"
        repo_path.mkdir()

        with chdir(repo_path):
            r = cli_runner.invoke(["init"])
            assert r.exit_code == 0, r

            repo = pygit2.Repository(str(repo_path))

            source = OgrImporter.open(data / "nz-pa-points-topo-150k.gpkg", table=table)

            fast_import.fast_import_tables(
                repo, {table: source}, version=import_version
            )

            assert not repo.is_empty
            assert repo.head.name == "refs/heads/master"
            assert repo.head.shorthand == "master"

            dataset = structure.RepositoryStructure(repo)[table]

            # has a single commit
            assert len([c for c in repo.walk(repo.head.target)]) == 1

            # has meta information
            assert import_version == dataset.version

            # has the right number of features
            feature_count = sum(1 for f in dataset.features())
            assert feature_count == source.row_count
