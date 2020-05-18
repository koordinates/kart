import itertools
import json
import os
import re
import subprocess

from osgeo import gdal, ogr
import pygit2
import pytest

from sno import gpkg
from sno.init import OgrImporter
from sno.structure import DatasetStructure, Dataset1


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
    DatasetStructure.version_numbers(),
)


def test_dataset_versions():
    assert DatasetStructure.version_numbers() == ("1.0",)
    klasses = DatasetStructure.all_versions()
    assert set(klass.VERSION_IMPORT for klass in klasses) == set(
        DatasetStructure.version_numbers()
    )

    di = DatasetStructure.importer("bob", version=None)
    assert di.__class__.__name__ == "Dataset1"
    assert di.VERSION_IMPORT == DatasetStructure.DEFAULT_IMPORT_VERSION

    di = DatasetStructure.importer("bob", version="1.0")
    assert di.__class__.__name__ == "Dataset1"


def _import_check(repo_path, table, source_gpkg, geopackage):
    repo = pygit2.Repository(str(repo_path))
    tree = repo.head.peel(pygit2.Tree) / table

    dataset = DatasetStructure.instantiate(tree, table)

    db = geopackage(source_gpkg)
    num_rows = db.cursor().execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]

    o = subprocess.check_output(["git", "ls-tree", "-r", "-t", "HEAD", table])
    print("\n".join(l.decode("utf8") for l in o.splitlines()[:20]))

    if dataset.version.startswith("1."):
        re_paths = (
            r"^\d{6} blob [0-9a-f]{40}\t%s/.sno-table/[0-9a-f]{2}/[0-9a-f]{2}/([^/]+)$"
            % table
        )
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

    # wrap the DatasetStructure import with benchmarking
    orig_import_func = DatasetStructure.fast_import_table

    def _benchmark_import(*args, **kwargs):
        # one round/iteration isn't very statistical, but hopefully crude idea
        return benchmark.pedantic(
            orig_import_func, args=args, kwargs=kwargs, rounds=1, iterations=1
        )

    monkeypatch.setattr(DatasetStructure, 'fast_import_table', _benchmark_import)

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
                    f'--table={table}',
                    table,
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


@pytest.mark.slow
@pytest.mark.parametrize(
    GPKG_IMPORTS[0],
    [
        # Exclude the table one, because SHP can't handle tables very well
        *GPKG_IMPORTS[1][:3],
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
    """ Import the GeoPackage (eg. `kx-foo-layer.gpkg`) into a Sno repository. """
    param_ids = H.parameter_ids(request)

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

        if param_ids[-1] == "empty":
            assert num_rows == 0

        # convert to a new format using OGR
        source_filename = tmp_path / f"data.{source_format.lower()}"
        gdal.VectorTranslate(
            str(source_filename),
            gdal.OpenEx(str(data / source_gpkg)),
            format=source_ogr_driver,
            layers=[table],
        )
        with chdir(repo_path):
            r = cli_runner.invoke(["init"])
            assert r.exit_code == 0, r

            repo = pygit2.Repository(str(repo_path))
            assert repo.is_bare
            assert repo.is_empty

            # Import from SHP/TAB/something into sno
            r = cli_runner.invoke(
                ["import", str(source_filename), f"--version={import_version}", table]
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

            if num_rows > 0:
                # compare the first feature in the repo against the source DB
                key, got_feature = next(dataset.features())
                fid = dataset.decode_pk(key)

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

    dataset = DatasetStructure.instantiate(tree, path)
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
        f_obj = tree / dataset.get_feature_path(pk)
        pk_enc = dataset.encode_pk(pk)

        benchmark(dataset.repo_feature_to_dict, pk_enc, f_obj)

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
                        f"--table={table}",
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
                                f"--table={table}",
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
                dataset = DatasetStructure.for_version(import_version)(None, table)
                feature_iter = itertools.cycle(source.iter_features())

                index = pygit2.Index()

                kwargs = {
                    "geom_cols": source.geom_cols,
                    "field_cid_map": source.field_cid_map,
                    "primary_key": source.primary_key,
                    "path": dataset.path,
                }

                def _write_feature():
                    return dataset.write_feature(
                        next(feature_iter), repo, index, **kwargs
                    )

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

            dataset = DatasetStructure.for_version(import_version)(None, table)
            dataset.fast_import_table(repo, source)

            assert not repo.is_empty
            assert repo.head.name == "refs/heads/master"
            assert repo.head.shorthand == "master"

            dataset.tree = repo.head.peel(pygit2.Tree) / table

            # has a single commit
            assert len([c for c in repo.walk(repo.head.target)]) == 1

            # has meta information
            assert import_version == dataset.get_meta_item("version")["version"]

            # has the right number of features
            feature_count = sum(1 for f in dataset.features())
            assert feature_count == source.row_count
