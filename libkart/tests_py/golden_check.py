#!/usr/bin/env python3
"""Golden / C-ABI verification harness for libkart.

Loads the libkart cdylib via cffi and compares its C-ABI output against Kart's
own Python implementation (imported in the same process) for real fixture repos.

Run with a Python that can import kart (e.g. the build venv), with LIBKART_PATH
pointing at the built library; CTest does this automatically via the
``libkart-golden`` test. Without LIBKART_PATH it falls back to a local
``cargo build --release`` under the crate. Exits 0 iff every check passes.
"""

import os
import subprocess
import sys
import tempfile

import cffi
import pygit2

from kart.repo import KartRepo
from kart.geometry import Geometry

HERE = os.path.dirname(os.path.abspath(__file__))
CRATE = os.path.dirname(HERE)
if sys.platform == "darwin":
    _LIB_NAME = "libkart.dylib"
elif sys.platform == "win32":
    _LIB_NAME = "libkart.dll"
else:
    _LIB_NAME = "libkart.so"
# LIBKART_PATH (set e.g. by the CMake/CTest harness) wins; otherwise fall back to a
# local `cargo build --release` under the crate.
DYLIB = os.environ.get("LIBKART_PATH") or os.path.join(
    CRATE, "target", "release", _LIB_NAME
)
HEADER = os.path.join(CRATE, "include", "libkart.h")
DATA = os.path.join(os.path.dirname(CRATE), "tests", "data")

# ---- cffi setup -------------------------------------------------------------

CDEF = """
int kart_repo_open(const char *path, uint64_t *out_repo);
void kart_repo_free(uint64_t repo);
int kart_repo_table_dataset_version(uint64_t repo, int *out_version);
int kart_repo_list_datasets(uint64_t repo, const char *refish, uint8_t **out_json, size_t *out_len);

int kart_dataset_open(uint64_t repo, const char *refish, const char *path, uint64_t *out_ds);
void kart_dataset_free(uint64_t ds);
int kart_dataset_type(uint64_t ds, uint8_t **out, size_t *out_len);
int kart_dataset_schema_json(uint64_t ds, uint8_t **out, size_t *out_len);
int kart_dataset_crs_wkt(uint64_t ds, uint8_t **out, size_t *out_len);
int kart_dataset_meta_item(uint64_t ds, const char *name, uint8_t **out, size_t *out_len);

int kart_feature_geometry(uint64_t ds, const uint8_t *blob, size_t blob_len, uint8_t **out, size_t *out_len);
int kart_tile_summary_json(uint64_t ds, const uint8_t *blob, size_t blob_len, uint8_t **out, size_t *out_len);

int kart_gpkg_is_empty(const uint8_t *g, size_t n, int *out);
int kart_gpkg_geometry_type(const uint8_t *g, size_t n, int *out);
int kart_gpkg_envelope(const uint8_t *g, size_t n, int only_2d, int calc, double *out6, int *out_count);
int kart_gpkg_to_wkb(const uint8_t *g, size_t n, uint8_t **out, size_t *out_len);

const char *kart_last_error(void);
void kart_free(void *ptr);
"""

ffi = cffi.FFI()
ffi.cdef(CDEF)
lib = ffi.dlopen(DYLIB)


def last_error():
    p = lib.kart_last_error()
    return ffi.string(p).decode("utf-8", "replace") if p else ""


class KartError(Exception):
    pass


def repo_open(path):
    out = ffi.new("uint64_t *")
    rc = lib.kart_repo_open(path.encode(), out)
    if rc != 0:
        raise KartError(f"kart_repo_open: {last_error()}")
    return out[0]


def dataset_open(repo, refish, path):
    out = ffi.new("uint64_t *")
    rc = lib.kart_dataset_open(repo, refish.encode(), path.encode(), out)
    if rc != 0:
        raise KartError(f"kart_dataset_open: {last_error()}")
    return out[0]


def _take_bytes(out_pp, out_len):
    """Copy a libkart-returned buffer into a Python bytes and free it. None -> b''."""
    if out_pp[0] == ffi.NULL or out_len[0] == 0:
        return b""
    data = bytes(ffi.buffer(out_pp[0], out_len[0]))
    lib.kart_free(ffi.cast("void *", out_pp[0]))
    return data


def repo_table_dataset_version(repo):
    out = ffi.new("int *")
    rc = lib.kart_repo_table_dataset_version(repo, out)
    if rc != 0:
        raise KartError(f"version: {last_error()}")
    return out[0]


def repo_list_datasets(repo, refish):
    pp = ffi.new("uint8_t **")
    n = ffi.new("size_t *")
    rc = lib.kart_repo_list_datasets(repo, refish.encode(), pp, n)
    if rc != 0:
        raise KartError(f"list_datasets: {last_error()}")
    import json

    return json.loads(_take_bytes(pp, n).decode("utf-8"))


def dataset_schema_json(ds):
    pp = ffi.new("uint8_t **")
    n = ffi.new("size_t *")
    rc = lib.kart_dataset_schema_json(ds, pp, n)
    if rc != 0:
        raise KartError(f"schema_json: {last_error()}")
    import json

    return json.loads(_take_bytes(pp, n).decode("utf-8"))


def dataset_crs_wkt(ds):
    pp = ffi.new("uint8_t **")
    n = ffi.new("size_t *")
    rc = lib.kart_dataset_crs_wkt(ds, pp, n)
    if rc != 0:
        raise KartError(f"crs_wkt: {last_error()}")
    data = _take_bytes(pp, n)
    return data.decode("utf-8") if data else None


def feature_geometry(ds, blob):
    pp = ffi.new("uint8_t **")
    n = ffi.new("size_t *")
    rc = lib.kart_feature_geometry(ds, blob, len(blob), pp, n)
    if rc != 0:
        raise KartError(f"feature_geometry: {last_error()}")
    return _take_bytes(pp, n) or None


def tile_summary_json(ds, blob):
    pp = ffi.new("uint8_t **")
    n = ffi.new("size_t *")
    rc = lib.kart_tile_summary_json(ds, blob, len(blob), pp, n)
    if rc != 0:
        raise KartError(f"tile_summary_json: {last_error()}")
    import json

    return json.loads(_take_bytes(pp, n).decode("utf-8"))


def gpkg_is_empty(g):
    out = ffi.new("int *")
    rc = lib.kart_gpkg_is_empty(g, len(g), out)
    if rc != 0:
        raise KartError(f"is_empty: {last_error()}")
    return bool(out[0])


def gpkg_geometry_type(g):
    out = ffi.new("int *")
    rc = lib.kart_gpkg_geometry_type(g, len(g), out)
    if rc != 0:
        raise KartError(f"geometry_type: {last_error()}")
    return out[0]


def gpkg_envelope(g, only_2d=False, calc=False):
    out6 = ffi.new("double[6]")
    cnt = ffi.new("int *")
    rc = lib.kart_gpkg_envelope(
        g, len(g), 1 if only_2d else 0, 1 if calc else 0, out6, cnt
    )
    if rc != 0:
        raise KartError(f"envelope: {last_error()}")
    if cnt[0] == 0:
        return None
    return tuple(out6[i] for i in range(cnt[0]))


def gpkg_to_wkb(g):
    pp = ffi.new("uint8_t **")
    n = ffi.new("size_t *")
    rc = lib.kart_gpkg_to_wkb(g, len(g), pp, n)
    if rc != 0:
        raise KartError(f"to_wkb: {last_error()}")
    return _take_bytes(pp, n)


# ---- harness scaffolding ----------------------------------------------------

results = []  # list of (name, pass, detail)


def check(name, ok, detail=""):
    results.append((name, bool(ok), detail))
    status = "PASS" if ok else "FAIL"
    print(f"[{status}] {name}: {detail}")


def extract(tgz, subdir):
    base = tempfile.mkdtemp(prefix="libkart-golden-")
    subprocess.run(["tar", "xzf", tgz, "-C", base], check=True)
    return os.path.join(base, subdir)


def first_blob(tree):
    for e in tree:
        if e.type_str == "blob":
            return e
        b = first_blob(e)
        if b is not None:
            return b
    return None


# ---- vector repo checks (polygons.tgz, au-census.tgz) -----------------------


def check_vector(tgz, subdir, ds_path, expected_datasets):
    print(f"\n=== vector repo {subdir} ({ds_path}) ===")
    root = extract(tgz, subdir)
    pyrepo = KartRepo(root)
    pyds = [d for d in pyrepo.structure("HEAD").datasets() if d.path == ds_path][0]

    repo = repo_open(root)
    try:
        # version
        v_c = repo_table_dataset_version(repo)
        v_py = pyrepo.table_dataset_version
        check(f"{subdir}: table_dataset_version", v_c == v_py, f"c={v_c} py={v_py}")

        # list_datasets
        ds_c = sorted(repo_list_datasets(repo, "HEAD"))
        ds_py = sorted(d.path for d in pyrepo.structure("HEAD").datasets())
        check(
            f"{subdir}: list_datasets",
            ds_c == ds_py and set(expected_datasets) <= set(ds_c),
            f"c={ds_c} py={ds_py}",
        )

        ds = dataset_open(repo, "HEAD", ds_path)
        try:
            # schema_json: geometry column id/name and pk vs kart dataset
            sj = dataset_schema_json(ds)
            # derive geom + pk from kart schema objects
            geom_cols = [c for c in pyds.schema if c.data_type == "geometry"]
            py_geom_id = geom_cols[0].id if geom_cols else None
            py_geom_name = geom_cols[0].name if geom_cols else None
            pk_cols = [c for c in pyds.schema if c.pk_index is not None]
            py_pk = pk_cols[0].name if len(pk_cols) == 1 else None

            check(
                f"{subdir}: schema geom column id",
                sj["columns"]
                and any(
                    c.get("dataType") == "geometry" and c.get("id") == py_geom_id
                    for c in sj["columns"]
                ),
                f"c.geom_column_id={[c.get('id') for c in sj['columns'] if c.get('dataType')=='geometry']} py={py_geom_id}",
            )
            check(
                f"{subdir}: schema geom column name",
                sj.get("geom_column_name") == py_geom_name,
                f"c={sj.get('geom_column_name')} py={py_geom_name}",
            )
            check(
                f"{subdir}: schema primary_key",
                sj.get("primary_key") == py_pk,
                f"c={sj.get('primary_key')} py={py_pk}",
            )

            # columns byte-faithful to the raw meta/schema.json blob
            import json

            meta_schema = json.loads((pyds.inner_tree / "meta" / "schema.json").data)
            check(
                f"{subdir}: schema columns match meta/schema.json",
                sj["columns"] == meta_schema,
                f"n_cols c={len(sj['columns'])} py={len(meta_schema)}",
            )

            # crs_wkt
            wkt_c = dataset_crs_wkt(ds)
            crs_name = geom_cols[0]["geometryCRS"] if geom_cols else None
            wkt_py = None
            if crs_name:
                wkt_py = (
                    pyds.inner_tree / "meta" / "crs" / f"{crs_name}.wkt"
                ).data.decode("utf-8")
            check(
                f"{subdir}: crs_wkt",
                wkt_c == wkt_py,
                f"match={wkt_c == wkt_py} len_c={len(wkt_c or '')} len_py={len(wkt_py or '')}",
            )

            # feature_geometry vs kart-decoded GPKG bytes (byte-equal) over several blobs
            feat_tree = pyds.inner_tree / "feature"
            blobs = []

            def collect(t, limit):
                for e in t:
                    if len(blobs) >= limit:
                        return
                    if e.type_str == "blob":
                        blobs.append(e)
                    else:
                        collect(e, limit)

            collect(feat_tree, 5)
            check(f"{subdir}: found feature blobs", len(blobs) > 0, f"n={len(blobs)}")
            geom_name = py_geom_name
            all_match = True
            detail = ""
            for b in blobs:
                raw = b.data
                geom_c = feature_geometry(ds, raw)
                feat = pyds.get_feature_from_blob(b)
                geom_py = feat[geom_name]
                py_bytes = bytes(geom_py) if geom_py is not None else None
                if geom_c != py_bytes:
                    all_match = False
                    detail = f"blob {b.id}: c_len={len(geom_c) if geom_c else None} py_len={len(py_bytes) if py_bytes else None}"
                    break
            if all_match:
                detail = f"{len(blobs)} feature geometries byte-equal"
            check(f"{subdir}: feature_geometry byte-equal", all_match, detail)

            # gpkg helpers vs kart Geometry on the first feature geometry
            b0 = blobs[0]
            gpkg = bytes(pyds.get_feature_from_blob(b0)[geom_name])
            geom_py = Geometry(gpkg)
            check(
                f"{subdir}: gpkg_is_empty",
                gpkg_is_empty(gpkg) == geom_py.is_empty(),
                f"c={gpkg_is_empty(gpkg)} py={geom_py.is_empty()}",
            )
            check(
                f"{subdir}: gpkg_geometry_type",
                gpkg_geometry_type(gpkg) == geom_py.geometry_type,
                f"c={gpkg_geometry_type(gpkg)} py={geom_py.geometry_type}",
            )
            env_c = gpkg_envelope(gpkg, only_2d=False, calc=False)
            env_py = geom_py.envelope(only_2d=False, calculate_if_missing=False)
            env_ok = (env_c is None and env_py is None) or (
                env_c is not None
                and env_py is not None
                and len(env_c) == len(env_py)
                and all(a == b for a, b in zip(env_c, env_py))
            )
            check(f"{subdir}: gpkg_envelope", env_ok, f"c={env_c} py={env_py}")
            wkb_c = gpkg_to_wkb(gpkg)
            wkb_py = geom_py.to_wkb()
            check(
                f"{subdir}: gpkg_to_wkb byte-equal",
                wkb_c == wkb_py,
                f"c_len={len(wkb_c)} py_len={len(wkb_py)} eq={wkb_c == wkb_py}",
            )
        finally:
            lib.kart_dataset_free(ds)
    finally:
        lib.kart_repo_free(repo)


# ---- point-cloud repo checks (auckland.tgz) ---------------------------------


def check_pointcloud(tgz, subdir, ds_path):
    print(f"\n=== point-cloud repo {subdir} ({ds_path}) ===")
    root = extract(tgz, subdir)
    pyrepo = KartRepo(root)
    pyds = [d for d in pyrepo.structure("HEAD").datasets() if d.path == ds_path][0]

    repo = repo_open(root)
    try:
        ds_c = sorted(repo_list_datasets(repo, "HEAD"))
        ds_py = sorted(d.path for d in pyrepo.structure("HEAD").datasets())
        check(f"{subdir}: list_datasets", ds_c == ds_py, f"c={ds_c} py={ds_py}")

        ds = dataset_open(repo, "HEAD", ds_path)
        try:
            tcheck = bytes(ds_type_bytes(ds))
            check(
                f"{subdir}: dataset_type",
                tcheck == b"point-cloud",
                f"c={tcheck!r}",
            )

            # tile summary vs kart, over a few tiles. The C ABI receives only the blob
            # bytes, so it cannot know the tile's git filename -> it omits/derives 'name'.
            # Compare all other keys exactly.
            tile_tree = pyds.inner_tree / "tile"
            tiles = []

            def collect(t, limit):
                for e in t:
                    if len(tiles) >= limit:
                        return
                    if e.type_str == "blob":
                        tiles.append(e)
                    else:
                        collect(e, limit)

            collect(tile_tree, 5)
            check(f"{subdir}: found tile blobs", len(tiles) > 0, f"n={len(tiles)}")

            all_ok = True
            detail = ""
            for b in tiles:
                summary_c = tile_summary_json(ds, b.data)
                summary_py = pyds.get_tile_summary_from_pointer_blob(b)
                # keys present in both except 'name' (which needs the git filename)
                keys = set(summary_py) - {"name"}
                for k in keys:
                    if summary_c.get(k) != summary_py.get(k):
                        all_ok = False
                        detail = f"tile {b.name} key {k}: c={summary_c.get(k)!r} py={summary_py.get(k)!r}"
                        break
                if not all_ok:
                    break
            if all_ok:
                detail = f"{len(tiles)} tiles: all keys (except name) match"
            check(f"{subdir}: tile_summary keys match kart", all_ok, detail)
        finally:
            lib.kart_dataset_free(ds)
    finally:
        lib.kart_repo_free(repo)


def ds_type_bytes(ds):
    pp = ffi.new("uint8_t **")
    n = ffi.new("size_t *")
    rc = lib.kart_dataset_type(ds, pp, n)
    if rc != 0:
        raise KartError(f"dataset_type: {last_error()}")
    return _take_bytes(pp, n)


def main():
    if not os.path.exists(DYLIB):
        print(f"dylib missing: {DYLIB}")
        sys.exit(2)
    print(f"loaded dylib: {DYLIB}")

    check_vector(
        os.path.join(DATA, "polygons.tgz"),
        "polygons",
        "nz_waca_adjustments",
        ["nz_waca_adjustments"],
    )
    check_vector(
        os.path.join(DATA, "au-census.tgz"),
        "au-census",
        "census2016_sdhca_ot_ra_short",
        ["census2016_sdhca_ot_ra_short", "census2016_sdhca_ot_sos_short"],
    )
    check_pointcloud(
        os.path.join(DATA, "point-cloud", "auckland.tgz"),
        "auckland",
        "auckland",
    )

    print("\n==== SUMMARY ====")
    npass = sum(1 for _, ok, _ in results if ok)
    for name, ok, detail in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}")
    allpass = npass == len(results)
    print(f"\n{npass}/{len(results)} checks passed. allPass={allpass}")
    sys.exit(0 if allpass else 1)


if __name__ == "__main__":
    main()
