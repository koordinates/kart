#!/usr/bin/env python3

###############################################################################
#
# Purpose:  Kart OGR driver
# Author:   Robert Coup <robert.coup@koordinates.com>
#
###############################################################################
# Copyright (c) Koordinates Limited
# SPDX-License-Identifier: LGPL-2.1+
###############################################################################

# Metadata parsed by GDAL C++ code at driver pre-loading, starting with '# gdal: '
# gdal: DRIVER_NAME = "KART"
# gdal: DRIVER_SUPPORTED_API_VERSION = [1]
# gdal: DRIVER_DCAP_VECTOR = "YES"
# gdal: DRIVER_DMD_LONGNAME = "Kart version control repository"
# gdal: DRIVER_DMD_OPENOPTIONLIST = "<OpenOptionList><Option name='GEOMTYPE' type='string' description='Geometry type override' /></OpenOptionList>"

import json
import re
import shlex
import subprocess
import traceback
from functools import wraps
from pathlib import Path
from typing import Tuple

from osgeo import gdal, ogr

from gdal_python_driver import BaseDataset, BaseDriver, BaseLayer


OGR_DATATYPE_MAP = {
    "boolean": ogr.OFTInteger,
    "blob": ogr.OFTBinary,
    "date": ogr.OFTDate,
    "float": ogr.OFTReal,
    "integer": ogr.OFTInteger64,
    "interval": ogr.OFTInteger64,
    "numeric": ogr.OFTString,
    "text": ogr.OFTString,
    "time": ogr.OFTTime,
    "timestamp": ogr.OFTDateTime,
}
OGR_GEOMTYPE_MAP = {
    "POINT": ogr.wkbPoint,
    "LINESTRING": ogr.wkbLineString,
    "POLYGON": ogr.wkbPolygon,
    "MULTIPOINT": ogr.wkbMultiPoint,
    "MULTILINESTRING": ogr.wkbMultiLineString,
    "MULTIPOLYGON": ogr.wkbMultiPolygon,
    "GEOMETRYCOLLECTION": ogr.wkbGeometryCollection,
    "GEOMETRY": ogr.wkbUnknown,
    # TODO? Z/M types
}


def invoke_kart(
    cmd: list, stdout=subprocess.PIPE, check=True, as_json=False, **run_kwargs
):
    full_cmd = ["kart"] + list(map(str, cmd))
    gdal.Debug("KART", f"$ {shlex.join(full_cmd)} {run_kwargs}")
    try:
        p = subprocess.run(
            ["kart"] + cmd, check=check, stdout=stdout, encoding="utf-8", **run_kwargs
        )
    except subprocess.CalledProcessError as ex:
        gdal.Error(f"KART $! {ex.returncode}\n{ex.stderr.decode('utf-8')}")
        raise
    except OSError as ex:
        gdal.Error(f"KART $! {ex}")
        raise
    else:
        if p.returncode == 0 and as_json:
            return json.loads(p.stdout)

    return p


# decorator to wrap gdal api calls with debug output
def gdal_api_wrapper(func):
    @wraps(func)
    def wrapped(*args, **kwargs):
        try:
            gdal.Debug("KART", f">{func.__name__}({args}, {kwargs})")
            result = func(*args, **kwargs)
        except Exception as ex:
            gdal.Error(f"KART !{func.__name__}() {ex}\n{traceback.format_exc()}")
            raise
        else:
            gdal.Debug("KART", f"<{func.__name__}() {result}")
            return result

    return wrapped


class Layer(BaseLayer):
    def __init__(self, name: str, dataset, options: dict):
        self.dataset = dataset
        self.name = name
        self.options = options

        self._ds_info = self._kart_get_dataset_info()
        self._parse_schema()
        self._feature_count = None

        # self.iterator_honour_spatial_filter = self.dataset.is_spatial
        # self.kart_spatial_filter = None

    def _kart_get_dataset_info(self):
        data = invoke_kart(
            [
                "-C",
                self.dataset.repo_path,
                "meta",
                "get",
                "--with-dataset-types",
                "-o",
                "json",
                "--ref",
                self.dataset.commit,
                self.name,
            ],
            as_json=True,
        )
        return data[self.name]

    def _kart_epsg(self, crs_id):
        if not crs_id.startswith("EPSG:"):
            raise NotImplementedError("Support for non-EPSG CRS")
        return int(crs_id.split(":")[1])

    def _parse_schema(self):
        schema = self._ds_info["schema.json"]

        fields = []
        geom_fields = []
        self.geom_columns = []
        self.pk_column = None

        for col_schema in schema:
            if col_schema["dataType"] == "geometry":
                override_geom_type = self.options.get("GEOMTYPE", None)
                if override_geom_type:
                    gdal.Debug(
                        "KART",
                        f"{self.name} overriding geometry type to {override_geom_type}",
                    )
                    ogr_type = OGR_GEOMTYPE_MAP[override_geom_type.upper()]
                else:
                    ogr_type = OGR_GEOMTYPE_MAP[col_schema["geometryType"]]

                geom_fields.append(
                    {
                        "name": col_schema["name"],
                        "type": ogr_type,
                        "srs": col_schema["geometryCRS"],
                    }
                )
                self.geom_columns.append(col_schema["name"])
            else:
                ogr_type = OGR_DATATYPE_MAP[col_schema["dataType"]]
                fields.append({"name": col_schema["name"], "type": ogr_type})

            if col_schema.get("primaryKeyIndex", -1) == 0:
                self.pk_column = col_schema["name"]

        # GDAL accesses these attributes
        self.fid_name = self.pk_column
        self.fields = fields
        self.geometry_fields = geom_fields

    @gdal_api_wrapper
    def test_capability(self, cap):
        if cap in (ogr.OLCStringsAsUTF8, ogr.OLCRandomRead):
            return True
        else:
            return False

    @gdal_api_wrapper
    def feature_count(self, force_computation):
        if force_computation or self._feature_count is None:
            d = invoke_kart(
                [
                    "-C",
                    self.dataset.repo_path,
                    "diff",
                    "-o",
                    "json",
                    "--only-feature-count=exact",
                    "[EMPTY]",
                    self.dataset.commit,
                    "--",
                    self.name,  # doesn't actually filter
                ],
                as_json=True,
            )

            self._feature_count = d[self.name]
        return self._feature_count

    def _as_ogr_feature(self, kart_diff_feature):
        feature = kart_diff_feature["change"]["+"]

        f = {
            "type": "OGRFeature",
            "fields": {},
            "geometry_fields": {},
        }

        if self.pk_column:
            f["id"] = feature[self.pk_column]

        for col, value in feature.items():
            if col in self.geom_columns:
                if value:
                    f["geometry_fields"][col] = bytes.fromhex(value)
                else:
                    f["geometry_fields"][col] = None
            else:
                f["fields"][col] = value

        return f

    @gdal_api_wrapper
    def feature_by_id(self, fid):
        fd = invoke_kart(
            [
                "-C",
                self.dataset.repo_path,
                "diff",
                "-o",
                "json",
                "[EMPTY]",
                self.dataset.commit,
                "--",
                f"{self.name}:{fid}",
            ],
            as_json=True,
        )

        f = fd["kart.diff/v1+hexwkb"].get(fid)
        if f is None:
            return None

        return self._as_ogr_feature(f)

    @gdal_api_wrapper
    def __iter__(self):
        cmd = [
            "kart",
            "-C",
            self.dataset.repo_path,
            "diff",
            "-o",
            "json-lines",
            "[EMPTY]",
            self.dataset.commit,
            "--",
            self.name,
        ]
        popen_params = {
            "stdout": subprocess.PIPE,
            "encoding": "utf-8",
            "bufsize": 1,
        }
        gdal.Debug("KART", f"__iter__() $ {shlex.join(map(str, cmd))}")
        try:
            with subprocess.Popen(cmd, **popen_params) as p:
                count = 0
                for line in p.stdout:
                    record = json.loads(line)
                    if record["type"] == "version":
                        # first row
                        assert record["version"] == "kart.diff/v2"
                        assert record["outputFormat"] == "JSONL+hexwkb"
                    elif record["type"] == "feature":
                        count += 1
                        try:
                            yield self._as_ogr_feature(record)
                        except Exception as ex:
                            gdal.Error(
                                f"KART !__iter__() {ex}\n{traceback.format_exc()}"
                            )
                            raise
            gdal.Debug("KART", f"__iter__() yielded {count} features")
        except OSError as ex:
            gdal.Error(f"KART !__iter__() {ex}")
            raise

    # @gdal_api_wrapper
    # def spatial_filter_changed(self):
    #     filter_wkt = self.spatial_filter
    #     if filter_wkt is None:
    #         self.kart_spatial_filter = None
    #     else:
    #         ogr_geom = ogr.CreateGeometryFromWkt(filter_wkt)
    #         self.kart_spatial_filter = spatial_filter.SpatialFilter(self.geom_crs_id, ogr_geom)


class DiffLayer(BaseLayer):
    def __init__(self, name: str, dataset, options: dict):
        self.dataset = dataset
        self.name = name
        self.options = options

        self._schema = None
        self._feature_count = None

        self._schema = next(self._kart_diff_iter(schema=True))

    def _kart_epsg(self, crs_id):
        if not crs_id.startswith("EPSG:"):
            raise NotImplementedError("Support for non-EPSG CRS")
        return int(crs_id.split(":")[1])

    def _parse_schema(self, schema):
        fields = [
            {"name": "__id__", "type": ogr.OFTString},
            {"name": "__change__", "type": ogr.OFTString},
        ]
        geom_fields = []
        self.geom_columns = []

        for col_schema in schema:
            if col_schema["dataType"] == "geometry":
                override_geom_type = self.options.get("GEOMTYPE", None)
                if override_geom_type:
                    gdal.Debug(
                        "KART",
                        f"{self.name} overriding geometry type to {override_geom_type}",
                    )
                    ogr_type = OGR_GEOMTYPE_MAP[override_geom_type.upper()]
                else:
                    ogr_type = OGR_GEOMTYPE_MAP[col_schema["geometryType"]]

                geom_fields.append(
                    {
                        "name": col_schema["name"],
                        "type": ogr_type,
                        "srs": col_schema["geometryCRS"],
                    }
                )
                self.geom_columns.append(col_schema["name"])
            else:
                ogr_type = OGR_DATATYPE_MAP[col_schema["dataType"]]
                fields.append({"name": col_schema["name"], "type": ogr_type})

        # GDAL accesses these attributes
        self.fid_name = "__id__"
        self.fields = fields
        self.geometry_fields = geom_fields

        return schema

    @gdal_api_wrapper
    def test_capability(self, cap):
        if cap in (ogr.OLCStringsAsUTF8, ogr.OLCRandomRead):
            return True
        else:
            return False

    @gdal_api_wrapper
    def feature_count(self, force_computation):
        if force_computation or self._feature_count is None:
            d = invoke_kart(
                [
                    "-C",
                    self.dataset.repo_path,
                    "diff",
                    "-o",
                    "json",
                    "--only-feature-count=exact",
                    self.dataset.range,
                    "--",
                    self.name,  # doesn't actually filter
                ],
                as_json=True,
            )

            self._feature_count = d[self.name]
        return self._feature_count

    def _as_ogr_feature(self, kart_diff_feature, index=0):
        change = kart_diff_feature["change"]

        if "+" in change and "-" in change:
            change_type = "UPDATE"
            change_feature = change["+"]
        elif "+" in change:
            change_type = "INSERT"
            change_feature = change["+"]
        elif "-" in change:
            change_type = "DELETE"
            change_feature = change["-"]
        else:
            assert False, "Unknown change type"

        pk = f"{self.name}.{index}"
        f = {
            "type": "OGRFeature",
            "fields": {
                "__change__": change_type,
                "__id__": pk,
            },
            "geometry_fields": {},
            "id": pk,
        }

        for col, value in change_feature.items():
            if col in self.geom_columns:
                if value:
                    f["geometry_fields"][col] = bytes.fromhex(value)
                else:
                    f["geometry_fields"][col] = None
            else:
                f["fields"][col] = value

        return f

    @gdal_api_wrapper
    def feature_by_id(self, fid):
        fd = invoke_kart(
            [
                "-C",
                self.dataset.repo_path,
                "diff",
                "-o",
                "json",
                self.dataset.range,
                "--",
                f"{self.name}:{fid}",
            ],
            as_json=True,
        )

        f = fd["kart.diff/v1+hexwkb"].get(fid)
        if f is None:
            return None

        return self._as_ogr_feature(f)

    @gdal_api_wrapper
    def __iter__(self):
        yield from self._kart_diff_iter()

    def _kart_diff_iter(self, schema=False):
        cmd = [
            "kart",
            "-C",
            self.dataset.repo_path,
            "diff",
            "-o",
            "json-lines",
            self.dataset.range,
            "--",
            self.name,
        ]
        popen_params = {
            "stdout": subprocess.PIPE,
            "encoding": "utf-8",
            "bufsize": 1,
        }
        gdal.Debug("KART", f"diff_iter $ {shlex.join(map(str, cmd))}")
        try:
            with subprocess.Popen(cmd, **popen_params) as p:
                index = 0
                for line in p.stdout:
                    record = json.loads(line)
                    if record["type"] == "version":
                        # first row
                        gdal.Debug("KART", "diff_iter: version")
                        assert record["version"] == "kart.diff/v2"
                        assert record["outputFormat"] == "JSONL+hexwkb"

                    elif record["type"] == "metaInfo" and schema:
                        gdal.Debug("KART", f"diff_iter: metaInfo: {record['key']}")
                        p.terminate()
                        if record["key"] == "schema.json":
                            yield self._parse_schema(record["value"])
                            raise StopIteration()

                    elif record["type"] == "feature":
                        if schema:
                            raise ValueError("Expexcted metaInfo by now")
                        assert self._schema is not None, "expected headers by now"

                        try:
                            yield self._as_ogr_feature(record, index)
                        except Exception as ex:
                            gdal.Error(
                                f"KART !__iter__() {ex}\n{traceback.format_exc()}"
                            )
                            raise
                        index += 1

            gdal.Debug("KART", f"diff_iter yielded {index} features")
        except OSError as ex:
            gdal.Error(f"KART !diff_iter {ex}")
            raise


class Dataset(BaseDataset):
    def __init__(self, repo_path, commit, ref, options):
        self.repo_path = repo_path
        self.commit = commit
        self.ref = ref
        self.options = options
        self.layers = [
            Layer(layer, self, options=options) for layer in self._kart_get_layers()
        ]

    def _kart_get_layers(self):
        data = invoke_kart(
            [
                "-C",
                self.repo_path,
                "data",
                "ls",
                "--with-dataset-types",
                "-o",
                "json",
                self.commit,
            ],
            as_json=True,
        )
        return [d["path"] for d in data["kart.data.ls/v2"] if d["type"] == "table"]


class DiffDataset(BaseDataset):
    def __init__(self, repo_path, range, options):
        self.repo_path = repo_path
        self.range = range
        self.options = options
        self.layers = [
            DiffLayer(layer, self, options=options) for layer in self._kart_get_layers()
        ]

    def _kart_get_layers(self):
        data = invoke_kart(
            [
                "-C",
                self.repo_path,
                "diff",
                "--only-feature-count=veryfast",
                "-o",
                "json",
                self.range,
                "--",
            ],
            as_json=True,
        )
        return data.keys()


class Driver(BaseDriver):
    @gdal_api_wrapper
    def identify(self, filename, first_bytes, open_flags, open_options={}):
        if filename.startswith("KART:"):
            filename = filename[5:]

        file_path = Path(filename)
        if file_path.is_dir():
            return self._kart_validate(file_path)
        return False

    def _kart_validate(self, repo_path: Path) -> bool:
        try:
            p = invoke_kart(
                ["-C", repo_path, "data", "ls", "--with-dataset-types", "-o", "json"],
                check=False,
            )
        except subprocess.CalledProcessError as ex:
            gdal.Error(f"KART Error checking path {repo_path}: {ex}")
            return False
        else:
            if p.returncode == 0:
                # Success!
                return True
            elif p.returncode == 41:
                # Normal error for not-a-kart-repo
                return False
            else:
                gdal.Error(
                    f"KART Error checking path {repo_path} [{p.returncode}]: {p.stderr.decode('utf-8')}"
                )
                return False

    @gdal_api_wrapper
    def open(self, filename, first_bytes, open_flags, open_options={}):
        if filename.startswith("KART:"):
            filename = filename[5:]

        parts = filename.split("@", 1)
        file_path = Path(parts[0])

        if not file_path.is_dir():
            return None

        try:
            if not self._kart_validate(file_path):
                return None
        except Exception:
            return None

        refish = parts[1] if len(parts) == 2 else "HEAD"
        if ".." in refish:
            assert not re.search(r"\s", refish)
            assert not refish.startswith("-")
            return DiffDataset(file_path, refish, options=open_options)

        else:
            commit, ref = self._kart_resolve_refish(file_path, refish)
            gdal.Debug("KART", f"Resolved {refish!r} to {commit}")
            return Dataset(file_path, commit, ref, options=open_options)

    def _kart_resolve_refish(
        self, repo_path: Path, refish: str
    ) -> Tuple[str, str | None]:
        p = invoke_kart(
            [
                "-C",
                repo_path,
                "git",
                "rev-parse",
                "--verify",
                "--end-of-options",
                f"{refish}^{{commit}}",
            ],
            check=False,
        )
        if p.returncode == 0:
            return p.stdout.strip(), refish
        else:
            raise ValueError(f"{refish} does not resolve to a commit")
