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
# gdal: DRIVER_DMD_LONGNAME = "Kart"
# gdal: DRIVER_DMD_OPENOPTIONLIST = "<OpenOptionList><Option name='GEOMTYPE' type='string' description='Geometry type override' /></OpenOptionList>"

from functools import wraps
import sys
import traceback
from pathlib import Path

from kart.repo import KartRepo
from kart import crs_util
from kart import spatial_filter

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
    def __init__(self, dataset, options):
        self.dataset = dataset
        self.options = options

        self.name = self.dataset.path
        self._parse_schema()

        self.iterator_honour_spatial_filter = self.dataset.is_spatial
        self.kart_spatial_filter = None

    def _parse_schema(self):
        schema = self.dataset.schema

        fields = []
        geom_fields = []
        self.geom_columns = []
        for col_schema in schema:
            if col_schema.data_type == "geometry":
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
                        "name": col_schema.name,
                        "type": ogr_type,
                        "srs": col_schema["geometryCRS"],
                    }
                )
                self.geom_columns.append(col_schema.name)
            else:
                ogr_type = OGR_DATATYPE_MAP[col_schema.data_type]
                fields.append({"name": col_schema.name, "type": ogr_type})

        # TODO: handle multiple pk fields
        self.pk_column = self.dataset.primary_key
        if self.geom_columns:
            self.geom_crs_id = crs_util.get_identifier_int_from_dataset(
                self.dataset, schema.geometry_columns[0]["geometryCRS"]
            )

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
        return self.dataset.feature_count

    def _as_ogr_feature(self, kart_feature):
        f = {
            "type": "OGRFeature",
            "fields": {},
            "geometry_fields": {},
        }

        if self.pk_column:
            f["id"] = kart_feature[self.pk_column]

        for col, value in kart_feature.items():
            if col in self.geom_columns:
                if value:
                    f["geometry_fields"][col] = value.to_wkb()
                else:
                    f["geometry_fields"][col] = None
            else:
                f["fields"][col] = value

        return f

    @gdal_api_wrapper
    def feature_by_id(self, fid):
        try:
            f = self.dataset.get_feature_with_crs_id([fid])
        except KeyError:
            return None
        else:
            return self._as_ogr_feature(f)

    @gdal_api_wrapper
    def __iter__(self):
        sf = self.kart_spatial_filter or spatial_filter.SpatialFilter.MATCH_ALL

        for kart_feature in self.dataset.features_with_crs_ids(spatial_filter=sf):
            try:
                yield self._as_ogr_feature(kart_feature)
            except Exception as ex:
                gdal.Error(f"KART !__iter__() {ex}\n{traceback.format_exc()}")
                raise

    @gdal_api_wrapper
    def spatial_filter_changed(self):
        filter_wkt = self.spatial_filter
        if filter_wkt is None:
            self.kart_spatial_filter = None
        else:
            ogr_geom = ogr.CreateGeometryFromWkt(filter_wkt)
            self.kart_spatial_filter = spatial_filter.SpatialFilter(
                self.geom_crs_id, ogr_geom
            )


class Dataset(BaseDataset):
    def __init__(self, repo, commit, ref, options):
        self.repo = repo
        self.commit = commit
        self.ref = ref
        self.layers = [
            Layer(dataset, options=options)
            for dataset in self.repo.datasets(
                refish=commit, filter_dataset_type="table"
            )
        ]
        self.options = options


class Driver(BaseDriver):
    @gdal_api_wrapper
    def identify(self, filename, first_bytes, open_flags, open_options={}):
        if filename.startswith("KART:"):
            filename = filename[5:]

        file_path = Path(filename)
        if file_path.is_dir():
            KartRepo(file_path, validate=True)
            return True

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
            repo = KartRepo(file_path, validate=True)
        except Exception:
            return None

        if len(parts) == 2:
            commit, ref = repo.resolve_refish(parts[1])
        else:
            commit, ref = repo.resolve_refish("HEAD")

        return Dataset(repo, commit, ref, options=open_options)
