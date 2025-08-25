import subprocess
from pathlib import Path
from osgeo import gdal

from kart.raster.metadata_util import is_cog


# Setting num-threads to two since we aim to spin up $ALL_CPUS threads and send tile-import GDAL tasks
# to each thread - if we also configured each GDAL task to use $ALL_CPUS threads, then we would end up
# spinning up O($ALL_CPUS * $ALL_CPUS) threads, which doesn't make much sense.
# However, set it to 2 so each task can have a main thread and a compression thread.
gdal.SetConfigOption("GDAL_NUM_THREADS", "2")
gdal.SetConfigOption("GDAL_TIFF_INTERNAL_MASK", "TRUE")
gdal.SetConfigOption("INTERLEAVE_OVERVIEW", "PIXEL")
gdal.SetConfigOption("BIGTIFF_OVERVIEW", "IF_SAFER")
gdal.SetConfigOption("GDAL_TIFF_OVR_BLOCKSIZE", "512")


def convert_tile_to_format(source, dest, target_format, override_srs=None):
    """
    Converts any GeoTIFF file at source to a tile of the given format at dest.
    """
    # convert-to-COG is the only tile conversion supported or required, so far.
    assert is_cog(target_format)
    return convert_tile_to_cog(source, dest, override_srs=override_srs)


def convert_tile_to_cog(source, dest, override_srs=None):
    """
    Converts any GeoTIFF file at source to a cloud-optimized GeoTIFF file at dest.
    """
    translate_options = gdal.TranslateOptions(
        format="COG",
        # Most of the GeoTIFF creation options don't affect the COG driver, so not much to put here.
        creationOptions=["BIGTIFF=IF_SAFER"],
        # Override the source CRS if specified
        outputSRS=str(override_srs) if override_srs else None,
    )

    gdal.Translate(str(dest), str(source), options=translate_options)

    assert dest.is_file()


def convert_tile_with_crs_override(source: Path, dest: Path, override_srs: str):
    """
    Converts a GeoTIFF file at source to the same format at dest, but with CRS override.
    This is used when --override-crs is specified but no other conversion is needed.
    Preserves compression, predictor, metadata and block size - but adds ZSTD compression if uncompressed.
    """
    src_ds = gdal.Open(str(source))
    assert src_ds is not None
    assert not dest.exists()

    creation_options = [
        "BIGTIFF=IF_SAFER",
        "COPY_SRC_OVERVIEWS=YES",
        "COPY_SRC_MDD=YES",
    ]

    # Preserve tiling/blocking
    band = src_ds.GetRasterBand(1)

    block_x, block_y = band.GetBlockSize()
    creation_options.extend([f"BLOCKXSIZE={block_x}", f"BLOCKYSIZE={block_y}"])
    if block_x != src_ds.RasterXSize:
        # Source is tiled
        creation_options.extend(["TILED=YES"])

    # Preserve or add compression
    compression = src_ds.GetMetadataItem("COMPRESSION", "IMAGE_STRUCTURE")
    if compression and compression.upper() != "NONE":
        # Preserve existing compression
        creation_options.append(f"COMPRESS={compression}")

        # Preserve predictor if it exists
        predictor = src_ds.GetMetadataItem("PREDICTOR", "IMAGE_STRUCTURE")
        if predictor:
            creation_options.append(f"PREDICTOR={predictor}")
    else:
        # Not having compression is *really* inefficient, and since we're here
        # we're going to be opinionated and just add some.
        dt = band.GetDataType()
        # per https://kokoalberti.com/articles/geotiff-compression-optimization-guide/
        # we use the predictor that gives the best compression ratio for the data type
        if "Float" in gdal.GetDataTypeName(dt):
            predictor = 3
        else:
            predictor = 2
        creation_options.extend(["COMPRESS=ZSTD", f"PREDICTOR={predictor}"])

    # Preserve interleave
    interleave = src_ds.GetMetadataItem("INTERLEAVE", "IMAGE_STRUCTURE")
    if interleave:
        creation_options.append(f"INTERLEAVE={interleave}")

    # create the output with CRS override and preserved characteristics
    translate_options = gdal.TranslateOptions(
        format="GTiff",
        creationOptions=creation_options,
        outputSRS=str(override_srs),
    )

    result_ds = gdal.Translate(str(dest), src_ds, options=translate_options)

    # Close datasets
    src_ds = None
    result_ds = None

    assert dest.is_file()
