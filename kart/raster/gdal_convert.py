from osgeo import gdal


# Setting num-threads to two since we aim to spin up $ALL_CPUS threads and send tile-import GDAL tasks
# to each thread - if we also configured each GDAL task to use $ALL_CPUS threads, then we would end up
# spinning up O($ALL_CPUS * $ALL_CPUS) threads, which doesn't make much sense.
# However, set it to 2 so each task can have a main thread and a compression thread.
gdal.SetConfigOption("GDAL_NUM_THREADS", "2")
gdal.SetConfigOption("GDAL_TIFF_INTERNAL_MASK", "TRUE")
gdal.SetConfigOption("INTERLEAVE_OVERVIEW", "PIXEL")
gdal.SetConfigOption("BIGTIFF_OVERVIEW", "IF_SAFER")
gdal.SetConfigOption("GDAL_TIFF_OVR_BLOCKSIZE", "512")


def convert_tile_to_cog(source, dest):
    """
    Converts any GeoTIFF file at source to a cloud-optimized GeoTIFF file at dest.
    """
    translate_options = gdal.TranslateOptions(
        format="COG",
        # Most of the GeoTIFF creation options don't affect the COG driver, so not much to put here.
        creationOptions=["BIGTIFF=IF_SAFER"],
    )

    gdal.Translate(str(dest), str(source), options=translate_options)

    assert dest.is_file()
