from osgeo import gdal

import os


KART_VRT_HEADING = os.linesep.join(
    [
        "<!-- Kart maintains this VRT file as a mosaic of every tile in this dataset. -->",
        "<!-- Any changes made to this file will be overwritten by Kart each time the dataset changes. -->",
        "<!-- To maintain your own VRT files in this folder, choose a different name. -->",
        "",
    ]
)


def write_vrt_for_directory(directory_path):
    """
    Given a folder containing some GeoTIFFs, write a mosaic file that combines them all into a single VRT.
    The VRT will contain references to the tiles, rather than replicating their contents.
    """
    tiles = [str(p) for p in directory_path.glob("*.tif")]
    if not tiles:
        return

    vrt_path = directory_path / f"{directory_path.name}.vrt"

    # We write the VRT file in-place ... then we re-write so we can prepend the KART_VRT_HEADING.
    # Trying to write it somewhere else is likely to mess up relative paths.

    vrt = gdal.BuildVRT(str(vrt_path), tiles)
    vrt.FlushCache()
    del vrt

    vrt_text = vrt_path.read_text()
    vrt_path.write_text(KART_VRT_HEADING + vrt_text)
