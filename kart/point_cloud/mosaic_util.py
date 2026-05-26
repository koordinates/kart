import logging

from tempfile import NamedTemporaryFile
from kart.subprocess_util import check_output


L = logging.getLogger("kart.point_cloud")


def write_vpc_for_directory(directory_path):
    """
    Given a folder containing some LAZ/LAS files, write a mosaic file that
    combines them all into a single Virtual Point Cloud (VPC). The VPC will
    contain references to the tiles, rather than replicating their contents.
    """
    vrt_path = directory_path / f"{directory_path.name}.vpc"

    tiles = [str(p) for p in directory_path.glob("*.copc.laz")]
    if not tiles:
        vrt_path.unlink(missing_ok=True)
        return

    with NamedTemporaryFile("w+t", suffix=".kart_tiles", encoding="utf-8") as tile_list:
        tile_list.write("\n".join(tiles))
        tile_list.flush()

        try:
            check_output(
                [
                    "pdal_wrench",
                    "build_vpc",
                    f"--output={vrt_path}",
                    f"--input-file-list={tile_list.name}",
                ]
            )
        except FileNotFoundError:
            L.warning("pdal_wrench not found. Skipping VPC generation.")
            return
