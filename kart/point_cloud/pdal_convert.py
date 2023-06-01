from kart.exceptions import InvalidOperation, INVALID_FILE_FORMAT
from kart.point_cloud.metadata_util import is_copc, get_las_version
from kart.point_cloud import pdal_execute_pipeline
from kart import subprocess_util as subprocess


def convert_tile_to_format(source, dest, target_format):
    """
    Converts some sort of a .las/.laz file at source to a tile of the given format at dest.
    """
    if is_copc(target_format):
        return convert_tile_to_copc(source, dest)
    else:
        return convert_tile_to_laz(source, dest, target_format)


def convert_tile_to_copc(source, dest):
    """
    Converts some sort of a .las/.laz file at source to a .copc.laz file at dest.
    """
    pipeline = [
        {
            "type": "readers.las",
            "filename": str(source),
        },
        {
            "type": "writers.copc",
            "filename": str(dest),
            "forward": "all",
        },
    ]
    try:
        pdal_execute_pipeline(pipeline)
    except subprocess.CalledProcessError as e:
        raise InvalidOperation(
            f"Error converting {source}\n{e}", exit_code=INVALID_FILE_FORMAT
        )

    assert dest.is_file()


def convert_tile_to_laz(source, dest, target_format):
    """
    Converts some sort of .las/.laz at source to some sort of .laz file at dest.
    """
    major_version, minor_version = get_las_version(target_format).split(".", maxsplit=1)

    pipeline = [
        {
            "type": "readers.las",
            "filename": str(source),
        },
        {
            "type": "writers.las",
            "filename": str(dest),
            "forward": "all",
            "compression": True,
            "major_version": major_version,
            "minor_version": minor_version,
        },
    ]
    try:
        pdal_execute_pipeline(pipeline)
    except subprocess.CalledProcessError as e:
        raise InvalidOperation(
            f"Error converting {source}\n{e}", exit_code=INVALID_FILE_FORMAT
        )
    assert dest.is_file()
