import logging


from kart.fast_import import (
    write_blob_to_stream,
)
from kart.lfs_util import dict_to_pointer_file_bytes
from kart.progress_util import progress_bar
from kart.tile.tilename_util import PAM_SUFFIX

L = logging.getLogger(__name__)


class LinkedTileImporter:
    """
    Subclassable logic for importing the metadata from tile-based datasets,
    while leaving the data in-place on existing hosted storage.
    """

    ALLOWED_SCHEMES = ("s3",)
    ALLOWED_SCHEMES_DESC = "an S3 URL"

    @property
    def extracting_tile_metadata_desc(self):
        return "Fetching tile metadata"

    def extract_tile_metadata(self, tile_location):
        # Implemented in subclasses.
        raise NotImplementedError()

    def get_conversion_func(self, source_metadata):
        return None

    def import_tiles_to_stream(self, stream, sources):
        progress = progress_bar(
            total=len(sources), unit="tile", desc="Writing tile metadata"
        )
        with progress as p:
            for source in sources:
                tilename = self.DATASET_CLASS.tilename_from_path(source)
                rel_blob_path = self.DATASET_CLASS.tilename_to_blob_path(
                    tilename, relative=True
                )
                blob_path = f"{self.dataset_inner_path}/{rel_blob_path}"

                # Check if tile has already been imported previously:
                if self.existing_dataset is not None:
                    existing_summary = self.existing_dataset.get_tile_summary(
                        tilename, missing_ok=True
                    )
                    if existing_summary:
                        source_oid = self.source_to_hash_and_size[source][0]
                        if self.existing_tile_matches_source(
                            source_oid, existing_summary
                        ):
                            # This tile has already been imported before. Reuse it rather than re-importing it.
                            # Re-importing it could cause it to be re-converted, which is a waste of time,
                            # and it may not convert the same the second time, which is then a waste of space
                            # and shows up as a pointless diff.
                            write_blob_to_stream(
                                stream,
                                blob_path,
                                (self.existing_dataset.inner_tree / rel_blob_path).data,
                            )
                            self.include_existing_metadata = True
                            continue

                # Tile hasn't been imported previously.
                tile_info = self.source_to_metadata[source]["tile"]
                pointer_data = dict_to_pointer_file_bytes(tile_info)
                write_blob_to_stream(stream, blob_path, pointer_data)
                if "pamOid" in tile_info:
                    pam_data = dict_to_pointer_file_bytes(
                        {
                            "url": tile_info["pamUrl"],
                            "oid": tile_info["pamOid"],
                            "size": tile_info["pamSize"],
                        }
                    )
                    write_blob_to_stream(stream, blob_path + PAM_SUFFIX, pam_data)
                p.update(1)

        self.source_to_imported_metadata = self.source_to_metadata

    def prompt_for_convert_to_cloud_optimized(self):
        return False

    def write_meta_blobs_to_stream(self, stream, merged_metadata):
        merged_metadata = {
            **merged_metadata,
            "linked-storage.json": {"urlRedirects": {}},
        }
        super().write_meta_blobs_to_stream(stream, merged_metadata)
