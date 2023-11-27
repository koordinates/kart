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
        return "Fetching tiles" if self.do_checkout else "Fetching tile metadata"

    def extract_tile_metadata(self, tile_location):
        if self.do_checkout:
            # Standard import flow will work - which fetches tiles first and extracts after.
            local_path, metadata = super().extract_tile_metadata(tile_location)
            metadata["tile"]["url"] = tile_location
            if "pamOid" in metadata["tile"]:
                metadata["tile"]["pamUrl"] = tile_location + PAM_SUFFIX
            return local_path, metadata
        else:
            # Linked-dataset specific flow which extracts metadata from remote tile.
            return None, self.extract_tile_metadata_from_s3(tile_location)

    def extract_tile_metadata_from_s3(self, tile_location):
        """Returns tile metadata without fetching the entire tile from S3."""
        # Overridden in subclasses.
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

                tile_info = self.source_to_metadata[source]["tile"]
                pointer_data = dict_to_pointer_file_bytes(tile_info)
                if self.do_checkout:
                    self.copy_file_to_local_lfs_cache(
                        source, oid_and_size=self.source_to_hash_and_size[source]
                    )
                write_blob_to_stream(stream, blob_path, pointer_data)

                if "pamOid" in tile_info:
                    pam_data = dict_to_pointer_file_bytes(
                        {
                            "url": tile_info["pamUrl"],
                            "oid": tile_info["pamOid"],
                            "size": tile_info["pamSize"],
                        }
                    )
                    if self.do_checkout:
                        self.copy_file_to_local_lfs_cache(
                            str(self.source_to_local_path.get(source)) + PAM_SUFFIX,
                            oid_and_size=self.source_to_hash_and_size[source],
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
