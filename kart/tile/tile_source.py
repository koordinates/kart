from pathlib import Path
import re
import uuid

import botocore

from kart import lfs_util
from kart.exceptions import InvalidOperation, INVALID_FILE_FORMAT
from kart.s3_util import (
    fetch_from_s3,
    get_error_code,
    get_lfs_oid_and_size_of_s3_object,
)
from kart.tile.tilename_util import find_similar_files_case_insensitive


class TileSource:
    """
    Represents a single tile to be imported.
    Helps keep track of various metadata associated with that tile -
    the tile's location, its local path if fetched, its metadata, its OID and size,
    its metadata once converted (if converted), any sidecar files.
    """

    URI_PATTERN = re.compile(r"([A-Za-z0-9-]{,20})://")

    def __init__(self, spec):
        self.spec = str(spec)
        self.scheme = self.parse_scheme(self.spec)
        if self.scheme is None:
            self.local_path = Path(self.spec)
        else:
            self.local_path = None

    def __repr__(self):
        return f"TileSource({self.spec})"

    @property
    def is_remote(self):
        return self.scheme is not None

    @classmethod
    def parse_scheme(cls, spec):
        m = cls.URI_PATTERN.match(spec)
        return m.group(1) if m else None

    def fetch_if_remote(self, tmpdir):
        """
        If this source is not already local, fetch it and write it to the given tmpdir.
        Sets self.local_path as the path to the local copy.
        """
        if self.local_path is not None:
            return

        # Currently S3 is the only supported remote scheme.
        assert self.scheme == "s3"

        self.local_path = tmpdir / str(uuid.uuid4())
        fetch_from_s3(self.spec, self.local_path)

    def find_or_fetch_sidecar_files(self, tmpdir, sidecar_files):
        """
        Populates self.local_sidecar_paths with the form {prefix: path_to_sidecar_file}
        eg {"pam": "path/to/file.tif.aux.xml"}
        Fetches sidecar files if they are remote.
        Populated self.local_sidecar_paths will be empty if there are no sidecar files.
        """

        self.local_sidecar_paths = {}

        if self.is_remote:
            assert self.scheme == "s3"
            local_path = self.local_path
            if not local_path:
                local_path = tmpdir / str(uuid.uuid4())

            for prefix, suffix in sidecar_files.items():
                try:
                    local_sidecar_path = local_path.with_name(local_path.name + suffix)
                    fetch_from_s3(self.spec + suffix, local_sidecar_path)
                    self.local_sidecar_paths[prefix] = local_sidecar_path
                except botocore.exceptions.ClientError as e:
                    if get_error_code(e) == 404:
                        # Not having any particular type of sidecar for any particular tile is allowed.
                        continue
                    else:
                        raise e

        else:
            for prefix, suffix in sidecar_files.items():
                sidecar_path = self.spec + suffix
                sidecars = find_similar_files_case_insensitive(sidecar_path)
                if len(sidecars) == 1:
                    self.local_sidecar_paths[prefix] = sidecars[0]
                elif len(sidecars) > 1:
                    detail = "\n".join(str(s) for s in sidecars)
                    raise InvalidOperation(
                        f"More than one {suffix} file found for {self.spec}:\n{detail}",
                        exit_code=INVALID_FILE_FORMAT,
                    )

    def extract_metadata(self, importer):
        """
        Extracts metadata from the tile, works for either local or remote sources.
        Populates self.metadata - this is the original metadata of the tile as found at the source,
        and self.oid_and_size - a tuple in the form (oid, size).
        """
        # Don't let the metadata extractor function search for the sidecar files - we've already fetched them.
        extra_kwargs = {}
        for prefix in importer.SIDECAR_FILES:
            extra_kwargs.update(
                {
                    f"{prefix}_path": self.local_sidecar_paths.get(prefix),
                    f"search_for_{prefix}": False,
                }
            )

        if self.local_path:
            self.metadata = importer.extract_tile_metadata(
                self.local_path, **extra_kwargs
            )
        else:
            assert self.scheme == "s3"
            oid_and_size = get_lfs_oid_and_size_of_s3_object(self.spec)
            self.metadata = importer.extract_tile_metadata(
                self.spec, oid_and_size=oid_and_size, **extra_kwargs
            )
        tile_metadata = self.metadata["tile"]
        self.oid_and_size = tile_metadata["oid"], tile_metadata["size"]
        return self.metadata

    @property
    def oid(self):
        return self.oid_and_size[0]

    def copy_or_convert(self, importer):
        """
        Copies the source tile to the LFS cache, or converts the source tile and puts the
        converted version in the LFS cache. Does not copy or convert if importer.do_fetch_tiles is False.
        Sets self.imported_metadata and self.imported_oid_and_size:
        these will be the same as self.metadata and self.oid_and_size if no conversion is performed,
        and different if a conversion has been performed.
        """
        if not importer.do_fetch_tiles:
            self.imported_metadata = self.metadata
            self.imported_oid_and_size = self.oid_and_size
            return self.imported_metadata

        self.conversion_func = importer.get_conversion_func(self)
        preserve_original = not self.is_remote
        oid_and_size = self.oid_and_size if self.conversion_func is None else None
        pointer_dict = lfs_util.copy_file_to_local_lfs_cache(
            importer.repo,
            self.local_path,
            conversion_func=self.conversion_func,
            oid_and_size=oid_and_size,
            preserve_original=preserve_original,
        )
        if self.conversion_func is None:
            self.imported_oid_and_size = self.oid_and_size
            self.imported_metadata = self.metadata
        else:
            self.imported_oid_and_size = pointer_dict["oid"], pointer_dict["size"]
            lfs_path = lfs_util.get_local_path_from_lfs_oid(
                importer.repo, pointer_dict["oid"]
            )
            self.imported_metadata = importer.extract_tile_metadata(
                lfs_path, oid_and_size=self.imported_oid_and_size
            )
            self.imported_metadata["tile"]["sourceOid"] = self.oid

        for local_sidecar_path in self.local_sidecar_paths.values():
            lfs_util.copy_file_to_local_lfs_cache(importer.repo, local_sidecar_path)

        return self.imported_metadata

    def cleanup(self):
        if not self.is_remote:
            return

        if self.local_path is not None:
            self.local_path.unlink(missing_ok=True)
        for local_sidecar_path in self.local_sidecar_paths.values():
            local_sidecar_path.unlink(missing_ok=True)
