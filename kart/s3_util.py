from base64 import standard_b64decode
import functools
import os
from pathlib import Path
import tempfile
from urllib.parse import urlparse

import boto3

# Utility functions for dealing with S3 - not yet launched.


@functools.lru_cache(maxsize=1)
def get_s3_config():
    # TODO - add an option --s3-region to commands where it would be useful.
    return None


@functools.lru_cache(maxsize=1)
def get_s3_client():
    return boto3.client("s3", config=get_s3_config())


@functools.lru_cache(maxsize=1)
def get_s3_resource():
    return boto3.resource("s3", config=get_s3_config())


@functools.lru_cache()
def get_bucket(name):
    return get_s3_resource().Bucket(name)


def fetch_from_s3(s3_url, output_path=None):
    """
    Downloads the object at s3_url to output_path.
    If output-path is not set, creates a temporary file using tempfile.mkstemp()
    """
    # TODO: handle failure.
    parsed = urlparse(s3_url)
    bucket = get_bucket(parsed.netloc)
    if output_path is None:
        fd, path = tempfile.mkstemp()
        # If we keep it open, boto3 won't be able to write to it (on Windows):
        os.close(fd)
        output_path = Path(path)
    bucket.download_file(parsed.path.lstrip("/"), str(output_path.resolve()))
    return output_path


def expand_s3_glob(source_spec):
    """
    Given an s3_path with wildcard in, uses prefix and suffix matching to find all S3 objects that match.
    """
    # TODO: handle any kind of failure, sanity check to make sure we don't match a million objects.
    if "*" not in source_spec:
        yield source_spec
        return
    else:
        parsed = urlparse(source_spec)
        bucket = get_bucket(parsed.netloc)
        prefix, suffix = parsed.path.split("*", maxsplit=1)
        prefix = prefix.lstrip("/")
        matches = bucket.objects.filter(Prefix=prefix)
        for match in matches:
            if match.key.endswith(suffix):
                yield f"s3://{match.bucket_name}/{match.key}"


def get_hash_and_size_of_s3_object(s3_url):
    """Returns the (SHA256-hash-in-Base64, filesize) of an S3 object."""
    parsed = urlparse(s3_url)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    response = get_s3_client().get_object_attributes(
        Bucket=bucket,
        Key=key,
        ObjectAttributes=["Checksum", "ObjectSize"],
    )
    # TODO - handle failure (eg missing SHA256 checksum), which is extremely likely.
    sha256 = standard_b64decode(response["Checksum"]["ChecksumSHA256"]).hex()
    size = response["ObjectSize"]

    return sha256, size
