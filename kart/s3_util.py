from base64 import standard_b64decode
import functools
import os
from pathlib import Path
import tempfile
from urllib.parse import urlparse

import boto3
import click

from kart.exceptions import NotFound, NO_IMPORT_SOURCE, NO_CHECKSUM

# Utility functions for dealing with S3 - not yet launched.


@functools.lru_cache(maxsize=1)
def get_s3_config():
    # TODO - add an option --s3-region to commands where it would be useful.
    return None


@functools.lru_cache(maxsize=1)
def get_s3_client():
    client = boto3.client("s3", config=get_s3_config())
    if "AWS_NO_SIGN_REQUEST" in os.environ:
        client._request_signer.sign = lambda *args, **kwargs: None
    return client


@functools.lru_cache(maxsize=1)
def get_s3_resource():
    resource = boto3.resource("s3", config=get_s3_config())
    if "AWS_NO_SIGN_REQUEST" in os.environ:
        resource.meta.client._request_signer.sign = lambda *args, **kwargs: None
    return resource


@functools.lru_cache(maxsize=1)
def get_region_name():
    return get_s3_client().meta.config.region_name


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
    Given an s3_path with '*' wildcard in, uses prefix and suffix matching to find all S3 objects that match.
    Subdirectories (or the S3 equivalent - S3 is not exactly a directory hierarchy) are not matched -
    that is, s3://bucket/path/*.txt matches s3://bucket/path/example.txt but not s3://bucket/path/subpath/example.txt
    """
    # TODO: handle any kind of failure, sanity check to make sure we don't match a million objects.
    if "*" not in source_spec:
        return [source_spec]

    parsed = urlparse(source_spec)
    prefix, suffix = parsed.path.split("*", maxsplit=1)
    if "*" in suffix:
        raise click.UsageError(
            f"Two wildcards '*' found in {source_spec} - only one wildcard is supported"
        )
    prefix = prefix.lstrip("/")
    prefix_len = len(prefix)

    bucket = get_bucket(parsed.netloc)
    matches = bucket.objects.filter(Prefix=prefix)
    result = []
    for match in matches:
        assert match.key.startswith(prefix)
        match_suffix = match.key[prefix_len:]
        if match_suffix.endswith(suffix) and "/" not in match_suffix:
            result.append(f"s3://{match.bucket_name}/{match.key}")

    if not result:
        raise NotFound(
            f"No S3 objects found at {source_spec}", exit_code=NO_IMPORT_SOURCE
        )
    return result


def get_hash_and_size_of_s3_object(s3_url):
    """Returns the (SHA256-hash-in-Base64, filesize) of an S3 object."""
    parsed = urlparse(s3_url)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    response = get_s3_client().head_object(
        Bucket=bucket, Key=key, ChecksumMode="ENABLED"
    )
    # TODO: fall back to other ways of learning the checksum.
    if "ChecksumSHA256" not in response:
        raise NotFound(
            f"Object at {s3_url} has no SHA256 checksum attached. "
            "See https://docs.kartproject.org/en/latest/pages/s3.html#sha256-hashes",
            exit_code=NO_CHECKSUM,
        )
    sha256 = standard_b64decode(response["ChecksumSHA256"]).hex()
    size = response["ContentLength"]
    return sha256, size


def get_error_code(client_error):
    response = getattr(client_error, "response")
    error = response.get("Error") if response else None
    code = error.get("Code") if error else None
    if code and code.isdigit():
        code = int(code)
    return code
