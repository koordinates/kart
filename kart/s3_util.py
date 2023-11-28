from base64 import standard_b64decode
import concurrent.futures
import logging
import functools
import os
from pathlib import Path
import tempfile
from threading import current_thread
from urllib.parse import urlparse

import boto3
import click

from kart.exceptions import NotFound, NO_IMPORT_SOURCE, NO_CHECKSUM
from kart.lfs_util import get_oid_and_size_of_file
from kart.progress_util import progress_bar

# Utility functions for dealing with S3 - not yet launched.

L = logging.getLogger("kart.s3_util")


@functools.lru_cache()
def get_region(bucket):
    """Returns the name of the S3 region that a particular bucket is in."""

    # The region -> bucket cache is not thread-local, since the region a bucket is in doesn't depend
    # on which thread we are currently using.
    if not bucket:
        return None
    try:
        response = get_s3_client().head_bucket(Bucket=bucket)
        return response["ResponseMetadata"]["HTTPHeaders"]["x-amz-bucket-region"]
    except Exception as e:
        L.warning("Couldn't find S3 region for bucket %s: %s", bucket, e)
        # We don't necessarily need to know which region a bucket is in -
        # - we try to configure S3 clients to default to connecting to the right region, for efficiency
        # - but even if this doesn't work, the overall operation might still work. We'll keep going.
        return None


def threadlocal_lru_cache(*decorator_args, **decorator_kwargs):
    """
    Decorator that works just like functools.lru_cache, but stores the hash of the calling thread
    as part of the cache-key, so that each thread effectively gets a (albeit smaller) separate cache.

    Used heavily here since boto3 sessions and resources are not guaranteed thread-safe.
    """

    def _threadlocal_lru_cache(user_func):
        @functools.lru_cache(*decorator_args, **decorator_kwargs)
        def caching_func(*args, thread_hash=None, **kwargs):
            return user_func(*args, **kwargs)

        @functools.wraps(user_func)
        def wrapping_func(*args, **kwargs):
            return caching_func(*args, thread_hash=hash(current_thread()), **kwargs)

        return wrapping_func

    return _threadlocal_lru_cache


def add_bucket_kwarg():
    """
    Decorator that adds a `bucket=None` kwarg to a function definition that already
    has a `region` kwarg. If the region kwarg is not set and the bucket is set,
    then the wrapped function will have its region kwarg set to the region of the bucket
    using get_region(bucket).

    This decorator goes *before* threadlocal_lru_cache since the aim is to have one client
    per region per thread - there is no need to have one decorator per bucket.
    """

    def _add_bucket_kwarg(user_func):
        @functools.wraps(user_func)
        def wrapping_func(*args, region=None, bucket=None, **kwargs):
            return user_func(*args, region=region or get_region(bucket), **kwargs)

        return wrapping_func

    return _add_bucket_kwarg


@add_bucket_kwarg()
@threadlocal_lru_cache()
def get_s3_session(*, region=None):
    return boto3.session.Session(region_name=region)


@add_bucket_kwarg()
@threadlocal_lru_cache()
def get_s3_client(*, region=None):
    client = get_s3_session(region=region).client("s3")
    if "AWS_NO_SIGN_REQUEST" in os.environ:
        client._request_signer.sign = lambda *args, **kwargs: None
    return client


@add_bucket_kwarg()
@threadlocal_lru_cache()
def get_s3_resource(*, region=None, bucket=None):
    resource = get_s3_session(region=region).resource("s3")
    if "AWS_NO_SIGN_REQUEST" in os.environ:
        resource.meta.client._request_signer.sign = lambda *args, **kwargs: None
    return resource


@threadlocal_lru_cache()
def get_s3_bucket(bucket):
    return get_s3_resource(bucket=bucket).Bucket(bucket)


def parse_s3_url(s3_url):
    parsed = urlparse(s3_url)
    assert parsed.scheme == "s3"
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    return bucket, key


def fetch_from_s3(s3_url, output_path=None, sha256_hash=None):
    """
    Downloads the object at s3_url to output_path.
    If output-path is not set, creates a temporary file using tempfile.mkstemp()
    If sha_256 hash is set, verifies that the downloaded file has the expected hash -
        if it does not, deletes the downloaded file and raises a ValueError.
    """
    bucket, key = parse_s3_url(s3_url)
    if output_path is None:
        fd, output_path = tempfile.mkstemp()
        # If we keep it open, boto3 won't be able to write to it (on Windows):
        os.close(fd)
    output_path = Path(output_path).resolve()
    get_s3_bucket(bucket).download_file(key, str(output_path))

    if sha256_hash:
        actual_hash, size = get_oid_and_size_of_file(output_path)
        if actual_hash != sha256_hash:
            output_path.unlink()
            raise ValueError(
                f"Checksum verification failed on file downloaded from {s3_url}"
            )

    return output_path


# Fetching from S3 and writing to disk is I/O bound, so, we don't have a theoretical way of deciding how many threads to
# use (if it was compute bound, then $NUM_CORES threads would be a good place to start).
# But, in practise, 8 threads seems to be fast.
_FETCH_MULTIPLE_FROM_S3_WORKER_COUNT = 8


def fetch_multiple_from_s3(s3_urls_and_paths, quiet=False):
    """
    Given a list of tuples [(s3_url, pathlib_Path, sha256_hash), ...] downloads each URL to the given output path,
    and verifies that the downloaded file has the appropriate hash, using _FETCH_MULTIPLE_FROM_S3_WORKER_COUNT worker threads.
    The sha_256 is optional, it can be set to None or ommitted from the tuple entirely.

    Displays a progress bar unless disabled using quiet=True.
    """
    disable = True if quiet else None
    progress = progress_bar(
        total=len(s3_urls_and_paths),
        unit="object",
        desc="Fetching S3 objects",
        disable=disable,
    )

    with progress as p, concurrent.futures.ThreadPoolExecutor(
        max_workers=_FETCH_MULTIPLE_FROM_S3_WORKER_COUNT
    ) as executor:
        futures = [executor.submit(fetch_from_s3, *args) for args in s3_urls_and_paths]
        for future in concurrent.futures.as_completed(futures):
            future.result()  # Raises any exception that occurred in the worker thread.
            p.update(1)


def expand_s3_glob(source_spec):
    """
    Given an s3_path with '*' wildcard in, uses prefix and suffix matching to find all S3 objects that match.
    Subdirectories (or the S3 equivalent - S3 is not exactly a directory hierarchy) are not matched -
    that is, s3://bucket/path/*.txt matches s3://bucket/path/example.txt but not s3://bucket/path/subpath/example.txt
    """
    if "*" not in source_spec:
        return [source_spec]

    bucket, key = parse_s3_url(source_spec)
    if "*" in bucket:
        raise click.UsageError(
            "Wildcard '*' should only be in key part of s3 URL, not in bucket"
        )
    if "*" not in key:
        return [source_spec]

    prefix, suffix = key.split("*", maxsplit=1)
    if "*" in suffix:
        raise click.UsageError(
            f"Two wildcards '*' found in {source_spec} - only one wildcard is supported"
        )
    prefix = prefix.lstrip("/")
    prefix_len = len(prefix)

    matches = get_s3_bucket(bucket).objects.filter(Prefix=prefix)
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


def get_lfs_oid_and_size_of_s3_object(s3_url):
    """Returns a tuple (LFS-OID, filesize) of an S3 object."""
    bucket, key = parse_s3_url(s3_url)
    response = get_s3_client(bucket=bucket).head_object(
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
