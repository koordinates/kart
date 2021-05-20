import os
import re
import socket
from urllib.parse import urlsplit, urlunsplit, urlencode, parse_qs


from sqlalchemy.pool import NullPool


class BaseDb:
    """Base functionality common to all types of sqlalchemy databases that we support."""

    @classmethod
    def create_engine(cls, spec):
        """Create an engine for connecting to the database specified."""
        raise NotImplementedError()

    @classmethod
    def _pool_class(cls):
        # Ordinarily, sqlalchemy engine's maintain a pool of connections ready to go.
        # When running tests, we run lots of kart commands, and each command creates an engine, and each engine maintains
        # a pool. This can quickly exhaust the database's allowed connection limit.
        # One fix would be to share engines between kart commands run during tests that are connecting to the same DB.
        # But this fix is simpler for now: disable the pool during testing.
        return NullPool if "PYTEST_CURRENT_TEST" in os.environ else None

    @classmethod
    def _replace_localhost_with_ip(cls, url_netloc):
        def _get_localhost_ip(*args, **kwargs):
            return socket.gethostbyname("localhost")

        return re.sub(r"\blocalhost\b", _get_localhost_ip, url_netloc)

    @classmethod
    def _append_query_to_url(cls, uri, new_query_dict):
        url = urlsplit(uri)
        url_query = cls._append_to_query(url.query, new_query_dict)
        return urlunsplit([url.scheme, url.netloc, url.path, url_query, ""])

    @classmethod
    def _append_to_query(cls, existing_query, new_query_dict):
        query_dict = parse_qs(existing_query)
        # ignore new keys if they're already set in the querystring
        return urlencode({**new_query_dict, **query_dict}, doseq=True)
