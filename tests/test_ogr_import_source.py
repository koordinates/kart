import pytest

from sno.ogr_import_source import ImportPostgreSQL


def test_postgres_url_parsing():
    func = ImportPostgreSQL.postgres_url_to_ogr_conn_str
    with pytest.raises(ValueError):
        func('https://example.com/')

    # this is valid (AFAICT), means connect on the default domain socket
    # to the default database with the current posix user
    assert func('postgres://') == 'PG:'
    assert func('postgresql://') == 'PG:'
    assert func('postgres:///') == 'PG:'

    assert func('postgres://myhost/') == 'PG:host=myhost'
    assert func('postgres:///?host=myhost') == 'PG:host=myhost'
    # querystring params take precedence
    assert func('postgres://otherhost/?host=myhost') == 'PG:host=myhost'

    assert func('postgres://myhost:1234/') == 'PG:host=myhost port=1234'
    assert func('postgres://myhost/?port=1234') == 'PG:host=myhost port=1234'
    assert (
        func('postgres://myhost/dbname?port=1234')
        == 'PG:dbname=dbname host=myhost port=1234'
    )
    # everything, including extra options
    assert (
        func('postgres://u:p@h:1234/d?client_encoding=utf16')
        == 'PG:client_encoding=utf16 dbname=d host=h password=p port=1234 user=u'
    )

    # domain socket hostnames starting with a '/' are handled, but
    # must be url-quoted if they're part of the URL
    assert (
        func('postgres://%2Fvar%2Flib%2Fpostgresql/d')
        == 'PG:dbname=d host=/var/lib/postgresql'
    )
    # but url-quoting seems to be optional when they're in the querystring
    assert (
        func('postgres:///?host=/var/lib/postgresql') == 'PG:host=/var/lib/postgresql'
    )
    assert (
        func('postgres:///?host=%2Fvar%2Flib%2Fpostgresql')
        == 'PG:host=/var/lib/postgresql'
    )
