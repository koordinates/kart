from io import BytesIO
from kart.utils import iter_records_from_file


def test_iter_records_from_file():
    # basic case
    f = BytesIO(b"foo\0bar\0baz\0")
    assert list(iter_records_from_file(f, b"\0")) == [b"foo", b"bar", b"baz", b""]
    # separator is customisable
    f = BytesIO(b"foo\0bar\0baz\0")
    assert list(iter_records_from_file(f, b"b")) == [b"foo\0", b"ar\0", b"az\0"]
    # exhausted file means no more records
    assert list(iter_records_from_file(f, b"\0")) == []
    # no trailing separator means the final chunk isn't empty
    f = BytesIO(b"foo\0bar\0baz")
    assert list(iter_records_from_file(f, b"\0")) == [b"foo", b"bar", b"baz"]
    # if the chunk size is silly then it still works
    f = BytesIO(b"foo\0bar\0baz")
    assert list(iter_records_from_file(f, b"\0", chunk_size=1)) == [
        b"foo",
        b"bar",
        b"baz",
    ]
    # if the chunk size matches the position of the separator then it still works
    f = BytesIO(b"foo\0bar\0baz")
    assert list(iter_records_from_file(f, b"\0", chunk_size=3)) == [
        b"foo",
        b"bar",
        b"baz",
    ]
    # if the chunk size matches the position after the separator then it still works
    f = BytesIO(b"foo\0bar\0baz")
    assert list(iter_records_from_file(f, b"\0", chunk_size=4)) == [
        b"foo",
        b"bar",
        b"baz",
    ]
