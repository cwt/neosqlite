"""Regression test for #109: revisions below -1 returned the OLDEST
version because negative OFFSET was clamped to 0 by the ASC branch."""

import time

import pytest

from neosqlite.gridfs import GridFSBucket
from neosqlite.gridfs.errors import NoFile


@pytest.fixture
def bucket(connection):
    b = GridFSBucket(connection.db)
    for tag in ("v1", "v2", "v3"):
        b.upload_from_stream("f.txt", f"data-{tag}".encode())
        time.sleep(0.02)
    return b


@pytest.mark.parametrize(
    "revision,expected",
    [(0, "data-v1"), (1, "data-v2"), (2, "data-v3"),
     (-1, "data-v3"), (-2, "data-v2"), (-3, "data-v1")],
)
def test_revisions(bucket, revision, expected):
    out = bucket.open_download_stream_by_name("f.txt", revision=revision)
    assert out.read().decode() == expected


def test_out_of_range_raises_no_file(bucket):
    with pytest.raises(NoFile):
        bucket.open_download_stream_by_name("f.txt", revision=99)
