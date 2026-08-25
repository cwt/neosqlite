"""Regression test for #92: delete_by_name orphaned every chunk.

Chunks are keyed by the integer id column (files_id), but delete_by_name
collected the logical _id (ObjectId hex strings) for its chunk deletion,
so the DELETE matched zero rows and the byte payload leaked forever.
"""

import pytest

from neosqlite.gridfs import GridFSBucket
from neosqlite.gridfs.errors import NoFile


@pytest.fixture
def bucket(connection):
    return GridFSBucket(connection.db)


def test_delete_by_name_removes_all_chunks(bucket, connection):
    bucket.upload_from_stream("hello.txt", b"A" * 600_000)  # multi-chunk
    before = connection.db.execute(
        "SELECT COUNT(*) FROM fs_chunks"
    ).fetchone()[0]
    assert before > 1

    bucket.delete_by_name("hello.txt")

    after = connection.db.execute(
        "SELECT COUNT(*) FROM fs_chunks"
    ).fetchone()[0]
    files = connection.db.execute(
        "SELECT COUNT(*) FROM fs_files"
    ).fetchone()[0]
    assert after == 0, "chunks must be deleted with the file document"
    assert files == 0


def test_delete_by_name_missing_file_raises_no_file(bucket):
    with pytest.raises(NoFile):
        bucket.delete_by_name("does-not-exist.bin")


def test_multiple_versions_all_deleted_cleanly(bucket, connection):
    for i in range(3):
        bucket.upload_from_stream("f.txt", b"x" * 300_000)
    bucket.delete_by_name("f.txt")
    chunks = connection.db.execute(
        "SELECT COUNT(*) FROM fs_chunks"
    ).fetchone()[0]
    files = connection.db.execute(
        "SELECT COUNT(*) FROM fs_files"
    ).fetchone()[0]
    assert chunks == 0 and files == 0
