"""Tests for GridFS operations."""

import pytest


@pytest.fixture
def handler(tmp_path):
    from nx_27017.nx_27017 import NeoSQLiteHandler

    from neosqlite.gridfs import GridFSBucket

    db_path = str(tmp_path / "gridfs_test.db")
    h = NeoSQLiteHandler(db_path)

    db = h.get_database("test")
    bucket = GridFSBucket(db.db, bucket_name="fs")
    bucket.upload_from_stream("test.txt", b"Hello GridFS World!")
    bucket.upload_from_stream("data.json", b'{"key": "value"}')

    yield h
    h.conn.close()


class TestGridFSOperations:
    """Test GridFS operations through NX-27017 handler."""

    def test_gridfs_find_via_op_msg(self, handler):
        """Test GridFS find through OP_MSG (handle_command)."""
        find_msg = {
            "request_id": 1,
            "sections": [
                ("body", {"find": "fs.files", "filter": {}, "$db": "test"})
            ],
        }
        _, response = handler.handle_command(find_msg)
        assert response["ok"] == 1
        assert "cursor" in response
        assert len(response["cursor"]["firstBatch"]) == 2

        filenames = {f["filename"] for f in response["cursor"]["firstBatch"]}
        assert "test.txt" in filenames
        assert "data.json" in filenames

    def test_gridfs_find_via_op_query(self, handler):
        """Test GridFS find through OP_QUERY (handle_query)."""
        find_msg = {
            "request_id": 1,
            "query": {"find": "fs.files", "filter": {}},
            "collection": "fs.files",
            "db": "test",
        }
        _, docs = handler.handle_query(find_msg)
        assert len(docs) == 2

        filenames = {f["filename"] for f in docs}
        assert "test.txt" in filenames
        assert "data.json" in filenames

    def test_gridfs_find_with_filter(self, handler):
        """Test GridFS find with filename filter."""
        find_msg = {
            "request_id": 1,
            "sections": [
                (
                    "body",
                    {
                        "find": "fs.files",
                        "filter": {"filename": "test.txt"},
                        "$db": "test",
                    },
                )
            ],
        }
        _, response = handler.handle_command(find_msg)
        assert response["ok"] == 1
        assert len(response["cursor"]["firstBatch"]) == 1
        assert response["cursor"]["firstBatch"][0]["filename"] == "test.txt"

    def test_gridfs_delete(self, handler):
        """Test GridFS delete through OP_MSG."""
        find_msg = {
            "request_id": 1,
            "sections": [
                (
                    "body",
                    {
                        "find": "fs.files",
                        "filter": {"filename": "test.txt"},
                        "$db": "test",
                    },
                )
            ],
        }
        _, response = handler.handle_command(find_msg)
        file_id = response["cursor"]["firstBatch"][0]["_id"]

        delete_msg = {
            "request_id": 2,
            "sections": [
                (
                    "body",
                    {
                        "delete": "fs.files",
                        "deletes": [{"q": {"_id": file_id}, "limit": 1}],
                        "$db": "test",
                    },
                )
            ],
        }
        _, response = handler.handle_command(delete_msg)
        assert response["ok"] == 1
        assert response["n"] == 1

        _, response = handler.handle_command(find_msg)
        assert len(response["cursor"]["firstBatch"]) == 0

    def test_gridfs_upload_and_download(self, handler):
        """Test GridFS upload and download via NX-27017 handlers."""
        from neosqlite.gridfs import GridFSBucket

        db = handler.get_database("test")
        bucket = GridFSBucket(db.db, bucket_name="fs")
        bucket.upload_from_stream("newfile.txt", b"New content here!")

        find_msg = {
            "request_id": 1,
            "sections": [
                (
                    "body",
                    {
                        "find": "fs.files",
                        "filter": {"filename": "newfile.txt"},
                        "$db": "test",
                    },
                )
            ],
        }
        _, response = handler.handle_command(find_msg)
        assert len(response["cursor"]["firstBatch"]) == 1
        assert response["cursor"]["firstBatch"][0]["length"] == 17

    def test_gridfs_list_collections_includes_gridfs(self, handler):
        """Test that listCollections shows GridFS collections.

        Note: SQLite stores GridFS as fs_files and fs_chunks (underscore),
        not fs.files and fs.chunks (dot). The listCollections returns
        the actual table names as stored in SQLite.
        """
        list_msg = {
            "request_id": 1,
            "sections": [("body", {"listCollections": 1, "$db": "test"})],
        }
        _, response = handler.handle_command(list_msg)
        assert response["ok"] == 1
        coll_names = [c["name"] for c in response["cursor"]["firstBatch"]]
        assert "fs_files" in coll_names
        assert "fs_chunks" in coll_names
