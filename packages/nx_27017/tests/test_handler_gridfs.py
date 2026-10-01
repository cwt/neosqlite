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

    def test_gridfs_find_by_objectid_filter(self, handler):
        """Test GridFS find by _id using dict with $oid."""
        find_all_msg = {
            "request_id": 1,
            "sections": [
                ("body", {"find": "fs.files", "filter": {}, "$db": "test"})
            ],
        }
        _, response = handler.handle_command(find_all_msg)
        assert response["ok"] == 1
        file_id = response["cursor"]["firstBatch"][0]["_id"]

        find_oid_msg = {
            "request_id": 2,
            "sections": [
                (
                    "body",
                    {
                        "find": "fs.files",
                        "filter": {"_id": {"$oid": str(file_id)}},
                        "$db": "test",
                    },
                )
            ],
        }
        _, response2 = handler.handle_command(find_oid_msg)
        assert response2["ok"] == 1
        assert len(response2["cursor"]["firstBatch"]) == 1
        assert response2["cursor"]["firstBatch"][0]["_id"] == file_id

    def test_gridfs_update_file_metadata(self, handler):
        """Test GridFS update command on fs.files."""
        find_all_msg = {
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
        _, response = handler.handle_command(find_all_msg)
        assert response["ok"] == 1
        file_id = response["cursor"]["firstBatch"][0]["_id"]

        update_msg = {
            "request_id": 2,
            "sections": [
                (
                    "body",
                    {
                        "update": "fs.files",
                        "updates": [
                            {
                                "q": {"_id": file_id},
                                "u": {
                                    "$set": {
                                        "filename": "renamed_test.txt",
                                        "metadata": {"author": "neo"},
                                    }
                                },
                            }
                        ],
                        "$db": "test",
                    },
                )
            ],
        }
        _, upd_res = handler.handle_command(update_msg)
        assert upd_res["ok"] == 1
        assert upd_res["n"] == 1

        find_updated_msg = {
            "request_id": 3,
            "sections": [
                (
                    "body",
                    {
                        "find": "fs.files",
                        "filter": {"_id": file_id},
                        "$db": "test",
                    },
                )
            ],
        }
        _, find_upd_res = handler.handle_command(find_updated_msg)
        assert find_upd_res["ok"] == 1
        assert (
            find_upd_res["cursor"]["firstBatch"][0]["filename"]
            == "renamed_test.txt"
        )


class TestGridFSDottedRouting:
    """Dotted fs.files/fs.chunks names route to the bucket (P1-8)."""

    def _cmd(self, handler, body, req=10):
        _, res = handler.handle_command(
            {"request_id": req, "sections": [("body", body)]}
        )
        return res

    def test_drop_dotted_collections(self, handler):
        res = self._cmd(handler, {"drop": "fs.files", "$db": "test"}, req=11)
        assert res == {"ok": 1}
        res = self._cmd(handler, {"drop": "fs.chunks", "$db": "test"}, req=12)
        assert res == {"ok": 1}
        # Idempotent like real MongoDB.
        res = self._cmd(handler, {"drop": "fs.files", "$db": "test"}, req=13)
        assert res == {"ok": 1}

    def test_distinct_filename(self, handler):
        res = self._cmd(
            handler,
            {
                "distinct": "fs.files",
                "key": "filename",
                "query": {},
                "$db": "test",
            },
            req=14,
        )
        assert res["ok"] == 1
        assert sorted(res["values"]) == ["data.json", "test.txt"]

    def test_find_sort_direction_desc(self, handler):
        res = self._cmd(
            handler,
            {
                "find": "fs.files",
                "filter": {},
                "sort": {"uploadDate": -1},
                "$db": "test",
            },
            req=15,
        )
        docs = res["cursor"]["firstBatch"]
        dates = [doc["uploadDate"] for doc in docs]
        assert dates == sorted(dates, reverse=True)

    def test_delete_by_filename_removes_files_and_chunks(self, handler):
        """Legacy GridFS.delete_by_name: fs.files delete with a
        filename filter removes every version plus its chunks."""
        from neosqlite.gridfs import GridFSBucket

        db = handler.get_database("test")
        bucket = GridFSBucket(db.db, bucket_name="fs")
        bucket.upload_from_stream("delme.txt", b"bye")
        chunk_count_before = db.db.execute(
            "SELECT COUNT(*) FROM fs_chunks"
        ).fetchone()[0]
        assert chunk_count_before > 0

        res = self._cmd(
            handler,
            {
                "delete": "fs.files",
                "deletes": [{"q": {"filename": "delme.txt"}, "limit": 0}],
                "$db": "test",
            },
            req=16,
        )
        assert res["ok"] == 1
        assert res["n"] >= 1

        remaining_files = self._cmd(
            handler,
            {
                "find": "fs.files",
                "filter": {"filename": "delme.txt"},
                "$db": "test",
            },
            req=17,
        )
        assert remaining_files["cursor"]["firstBatch"] == []
        assert bucket.list() == ["data.json", "test.txt"]

    def test_chunks_delete_by_files_id(self, handler):
        """PyMongo chunk cleanup deletes fs.chunks rows by files_id."""
        db = handler.get_database("test")
        find_res = self._cmd(
            handler,
            {
                "find": "fs.files",
                "filter": {"filename": "test.txt"},
                "$db": "test",
            },
            req=18,
        )
        file_doc = find_res["cursor"]["firstBatch"][0]
        file_id = file_doc["_id"]
        before = db.db.execute(
            "SELECT COUNT(*) FROM fs_chunks WHERE files_id = ?",
            (
                db.db.execute(
                    "SELECT id FROM fs_files WHERE _id = ?", (str(file_id),)
                ).fetchone()[0],
            ),
        ).fetchone()[0]
        assert before > 0

        res = self._cmd(
            handler,
            {
                "delete": "fs.chunks",
                "deletes": [{"q": {"files_id": file_id}, "limit": 0}],
                "$db": "test",
            },
            req=19,
        )
        assert res["ok"] == 1
        assert res["n"] == before

    def test_upload_with_id_via_wire(self, handler):
        """bucket.upload_with_id == insert files doc, then chunks."""
        from neosqlite.gridfs import GridFSBucket
        from neosqlite.objectid import ObjectId

        oid_hex = "e" * 24
        _, files_res = handler.handle_insert(
            {
                "request_id": 20,
                "sections": [
                    ("body", {"insert": "fs.files", "$db": "test"}),
                    (
                        "payload_docs",
                        [
                            {
                                "_id": {"$oid": oid_hex},
                                "filename": "withid.txt",
                                "length": 5,
                                "chunkSize": 261120,
                                "uploadDate": "2026-10-01T00:00:00Z",
                                "md5": None,
                                "metadata": {"note": "wire"},
                            }
                        ],
                    ),
                ],
            }
        )
        assert files_res["ok"] == 1
        _, chunks_res = handler.handle_insert(
            {
                "request_id": 21,
                "sections": [
                    ("body", {"insert": "fs.chunks", "$db": "test"}),
                    (
                        "payload_docs",
                        [
                            {
                                "files_id": {"$oid": oid_hex},
                                "n": 0,
                                "data": b"Hello",
                            }
                        ],
                    ),
                ],
            }
        )
        assert chunks_res["ok"] == 1

        db = handler.get_database("test")
        bucket = GridFSBucket(db.db, bucket_name="fs")
        stream = bucket.open_download_stream(ObjectId(oid_hex))
        assert stream.read() == b"Hello"
