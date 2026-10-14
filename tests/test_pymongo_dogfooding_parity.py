"""Tests verifying 100% PyMongo compatibility gaps uncovered during dogfooding.

Addresses issues documented in neo-blogger:
1. GridFSBucket accepts neosqlite.Connection directly.
2. GridOutCursor supports chaining .sort(), .skip(), .limit(), .batch_size(), .to_list().
3. conn.fs.files.count_documents() and conn["fs.files"] work without 'no such column: data'.
4. Full-Text Search find() computes real BM25 textScore, supports projection
   {"score": {"$meta": "textScore"}}, sorting by textScore, and multi-field
   FTS ranking across title, subtitle, and body.
"""

from __future__ import annotations

import io

import neosqlite
from neosqlite.gridfs import GridFSBucket


class TestGridFSConnectionAndCursorParity:
    """Dogfooding Gap 1 & 2: GridFSBucket and GridOutCursor parity."""

    def test_gridfs_bucket_accepts_neosqlite_connection_directly(self):
        """GridFSBucket should accept neosqlite.Connection directly without conn.db."""
        conn = neosqlite.Connection(":memory:")
        bucket = GridFSBucket(conn)
        file_id = bucket.upload_from_stream(
            "sample.txt", io.BytesIO(b"Hello from dogfooding test!")
        )
        assert file_id is not None

        out = bucket.open_download_stream(file_id)
        assert out.read() == b"Hello from dogfooding test!"

    def test_grid_out_cursor_sort_skip_limit_to_list(self):
        """GridOutCursor should support fluent .sort(), .skip(), .limit(), and .to_list()."""
        conn = neosqlite.Connection(":memory:")
        bucket = GridFSBucket(conn)

        for i in range(10):
            bucket.upload_from_stream(
                f"doc_{i:02d}.txt",
                io.BytesIO(f"content {i}".encode()),
                metadata={"order": i, "category": "report"},
            )

        cursor = (
            bucket.find({"metadata.category": "report"})
            .sort("metadata.order", -1)
            .skip(2)
            .limit(3)
        )

        files = cursor.to_list()
        assert len(files) == 3
        # Should be doc_07, doc_06, doc_05 descending
        assert files[0].filename == "doc_07.txt"
        assert files[1].filename == "doc_06.txt"
        assert files[2].filename == "doc_05.txt"

    def test_grid_out_cursor_batch_size_and_iteration(self):
        """GridOutCursor should support .batch_size() and standard iteration."""
        conn = neosqlite.Connection(":memory:")
        bucket = GridFSBucket(conn)

        for i in range(5):
            bucket.upload_from_stream(
                f"item_{i}.bin",
                io.BytesIO(b"payload"),
            )

        cursor = bucket.find().sort("filename", 1).batch_size(2)
        filenames = []
        for grid_out in cursor:
            filenames.append(grid_out.filename)

        assert filenames == [f"item_{i}.bin" for i in range(5)]

    def test_gridfs_files_count_documents_with_filter(self):
        """conn.fs.files.count_documents() should work with metadata filter."""
        conn = neosqlite.Connection(":memory:")
        bucket = GridFSBucket(conn)

        bucket.upload_from_stream(
            "avatar1.png",
            io.BytesIO(b"avatar-1"),
            metadata={"owner": "alice", "type": "avatar"},
        )
        bucket.upload_from_stream(
            "avatar2.png",
            io.BytesIO(b"avatar-2"),
            metadata={"owner": "bob", "type": "avatar"},
        )
        bucket.upload_from_stream(
            "cover.png",
            io.BytesIO(b"cover-1"),
            metadata={"owner": "alice", "type": "cover"},
        )

        # Attribute-based collection access
        count_alice = conn.fs.files.count_documents({"metadata.owner": "alice"})
        assert count_alice == 2

        count_avatars = conn.fs.files.count_documents({"metadata.type": "avatar"})
        assert count_avatars == 2

        count_total = conn.fs.files.count_documents({})
        assert count_total == 3

    def test_gridfs_dot_notation_and_bracket_access(self):
        """conn['fs.files'] and conn['fs.chunks'] access should delegate correctly."""
        conn = neosqlite.Connection(":memory:")
        bucket = GridFSBucket(conn)

        bucket.upload_from_stream("test.txt", io.BytesIO(b"data"))

        files_coll = conn["fs.files"]
        chunks_coll = conn["fs.chunks"]

        assert files_coll.count_documents({}) == 1
        assert chunks_coll.count_documents({}) >= 1
        assert files_coll.name == "fs_files"
        assert chunks_coll.name == "fs_chunks"


class TestFullTextSearchScoreAndRankingParity:
    """Dogfooding Gap 3: BM25 textScore calculation, projection, and multi-field ranking."""

    def test_find_text_score_projection_and_sort(self):
        """find() should calculate BM25 score and sort by textScore."""
        conn = neosqlite.Connection(":memory:")
        articles = conn.articles

        articles.create_search_index("title")
        articles.create_search_index("body")

        articles.insert_many(
            [
                {
                    "_id": 1,
                    "title": "SQLite Parity in NeoSQLite",
                    "body": "Fast embedded database",
                },
                {
                    "_id": 2,
                    "title": "Unrelated Document",
                    "body": "Nothing about the search topic",
                },
                {
                    "_id": 3,
                    "title": "Database Overview",
                    "body": "SQLite is a popular relational database engine",
                },
            ]
        )

        results = list(
            articles.find(
                {"$text": {"$search": "SQLite"}},
                {"title": 1, "score": {"$meta": "textScore"}},
            ).sort([("score", {"$meta": "textScore"})])
        )

        assert len(results) == 2
        assert {results[0]["_id"], results[1]["_id"]} == {1, 3}
        assert "score" in results[0]
        assert "score" in results[1]
        assert results[0]["score"] > 0.0
        assert results[1]["score"] > 0.0
        assert results[0]["score"] >= results[1]["score"]
        assert "_textScore" not in results[0]

    def test_multi_field_fts_score_accumulation(self):
        """Documents matching multiple FTS fields should accumulate higher BM25 scores."""
        conn = neosqlite.Connection(":memory:")
        articles = conn.articles

        articles.create_search_index("title")
        articles.create_search_index("subtitle")
        articles.create_search_index("body")

        articles.insert_many(
            [
                {
                    "_id": 10,
                    "title": "Python Programming",
                    "subtitle": "Modern Python Guide",
                    "body": "Deep dive into Python language features",
                },
                {
                    "_id": 20,
                    "title": "Python Basics",
                    "subtitle": "Beginner Guide",
                    "body": "Learning syntax and variables",
                },
            ]
        )

        results = list(
            articles.find(
                {"$text": {"$search": "Python"}},
                {"title": 1, "score": {"$meta": "textScore"}},
            ).sort([("score", {"$meta": "textScore"})])
        )

        assert len(results) == 2
        # Doc 10 matched on title, subtitle, and body (3 fields)
        # Doc 20 matched only on title (1 field)
        assert results[0]["_id"] == 10
        assert results[1]["_id"] == 20
        assert results[0]["score"] > results[1]["score"]

    def test_find_text_search_without_score_projection_clean(self):
        """find() without score projection should not expose internal _textScore."""
        conn = neosqlite.Connection(":memory:")
        articles = conn.articles
        articles.create_search_index("title")

        articles.insert_one({"_id": 1, "title": "Hello World", "tags": ["test"]})

        docs = list(articles.find({"$text": {"$search": "Hello"}}))
        assert len(docs) == 1
        assert "_textScore" not in docs[0]
        assert docs[0] == {"_id": 1, "title": "Hello World", "tags": ["test"]}

    def test_find_text_search_pagination(self):
        """FTS query cursor should support skip() and limit() correctly."""
        conn = neosqlite.Connection(":memory:")
        articles = conn.articles
        articles.create_search_index("content")

        for i in range(10):
            articles.insert_one(
                {"_id": i, "content": f"article {i} search keyword match"}
            )

        page1 = list(
            articles.find({"$text": {"$search": "keyword"}})
            .sort("_id", 1)
            .limit(3)
        )
        assert len(page1) == 3
        assert [d["_id"] for d in page1] == [0, 1, 2]

        page2 = list(
            articles.find({"$text": {"$search": "keyword"}})
            .sort("_id", 1)
            .skip(3)
            .limit(3)
        )
        assert len(page2) == 3
        assert [d["_id"] for d in page2] == [3, 4, 5]

    def test_find_text_search_combined_with_regular_filters(self):
        """$text query combined with regular field conditions."""
        conn = neosqlite.Connection(":memory:")
        articles = conn.articles
        articles.create_search_index("title")

        articles.insert_many(
            [
                {"_id": 1, "title": "PyMongo Guide", "category": "db", "published": True},
                {"_id": 2, "title": "PyMongo Tutorial", "category": "db", "published": False},
                {"_id": 3, "title": "PyMongo Recipes", "category": "web", "published": True},
            ]
        )

        results = list(
            articles.find(
                {
                    "$text": {"$search": "PyMongo"},
                    "category": "db",
                    "published": True,
                },
                {"title": 1, "score": {"$meta": "textScore"}},
            ).sort([("score", {"$meta": "textScore"})])
        )

        assert len(results) == 1
        assert results[0]["_id"] == 1
        assert results[0]["title"] == "PyMongo Guide"
        assert results[0]["score"] > 0.0
