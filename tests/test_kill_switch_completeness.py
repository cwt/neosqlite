"""Comprehensive parity, GridFS decoupling, and SQL-trace tests for kill-switch completeness."""

from __future__ import annotations

import os
import re
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

UTC = timezone.utc  # noqa: UP017

import neosqlite
from neosqlite.collection.query_helper import (
    get_force_fallback,
    set_force_fallback,
)
from neosqlite.gridfs import GridFSBucket
from neosqlite.requests import DeleteOne, ReplaceOne, UpdateOne


@pytest.fixture(autouse=True)
def _reset_force_fallback():
    """Ensure force-fallback kill switch is always reset after each test."""
    original = get_force_fallback()
    try:
        yield
    finally:
        set_force_fallback(original)


def _seed_docs(coll):
    coll.insert_many(
        [
            {
                "_id": 1,
                "name": "Alice",
                "status": "active",
                "score": 10,
                "threshold": 5,
                "nested": {"b": 100},
                "tags": ["a", {"k": 1}],
                "created_at": datetime(2025, 1, 10, tzinfo=UTC),
            },
            {
                "_id": 2,
                "name": "Bob",
                "status": "active",
                "score": 25,
                "threshold": 20,
                "nested": {"b": 200},
                "tags": ["b", "a"],
                "created_at": datetime(2025, 2, 15, tzinfo=UTC),
            },
            {
                "_id": 3,
                "name": "Charlie",
                "status": "inactive",
                "score": 30,
                "threshold": 10,
                "nested": {"b": 100},
                "tags": [{"k": 1}, "c"],
                "created_at": datetime(2025, 3, 20, tzinfo=UTC),
            },
            {
                "_id": 4,
                "name": "Diana",
                "status": "active",
                "score": 15,
                "threshold": 15,
                "nested": {"b": None},
                "tags": [],
                "created_at": datetime(2025, 4, 5, tzinfo=UTC),
            },
        ]
    )


def test_env_var_initializes_force_fallback():
    """NEOSQLITE_FORCE_FALLBACK environment variable initializes get_force_fallback()."""
    for truthy in ("1", "true", "TRUE", "Yes", "on"):
        env = os.environ.copy()
        env["NEOSQLITE_FORCE_FALLBACK"] = truthy
        proc = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "from neosqlite.collection.query_helper import "
                    "get_force_fallback; assert get_force_fallback() is True"
                ),
            ],
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        assert proc.returncode == 0, proc.stderr

    for falsy in ("0", "false", "no", ""):
        env = os.environ.copy()
        if falsy:
            env["NEOSQLITE_FORCE_FALLBACK"] = falsy
        else:
            env.pop("NEOSQLITE_FORCE_FALLBACK", None)
        proc = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "from neosqlite.collection.query_helper import "
                    "get_force_fallback; assert get_force_fallback() is False"
                ),
            ],
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        assert proc.returncode == 0, proc.stderr


def test_counts_and_distinct_parity():
    """Verify count_documents, estimated_document_count, and distinct parity."""
    results_by_mode = {}
    for fallback in (False, True):
        with neosqlite.Connection(":memory:") as conn:
            coll = conn["items"]
            _seed_docs(coll)
            set_force_fallback(fallback)
            results_by_mode[fallback] = {
                "count_all": coll.count_documents({}),
                "count_filtered": coll.count_documents({"status": "active"}),
                "count_complex": coll.count_documents(
                    {
                        "$expr": {"$gt": ["$score", "$threshold"]},
                        "status": "active",
                    }
                ),
                "estimated": coll.estimated_document_count(),
                "distinct_top": sorted(coll.distinct("status")),
                "distinct_nested": sorted(coll.distinct("nested.b")),
                "distinct_tags": sorted(str(x) for x in coll.distinct("tags")),
                "distinct_filtered": sorted(
                    coll.distinct("name", {"status": "active"})
                ),
                "distinct_complex_filter": sorted(
                    coll.distinct(
                        "name",
                        {
                            "$expr": {"$gt": ["$score", "$threshold"]},
                            "status": "active",
                        },
                    )
                ),
            }

    assert results_by_mode[True] == results_by_mode[False]
    assert results_by_mode[True]["distinct_complex_filter"] == ["Alice", "Bob"]


def test_crud_bulk_and_ttl_parity():
    """Verify CRUD, upsert, bulk_write, and purge_expired parity."""
    snapshots = {}
    for fallback in (False, True):
        with neosqlite.Connection(":memory:") as conn:
            coll = conn["crud"]
            _seed_docs(coll)
            now = datetime.now(UTC) + timedelta(days=30)
            coll.create_index("expire_at", expireAfterSeconds=0)
            coll.insert_one(
                {
                    "_id": 99,
                    "name": "Expired",
                    "status": "temp",
                    "expire_at": now - timedelta(seconds=10),
                }
            )
            set_force_fallback(fallback)

            u1 = coll.update_one({}, {"$set": {"touched": True}})
            u_empty_upsert = coll.update_one(
                {"_id": 200}, {"$set": {"name": "Upserted"}}, upsert=True
            )
            um = coll.update_many({"status": "active"}, {"$inc": {"score": 5}})
            r1 = coll.replace_one(
                {"_id": 4},
                {"_id": 4, "name": "DianaReplaced", "status": "active"},
            )
            d1 = coll.delete_one({"status": "inactive"})
            bulk_res = coll.bulk_write(
                [
                    UpdateOne({"_id": 2}, {"$set": {"bulk": True}}),
                    DeleteOne({"_id": 200}),
                    ReplaceOne(
                        {"_id": 300},
                        {"_id": 300, "name": "BulkNew", "status": "other"},
                        upsert=True,
                    ),
                ]
            )
            purged = coll.purge_expired(now=now)
            dm = coll.delete_many({"status": "other"})

            snapshots[fallback] = {
                "u1": (u1.matched_count, u1.modified_count),
                "u_upsert": (
                    u_empty_upsert.matched_count,
                    u_empty_upsert.modified_count,
                    u_empty_upsert.upserted_id,
                ),
                "um": (um.matched_count, um.modified_count),
                "r1": (r1.matched_count, r1.modified_count),
                "d1": d1.deleted_count,
                "bulk": (
                    bulk_res.matched_count,
                    bulk_res.modified_count,
                    bulk_res.deleted_count,
                    bulk_res.upserted_count,
                ),
                "purged": purged,
                "dm": dm.deleted_count,
                "docs": sorted(coll.find(), key=lambda d: str(d["_id"])),
            }

    assert snapshots[True] == snapshots[False]


def test_find_one_and_modify_parity():
    """Verify find_one_and_delete/replace/update parity under kill switch."""
    snapshots = {}
    for fallback in (False, True):
        with neosqlite.Connection(":memory:") as conn:
            coll = conn["fam"]
            _seed_docs(coll)
            set_force_fallback(fallback)

            upd = coll.find_one_and_update(
                {"status": "active"},
                {"$inc": {"score": 100}},
                projection={"name": 1, "score": 1},
                sort=[("score", neosqlite.DESCENDING)],
                return_document=True,
            )
            rep = coll.find_one_and_replace(
                {"status": "active"},
                {"_id": 4, "name": "DianaV2", "status": "active", "score": 1},
                sort=[("_id", neosqlite.DESCENDING)],
                return_document=True,
            )
            dlt = coll.find_one_and_delete(
                {"status": "inactive"},
                projection={"name": 1},
            )
            upserted = coll.find_one_and_update(
                {"_id": 500},
                {"$set": {"name": "Created"}},
                upsert=True,
                return_document=True,
            )

            snapshots[fallback] = {
                "upd": upd,
                "rep": rep,
                "dlt": dlt,
                "upserted": upserted,
                "remaining": list(coll.find().sort("_id", neosqlite.ASCENDING)),
            }

    assert snapshots[True] == snapshots[False]


def test_cursor_datetime_expr_minmax_text_parity():
    """Verify datetime, $expr+siblings, min/max, sort/skip/limit, and $text parity."""
    outputs: dict[bool, dict[str, Any]] = {}
    for fallback in (False, True):
        with neosqlite.Connection(":memory:") as conn:
            coll = conn["queries"]
            _seed_docs(coll)
            coll.create_index("score")
            set_force_fallback(fallback)

            dt_docs = list(
                coll.find(
                    {"created_at": {"$gte": datetime(2025, 2, 1, tzinfo=UTC)}}
                )
                .sort("score", neosqlite.DESCENDING)
                .skip(1)
                .limit(2)
            )
            expr_sibling_docs = list(
                coll.find(
                    {
                        "$expr": {"$gt": ["$score", "$threshold"]},
                        "status": "active",
                    }
                )
                .min([("score", 10)])
                .max([("score", 25)])
                .sort("score", neosqlite.ASCENDING)
            )
            minmax_docs = list(
                coll.find({})
                .hint([("score", 1)])
                .min([("score", 15)])
                .max([("score", 30)])
                .sort("_id", neosqlite.ASCENDING)
            )

            articles = conn["articles"]
            articles.create_search_index("title")
            articles.create_search_index("body")
            articles.insert_many(
                [
                    {
                        "_id": 1,
                        "title": "Python Guide",
                        "body": "Deep dive into Python",
                        "tag": "lang",
                    },
                    {
                        "_id": 2,
                        "title": "Python Basics",
                        "body": "Intro to variables",
                        "tag": "lang",
                    },
                    {
                        "_id": 3,
                        "title": "Other",
                        "body": "Python snippet",
                        "tag": "misc",
                    },
                ]
            )
            text_proj = list(
                articles.find(
                    {"$text": {"$search": "Python"}, "tag": "lang"},
                    {"title": 1, "score": {"$meta": "textScore"}},
                ).sort([("score", {"$meta": "textScore"})])
            )
            text_clean = list(articles.find({"$text": {"$search": "Python"}}))

            no_fts = conn["no_fts"]
            no_fts.insert_one({"_id": 1, "body": "Python without index"})
            no_fts_res = list(
                no_fts.aggregate(
                    [
                        {"$match": {"body": {"$regex": "Python"}}},
                        {
                            "$project": {
                                "body": 1,
                                "score": {"$meta": "textScore"},
                            }
                        },
                    ]
                )
            )

            outputs[fallback] = {
                "dt_ids": [d["_id"] for d in dt_docs],
                "expr_ids": [d["_id"] for d in expr_sibling_docs],
                "minmax_ids": [d["_id"] for d in minmax_docs],
                "text_order": [d["_id"] for d in text_proj],
                "text_clean_has_score": any(
                    "_textScore" in d for d in text_clean
                ),
                "no_fts_scores": [d["score"] for d in no_fts_res],
            }

    assert outputs[True] == outputs[False]
    assert outputs[True]["expr_ids"] == [1]
    assert outputs[True]["minmax_ids"] == [2, 4]
    assert outputs[True]["text_order"][0] == 1
    assert outputs[True]["text_clean_has_score"] is False
    assert outputs[True]["no_fts_scores"] == [0.0]


def test_aggregation_count_facet_collstats_and_explain():
    """Verify $count, $facet, $collStats, and explain_aggregation in kill-switch mode."""
    with neosqlite.Connection(":memory:") as conn:
        coll = conn["agg"]
        _seed_docs(coll)

        count_pipe: list[dict[str, Any]] = [
            {"$match": {"status": "active"}},
            {"$count": "total"},
        ]
        facet_pipe: list[dict[str, Any]] = [
            {
                "$facet": {
                    "active": [
                        {"$match": {"status": "active"}},
                        {"$sort": {"score": -1}},
                        {"$project": {"name": 1, "score": 1}},
                    ],
                    "count": [{"$count": "n"}],
                }
            }
        ]
        stats_pipe: list[dict[str, Any]] = [
            {"$collStats": {"storageStats": {}}}
        ]

        set_force_fallback(False)
        res_normal_count = list(coll.aggregate(count_pipe))
        res_normal_facet = list(coll.aggregate(facet_pipe))

        set_force_fallback(True)
        res_fb_count = list(coll.aggregate(count_pipe))
        res_fb_facet = list(coll.aggregate(facet_pipe))
        res_fb_stats = list(coll.aggregate(stats_pipe))

        assert res_fb_count == res_normal_count == [{"total": 3}]
        assert res_fb_facet[0]["active"] == res_normal_facet[0]["active"]
        assert res_fb_facet[0]["count"] == res_normal_facet[0]["count"]
        assert res_fb_stats[0]["ns"] == "memory.agg"
        assert res_fb_stats[0]["storageStats"]["count"] == 4
        assert res_fb_stats[0]["storageStats"]["size"] > 0
        assert res_fb_stats[0]["storageStats"]["storageSize"] == 0
        assert res_fb_stats[0]["storageStats"]["indexSizes"] == {}

        match_pipe: list[dict[str, Any]] = [{"$match": {"status": "active"}}]
        for pipe in (count_pipe, facet_pipe, match_pipe):
            explain = coll.query_engine.explain_aggregation(pipe)
            assert explain["tier"] == 3


def test_gridfs_stays_sql_only_and_update_file_safe():
    """GridFS remains SQL-only and _update_gridfs_file targets only the matched file."""
    with neosqlite.Connection(":memory:") as conn:
        bucket = GridFSBucket(conn.db)
        fid1 = bucket.upload_from_stream("first.txt", b"hello world")
        fid2 = bucket.upload_from_stream("second.txt", b"goodbye world")

        set_force_fallback(True)

        found = [gf.filename for gf in bucket.find({"filename": "second.txt"})]
        assert found == ["second.txt"]
        assert conn["fs.files"].count_documents({"filename": "first.txt"}) == 1

        res = conn["fs.files"].update_one(
            {"_id": fid2}, {"$set": {"metadata": {"env": "prod"}}}
        )
        assert res.matched_count == 1
        row1 = bucket._db.execute(
            "SELECT json(metadata) FROM fs_files WHERE filename = 'first.txt'"
        ).fetchone()
        row2 = bucket._db.execute(
            "SELECT json(metadata) FROM fs_files WHERE filename = 'second.txt'"
        ).fetchone()
        assert row1[0] in (None, "{}")
        assert row2[0] is not None and '"env":"prod"' in row2[0]
        updated_files = list(bucket.find({"_id": fid2}))
        assert len(updated_files) == 1
        assert updated_files[0].metadata == {"env": "prod"}

        # Ensure downloaded bytes remain intact
        out = bucket.open_download_stream(fid1).read()
        assert out == b"hello world"

        # Non-GridFS collection ending in *_files still works cleanly via normal Python fallback
        regular_files = conn["user_files"]
        regular_files.insert_many(
            [{"_id": 1, "name": "a"}, {"_id": 2, "name": "b"}]
        )
        reg_res = regular_files.update_one(
            {"name": "b"}, {"$set": {"updated": True}}
        )
        assert reg_res.matched_count == 1
        doc1 = regular_files.find_one({"_id": 1})
        doc2 = regular_files.find_one({"_id": 2})
        assert doc1 is not None and doc2 is not None
        assert doc1.get("updated") is None
        assert doc2["updated"] is True


def test_sql_trace_audit_under_kill_switch():
    """Verify zero logical SQL execution runs for collection operations when kill switch is ON."""
    with neosqlite.Connection(":memory:") as conn:
        coll = conn["trace_test"]
        _seed_docs(coll)
        coll.create_index("score")
        coll.create_search_index("name")

        sql_log: list[str] = []
        conn.db.set_trace_callback(sql_log.append)
        try:
            set_force_fallback(True)

            coll.count_documents({"status": "active"})
            coll.estimated_document_count()
            coll.distinct("nested.b", {"status": "active"})

            list(
                coll.find({"status": "active"})
                .sort("score", neosqlite.DESCENDING)
                .skip(1)
                .limit(2)
            )
            list(
                coll.find(
                    {"created_at": {"$gte": datetime(2025, 2, 1, tzinfo=UTC)}}
                ).sort("score", neosqlite.ASCENDING)
            )
            list(
                coll.find(
                    {
                        "$expr": {"$gt": ["$score", "$threshold"]},
                        "status": "active",
                    }
                )
                .min([("score", 10)])
                .max([("score", 30)])
                .sort("score", neosqlite.ASCENDING)
            )
            list(
                coll.find(
                    {"$text": {"$search": "Alice"}},
                    {"name": 1, "score": {"$meta": "textScore"}},
                ).sort([("score", {"$meta": "textScore"})])
            )

            coll.update_one({"status": "active"}, {"$set": {"flag": 1}})
            coll.update_many({"status": "active"}, {"$inc": {"score": 1}})
            coll.replace_one(
                {"_id": 4},
                {"_id": 4, "name": "Diana2", "status": "active", "score": 15},
            )
            coll.find_one_and_update(
                {"status": "active"},
                {"$set": {"fam": True}},
                sort=[("score", neosqlite.DESCENDING)],
            )
            coll.find_one_and_replace(
                {"_id": 4},
                {"_id": 4, "name": "Diana3", "status": "inactive", "score": 5},
            )
            coll.find_one_and_delete({"_id": 4})
            coll.delete_one({"status": "inactive"})
            coll.delete_many({"status": "none"})

            list(
                coll.aggregate(
                    [{"$match": {"status": "active"}}, {"$count": "n"}]
                )
            )
            list(
                coll.aggregate(
                    [
                        {
                            "$facet": {
                                "items": [
                                    {"$sort": {"score": -1}},
                                    {"$limit": 2},
                                ],
                                "cnt": [{"$count": "c"}],
                            }
                        }
                    ]
                )
            )
            list(coll.aggregate([{"$collStats": {"storageStats": {}}}]))
        finally:
            conn.db.set_trace_callback(None)

        forbidden_patterns = [
            re.compile(r"\bSELECT\s+COUNT\b", re.IGNORECASE),
            re.compile(r"\bSELECT\s+DISTINCT\b", re.IGNORECASE),
            re.compile(r"\bORDER\s+BY\b", re.IGNORECASE),
            re.compile(r"\bLIMIT\b", re.IGNORECASE),
            re.compile(r"\bOFFSET\b", re.IGNORECASE),
            re.compile(r"\bRETURNING\b", re.IGNORECASE),
            re.compile(r"\bMATCH\b", re.IGNORECASE),
            re.compile(
                r"\bCREATE\s+(?:TEMP|TEMPORARY|VIRTUAL)\b", re.IGNORECASE
            ),
            re.compile(r"\bdbstat\b", re.IGNORECASE),
            re.compile(r"jsonb?_extract", re.IGNORECASE),
        ]

        violations = []
        for stmt in sql_log:
            # Skip sqlite_master index metadata inspection and PRAGMA statements
            upper = stmt.strip().upper()
            if "SQLITE_MASTER" in upper or upper.startswith("PRAGMA"):
                continue
            for pat in forbidden_patterns:
                if pat.search(stmt):
                    violations.append((pat.pattern, stmt))

        assert (
            violations == []
        ), f"Unexpected logical SQL under kill switch: {violations}"
