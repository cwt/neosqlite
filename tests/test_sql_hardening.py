"""Regression tests for SQL identifier quoting, JSON path escaping, and SQL construction hardening (#192)."""

import pytest

from neosqlite import ASCENDING, DESCENDING, Connection
from neosqlite.collection.json_path_utils import parse_json_path
from neosqlite.collection.query_helper.utils import (
    set_force_fallback,
)
from neosqlite.gridfs import GridFSBucket
from neosqlite.sql_utils import (
    quote_identifier,
    quote_table_name,
    validate_identifier,
)


@pytest.fixture(autouse=True)
def _reset_fallback():
    yield
    set_force_fallback(False)


def test_sql_utils_validate_and_quote_reserved_words():
    """validate_identifier returns raw string; quote_* double-quotes it."""
    for reserved in (
        "order",
        "group",
        "select",
        "where",
        "table",
        "index",
        "default",
        "check",
        "transaction",
    ):
        assert validate_identifier(reserved) == reserved
        assert quote_identifier(reserved) == f'"{reserved}"'
        assert quote_table_name(reserved) == f'"{reserved}"'

    for invalid in ("", "1bad", "has space", 'has"quote', "a; DROP TABLE x"):
        with pytest.raises(ValueError):
            validate_identifier(invalid)
        with pytest.raises(ValueError):
            quote_identifier(invalid)


def test_reserved_word_collection_end_to_end():
    """Reserved SQL keywords work as collection names across CRUD, indexes, aggregation, and GridFS."""
    with Connection(":memory:") as conn:
        for name in ("order", "group", "select"):
            coll = conn[name]
            coll.insert_many(
                [
                    {"a": 1, "b": 10, "body": "hello world"},
                    {"a": 2, "b": 20, "body": "sqlite test"},
                ]
            )

            assert (
                len(list(coll.find({"a": {"$gte": 1}}).sort("a", ASCENDING)))
                == 2
            )
            coll.update_one({"a": 1}, {"$set": {"b": 15}})
            assert coll.find_one({"a": 1})["b"] == 15

            # Indexes & FTS5 search index
            idx_a = coll.create_index("a")
            assert idx_a == f"idx_{name}_a"
            coll.create_index([("a", ASCENDING), ("b", DESCENDING)])
            fts_name = coll.create_index("body", fts=True)
            assert fts_name == f"{name}_body_fts"
            assert "body" in coll.list_search_indexes()
            assert any(
                i.startswith(f"idx_{name}_") for i in coll.list_indexes()
            )
            assert (
                list(coll.find({"$text": {"$search": "sqlite"}}))[0]["a"] == 2
            )
            coll.drop_index("a")
            coll.drop_search_index("body")

            # $expr query using TempTableExprEvaluator (tier 2 complexity)
            expr_res = list(
                coll.find(
                    {
                        "$expr": {
                            "$gt": [
                                {"$add": [{"$multiply": ["$a", 10]}, "$b"]},
                                30,
                            ]
                        }
                    }
                )
            )
            assert len(expr_res) == 1
            assert expr_res[0]["a"] == 2

            # collstats / table_info / validate commands
            stats = conn.command("collstats", collection=name)
            assert stats["ok"] == 1
            assert stats["count"] == 2
            assert conn.command("table_info", table=name)["ok"] == 1
            assert conn.command("validate", name)["ok"] == 1

            # rename & drop
            coll.rename(f"{name}_renamed")
            assert conn[f"{name}_renamed"].count_documents({}) == 2
            conn[f"{name}_renamed"].drop()

        # GridFSBucket with reserved word bucket name
        bucket = GridFSBucket(conn.db, bucket_name="order")
        fid = bucket.upload_from_stream("doc.txt", b"payload-data")
        assert bucket.open_download_stream(fid).read() == b"payload-data"
        assert bucket.list() == ["doc.txt"]
        assert len(list(bucket.find({"filename": "doc.txt"}))) == 1
        bucket.delete(fid)
        assert bucket.list() == []
        bucket.drop()


def test_parse_json_path_and_special_keys_in_queries():
    """Field names with ', \", whitespace, \\, or unmatched brackets query safely."""
    assert parse_json_path("it's") == "$.\"it''s\""
    assert parse_json_path("a[b") == '$."a[b"'
    assert parse_json_path("a]b") == '$."a]b"'
    assert parse_json_path("total count") == '$."total count"'
    assert parse_json_path('say "hi"') == '$."say \\"hi\\""'
    assert parse_json_path("tags[0]") == "$.tags[0]"
    assert parse_json_path("it's[0]") == "$.\"it''s\"[0]"

    with Connection(":memory:") as conn:
        coll = conn.p
        doc = {
            "_id": 1,
            "it's": "ok",
            "a[b": 42,
            "a]b": 99,
            "total count": 7,
            'say "hi"': "yes",
            "items": [{"k's": "matched"}],
        }
        coll.insert_one(doc)
        coll.insert_one({"_id": 2, "it's": "other"})

        for use_fallback in (False, True):
            set_force_fallback(use_fallback)
            assert [d["_id"] for d in coll.find({"it's": "ok"})] == [1]
            assert [d["_id"] for d in coll.find({"a[b": 42})] == [1]
            assert [d["_id"] for d in coll.find({"a]b": 99})] == [1]
            assert [d["_id"] for d in coll.find({"total count": 7})] == [1]
            assert [d["_id"] for d in coll.find({'say "hi"': "yes"})] == [1]
            assert [d["_id"] for d in coll.find({"items.k's": "matched"})] == [
                1
            ]


def test_group_project_bucket_special_field_names():
    """Spaces, keywords, quotes, and punctuation in aliases and $project/$group keys work in SQL & Python."""
    with Connection(":memory:") as conn:
        coll = conn.p
        coll.insert_many(
            [
                {"_id": 1, "cat": "x", "v": 10, "it's": "a"},
                {"_id": 2, "cat": "x", "v": 20, "it's": "b"},
                {"_id": 3, "cat": "y", "v": 5, "it's": "c"},
            ]
        )

        # $project with single quotes and spaces (Tier 1 & Tier 2/3)
        proj_pipeline = [
            {"$project": {"_id": 1, "it's": "$it's", "my order": "$v"}},
            {"$sort": {"_id": 1}},
        ]
        set_force_fallback(False)
        res_sql = list(coll.aggregate(proj_pipeline))
        set_force_fallback(True)
        res_py = list(coll.aggregate(proj_pipeline))
        assert res_sql == res_py
        assert res_sql[0]["it's"] == "a"

        # Tier-2 forced $project (after $limit so SQLTierAggregator skips or direct temp)
        set_force_fallback(False)
        res_t2_proj = list(
            coll.aggregate(
                [
                    {
                        "$unwind": {
                            "path": "$dummy",
                            "preserveNullAndEmptyArrays": True,
                        }
                    },
                    {"$project": {"_id": 1, "it's": "$it's", "order": 1}},
                    {"$sort": {"_id": 1}},
                ]
            )
        )
        assert res_t2_proj[0]["it's"] == "a"

        # $group with space, SQL keyword, and single quote (Tier 1 + Tier 2 ranked + Tier 3)
        group_pipeline = [
            {
                "$group": {
                    "_id": "$cat",
                    "total count": {"$sum": 1},
                    "order": {"$sum": "$v"},
                    "it's": {"$max": "$v"},
                }
            },
            {"$sort": {"total count": -1, "_id": 1}},
        ]
        set_force_fallback(False)
        g_sql = list(coll.aggregate(group_pipeline))
        set_force_fallback(True)
        g_py = list(coll.aggregate(group_pipeline))
        assert g_sql == g_py
        assert g_sql[0]["total count"] == 2
        assert g_sql[0]["order"] == 30

        # Tier-2 $group with $first/$last (ranked CTE) and $push template with quote key
        set_force_fallback(False)
        ranked_group = list(
            coll.aggregate(
                [
                    {"$sort": {"_id": 1}},
                    {
                        "$unwind": {
                            "path": "$dummy",
                            "preserveNullAndEmptyArrays": True,
                        }
                    },
                    {
                        "$group": {
                            "_id": "$cat",
                            "total count": {"$sum": 1},
                            "first's": {"$first": "$v"},
                            "recs": {"$push": {"k's": "$v"}},
                        }
                    },
                    {"$sort": {"_id": 1}},
                ]
            )
        )
        assert ranked_group[0]["total count"] == 2
        assert ranked_group[0]["first's"] == 10
        assert ranked_group[0]["recs"] == [{"k's": 10}, {"k's": 20}]

        # $bucket and $bucketAuto with special output field names
        b_res = list(
            coll.aggregate(
                [
                    {
                        "$bucket": {
                            "groupBy": "$v",
                            "boundaries": [0, 15, 30],
                            "default": "Other",
                            "output": {"total count": {"$sum": 1}},
                        }
                    }
                ]
            )
        )
        assert sum(r["total count"] for r in b_res) == 3

        ba_res = list(
            coll.aggregate(
                [
                    {
                        "$bucketAuto": {
                            "groupBy": "$v",
                            "buckets": 2,
                            "output": {"total count": {"$sum": 1}},
                        }
                    }
                ]
            )
        )
        assert sum(r["total count"] for r in ba_res) == 3


def test_cursor_comment_prevents_sql_comment_breakout():
    """Sequences like '*--/' and '/**/' cannot prematurely terminate SQL block comments."""
    with Connection(":memory:") as conn:
        coll = conn.p
        coll.insert_many([{"_id": 1, "a": 1}, {"_id": 2, "a": 2}])
        tricky = "note *--/ text /* */ -- \x00 SELECT 1"

        assert len(list(coll.find({}).comment(tricky))) == 2
        assert len(list(coll.find({"a": {"$gte": 1}}).comment(tricky))) == 2
        assert (
            len(
                list(
                    coll.find({})
                    .min([("_id", 1)])
                    .max([("_id", 10)])
                    .comment(tricky)
                )
            )
            == 2
        )
        set_force_fallback(True)
        assert len(list(coll.find({"a": 1}).comment(tricky))) == 1


def test_stage_tokenizer_and_pragma_validation():
    """Stage options, FTS5 tokenizers, and PRAGMA commands reject unsafe inputs."""
    with Connection(":memory:") as conn:
        coll = conn.p
        coll.insert_many([{"_id": 1, "v": 1, "body": "alpha beta"}])

        # $sample and $bucketAuto validation
        for bad in ("1; DROP TABLE p", True, -1, 1.5):
            with pytest.raises((ValueError, Exception)):
                list(coll.aggregate([{"$sample": {"size": bad}}]))
        for bad_b in ("2); DROP TABLE p", True, 1.5):
            with pytest.raises((ValueError, Exception)):
                list(
                    coll.aggregate(
                        [{"$bucketAuto": {"groupBy": "$v", "buckets": bad_b}}]
                    )
                )
        for non_pos in (0, -2):
            assert (
                list(
                    coll.aggregate(
                        [
                            {
                                "$bucketAuto": {
                                    "groupBy": "$v",
                                    "buckets": non_pos,
                                }
                            }
                        ]
                    )
                )
                == []
            )
            with pytest.raises(ValueError):
                coll.query_engine.sql_tier_aggregator._build_bucket_auto_sql(
                    {"groupBy": "$v", "buckets": non_pos},
                    "p",
                    type("Ctx", (), {"has_root": False})(),
                )

        assert len(list(coll.aggregate([{"$sample": {"size": 1}}]))) == 1

        # FTS5 tokenizer validation
        for bad_tok in (
            "porter); DROP TABLE p; --",
            "unicode61; SELECT 1",
            "porter' OR '1'='1",
            "",
        ):
            with pytest.raises(ValueError):
                coll.create_index("body", fts=True, tokenizer=bad_tok)

        coll.create_index("body", fts=True, tokenizer="porter")
        assert "body" in coll.list_search_indexes()
        coll.drop_search_index("body")
        coll.create_index(
            "body", fts=True, tokenizer="unicode61 remove_diacritics 0"
        )
        assert "body" in coll.list_search_indexes()

        # PRAGMA parameter & command allowlist hardening
        assert conn.command("cache_size", pages="100; DROP TABLE p")["ok"] == 0
        assert conn.command("cache_size", pages=True)["ok"] == 0
        assert conn.command("cache_size", pages=-2000)["ok"] == 1

        assert (
            conn.command("busy_timeout", milliseconds="10; DROP TABLE p")["ok"]
            == 0
        )
        assert conn.command("busy_timeout", milliseconds=-5)["ok"] == 0
        assert conn.command("busy_timeout", milliseconds=False)["ok"] == 0
        assert conn.command("busy_timeout", milliseconds=250)["ok"] == 1

        assert conn.command("writable_schema")["ok"] == 0
        assert conn.command("unknown_command_xyz")["ok"] == 0
        assert conn.command("page_size")["ok"] == 1


def test_like_underscore_wildcard_does_not_leak_indexes_or_fts():
    """Collection 'a_b' must not match indexes or FTS tables belonging to 'axb'."""
    with Connection(":memory:") as conn:
        coll_ab = conn["a_b"]
        coll_axb = conn["axb"]
        coll_ab.insert_one({"name": "from_ab", "text": "only in ab"})
        coll_axb.insert_one({"secret": "val", "body": "only in axb"})

        coll_axb.create_index("secret")
        coll_axb.create_index("body", fts=True)

        assert coll_ab.list_indexes() == []
        assert coll_ab.list_search_indexes() == []
        assert coll_ab.index_information() == {}
        assert coll_ab.query_engine.helpers._get_indexed_fields() == []

        # Python fallback text search on a_b must not treat axb_body_fts as a_b's FTS index
        set_force_fallback(True)
        res = list(
            coll_ab.find({"$text": {"$search": "only"}, "name": "from_ab"})
        )
        assert len(res) == 1
        assert res[0]["name"] == "from_ab"


def test_split_with_quotes_backslashes_and_control_chars():
    """$split handles \", \\, \\n, and \\t identically between SQL and Python tiers."""
    with Connection(":memory:") as conn:
        coll = conn.p
        coll.insert_many(
            [
                {"_id": 1, "s": 'say "hi" \\ there\nline\t2'},
                {"_id": 2, "s": ""},
                {"_id": 3, "s": None},
            ]
        )

        pipeline = [
            {"$project": {"_id": 1, "parts": {"$split": ["$s", " "]}}},
            {"$sort": {"_id": 1}},
        ]
        set_force_fallback(False)
        sql_res = list(coll.aggregate(pipeline))
        set_force_fallback(True)
        py_res = list(coll.aggregate(pipeline))
        assert sql_res == py_res
        assert sql_res[0]["parts"] == ["say", '"hi"', "\\", "there\nline\t2"]
