"""Tests covering type converters SQL generator and multi-field text index creation."""

from __future__ import annotations

import neosqlite


def test_multifield_text_index_creation():
    """Verify that collection.create_index accepts list of ('field', 'text') tuples."""
    conn = neosqlite.Connection(":memory:")
    coll1 = conn.test_docs1

    coll1.insert_many(
        [
            {
                "_id": 1,
                "title": "SQLite Performance Guide",
                "body": "Techniques for fast query execution",
            },
            {
                "_id": 2,
                "title": "Python Programming",
                "body": "Advanced async patterns in Python",
            },
        ]
    )

    idx_name = coll1.create_index([("title", "text"), ("body", "text")])
    assert idx_name is not None
    assert "test_docs1_title_fts" in idx_name
    assert "test_docs1_body_fts" in idx_name

    # Single text field in list in another collection
    coll2 = conn.test_docs2
    coll2.insert_one({"_id": 1, "title": "Single Field Guide"})
    idx_single = coll2.create_index([("title", "text")])
    assert idx_single == "test_docs2_title_fts"

    # Search using text index
    results = list(coll1.find({"$text": {"$search": "SQLite"}}))
    assert len(results) == 1
    assert results[0]["_id"] == 1


def test_type_converters_in_expr():
    """Verify $toInt, $toDouble, $toString, $toBool, and $isNumber in $expr queries."""
    conn = neosqlite.Connection(":memory:")
    coll = conn.numbers

    coll.insert_many(
        [
            {
                "_id": 1,
                "val_str": "42",
                "val_float": 3.14,
                "flag": 1,
                "text": "hello",
            },
            {
                "_id": 2,
                "val_str": "not_a_num",
                "val_float": 0.0,
                "flag": 0,
                "text": "world",
            },
        ]
    )

    # $toInt conversion in $expr
    docs = list(coll.find({"$expr": {"$eq": [{"$toInt": "$val_str"}, 42]}}))
    assert len(docs) == 1
    assert docs[0]["_id"] == 1

    # $toDouble in $expr
    docs = list(
        coll.find({"$expr": {"$gt": [{"$toDouble": "$val_float"}, 3.0]}})
    )
    assert len(docs) == 1
    assert docs[0]["_id"] == 1

    # $toString in $expr
    docs = list(
        coll.find({"$expr": {"$eq": [{"$toString": "$val_float"}, "3.14"]}})
    )
    assert len(docs) == 1
    assert docs[0]["_id"] == 1

    # $toBool in $expr
    docs = list(coll.find({"$expr": {"$eq": [{"$toBool": "$flag"}, True]}}))
    assert len(docs) == 1
    assert docs[0]["_id"] == 1

    # $isNumber in $expr
    docs = list(
        coll.find({"$expr": {"$eq": [{"$isNumber": "$val_float"}, True]}})
    )
    assert len(docs) == 2
