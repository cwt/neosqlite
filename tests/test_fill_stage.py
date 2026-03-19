from operator import itemgetter

import pytest

from neosqlite import Connection
from neosqlite.collection.query_helper.utils import set_force_fallback


@pytest.fixture
def collection(tmp_path):
    db_path = str(tmp_path / "test_fill.db")

    with Connection(db_path) as conn:
        coll = conn.collection
        coll.insert_many(
            [
                {"_id": 1, "time": 1, "val": 10, "cat": "A", "other": 5},
                {"_id": 2, "time": 2, "val": None, "cat": "A", "other": None},
                {"_id": 3, "time": 3, "val": None, "cat": "A", "other": 15},
                {"_id": 4, "time": 4, "val": 40, "cat": "A", "other": None},
                {"_id": 5, "time": 1, "val": 100, "cat": "B", "other": 50},
                {"_id": 6, "time": 2, "val": None, "cat": "B", "other": None},
            ]
        )
        yield coll


def test_fill_locf(collection):
    """Test $fill with locf method across tiers."""
    pipeline = [
        {
            "$fill": {
                "partitionBy": "$cat",
                "sortBy": {"time": 1},
                "output": {"val": {"method": "locf"}},
            }
        },
        {"$sort": {"_id": 1}},
    ]

    # Tier 1 (SQL)
    set_force_fallback(False)
    results = list(collection.aggregate(pipeline))
    assert results[1]["val"] == 10
    assert results[2]["val"] == 10
    assert results[5]["val"] == 100

    # Tier 3 (Python)
    set_force_fallback(True)
    results_py = list(collection.aggregate(pipeline))
    set_force_fallback(False)
    assert results_py == results


def test_fill_value(collection):
    """Test $fill with constant value."""
    pipeline = [
        {"$fill": {"output": {"val": {"value": -1}}}},
        {"$sort": {"_id": 1}},
    ]

    results = list(collection.aggregate(pipeline))
    assert results[1]["val"] == -1
    assert results[2]["val"] == -1
    assert results[5]["val"] == -1
    assert results[0]["val"] == 10


def test_fill_linear(collection):
    """Test $fill with linear interpolation (Tier 3 fallback)."""
    pipeline = [
        {
            "$fill": {
                "partitionBy": "$cat",
                "sortBy": {"time": 1},
                "output": {"val": {"method": "linear"}},
            }
        },
        {"$sort": {"_id": 1}},
    ]

    # Linear is not supported in Tier 1, so it will fall back to Tier 3
    results = list(collection.aggregate(pipeline))
    results_a = [r for r in results if r["cat"] == "A"]
    results_a.sort(key=itemgetter("time"))
    # A: 1->10, 4->40. Step = 10.
    assert results_a[1]["val"] == 20
    assert results_a[2]["val"] == 30


def test_fill_mixed_methods(collection):
    """Test $fill with multiple output fields and methods."""
    pipeline = [
        {
            "$fill": {
                "partitionBy": "$cat",
                "sortBy": {"time": 1},
                "output": {"val": {"method": "locf"}, "other": {"value": 0}},
            }
        },
        {"$sort": {"_id": 1}},
    ]

    results = list(collection.aggregate(pipeline))
    # _id 2: val should be 10 (locf), other should be 0 (value)
    doc2 = next(r for r in results if r["_id"] == 2)
    assert doc2["val"] == 10
    assert doc2["other"] == 0

    # _id 4: val is 40 (original), other should be 0 (value)
    doc4 = next(r for r in results if r["_id"] == 4)
    assert doc4["val"] == 40
    assert doc4["other"] == 0


def test_fill_tier2(collection):
    """Test $fill in a Tier-2 pipeline."""
    # Force Tier 2 using $lookup
    collection.database.other_coll.insert_one({"_id": 1, "note": "X"})

    pipeline = [
        {"$match": {"cat": "A"}},
        {
            "$lookup": {
                "from": "other_coll",
                "localField": "time",
                "foreignField": "_id",
                "as": "ext",
            }
        },
        {
            "$fill": {
                "sortBy": {"time": 1},
                "output": {"val": {"method": "locf"}},
            }
        },
        {"$sort": {"_id": 1}},
    ]

    results = list(collection.aggregate(pipeline))
    assert results[1]["val"] == 10
    assert results[2]["val"] == 10
    assert "ext" in results[0]
