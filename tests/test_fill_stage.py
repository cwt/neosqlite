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
                {"_id": 1, "time": 1, "val": 10, "cat": "A"},
                {"_id": 2, "time": 2, "val": None, "cat": "A"},
                {"_id": 3, "time": 3, "val": None, "cat": "A"},
                {"_id": 4, "time": 4, "val": 40, "cat": "A"},
                {"_id": 5, "time": 1, "val": 100, "cat": "B"},
                {"_id": 6, "time": 2, "val": None, "cat": "B"},
            ]
        )
        yield coll


def test_fill_locf(collection):
    """Test $fill with locf method."""
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
    # A: 1->10, 2->?, 3->?, 4->40. Step = (40-10)/(4-1) = 10.
    # 2 -> 10 + 10 = 20
    # 3 -> 20 + 10 = 30
    # Filter for cat A
    results_a = [r for r in results if r["cat"] == "A"]
    results_a.sort(key=lambda x: x["time"])
    assert results_a[1]["val"] == 20
    assert results_a[2]["val"] == 30


def test_fill_tier2(collection):
    """Test $fill in a Tier-2 pipeline."""
    # Force Tier 2 using $lookup
    collection.database.other.insert_one({"_id": 1, "note": "X"})

    pipeline = [
        {"$match": {"cat": "A"}},
        {
            "$lookup": {
                "from": "other",
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

    explanation = collection.aggregate(pipeline).explain()
    assert explanation["tier"] == 2

    results = list(collection.aggregate(pipeline))
    assert results[1]["val"] == 10
    assert results[2]["val"] == 10
