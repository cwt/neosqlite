import pytest
from neosqlite import Connection
from neosqlite.collection.query_helper.utils import set_force_fallback


@pytest.fixture
def collection(tmp_path):
    db_path = str(tmp_path / "test_graph.db")

    with Connection(db_path) as conn:
        coll = conn.collection
        coll.insert_many(
            [
                {
                    "_id": 1,
                    "name": "Manager",
                    "reportsTo": None,
                    "dept": "Exec",
                },
                {"_id": 2, "name": "Team Lead", "reportsTo": 1, "dept": "Eng"},
                {"_id": 3, "name": "Developer", "reportsTo": 2, "dept": "Eng"},
                {"_id": 4, "name": "Intern", "reportsTo": 3, "dept": "Eng"},
                {
                    "_id": 5,
                    "name": "HR Manager",
                    "reportsTo": None,
                    "dept": "HR",
                },
                {
                    "_id": 6,
                    "name": "HR Specialist",
                    "reportsTo": 5,
                    "dept": "HR",
                },
            ]
        )
        yield coll, conn


def test_graph_lookup_basic(collection):
    """Test basic $graphLookup."""
    collection, conn = collection
    pipeline = [
        {"$match": {"_id": 4}},
        {
            "$graphLookup": {
                "from": "collection",
                "startWith": "$reportsTo",
                "connectFromField": "reportsTo",
                "connectToField": "_id",
                "as": "hierarchy",
            }
        },
    ]

    # Tier 1 (SQL)
    set_force_fallback(False)
    results = list(collection.aggregate(pipeline))
    assert len(results) == 1
    hierarchy = results[0]["hierarchy"]
    assert len(hierarchy) == 3
    ids = {doc["_id"] for doc in hierarchy}
    assert ids == {1, 2, 3}

    # Tier 3 (Python)
    set_force_fallback(True)
    results_py = list(collection.aggregate(pipeline))
    set_force_fallback(False)

    # Sort for comparison as order is not guaranteed
    results[0]["hierarchy"].sort(key=lambda x: x["_id"])
    results_py[0]["hierarchy"].sort(key=lambda x: x["_id"])
    assert results == results_py


def test_graph_lookup_max_depth(collection):
    """Test $graphLookup with maxDepth option."""
    collection, conn = collection
    pipeline = [
        {"$match": {"_id": 4}},
        {
            "$graphLookup": {
                "from": "collection",
                "startWith": "$reportsTo",
                "connectFromField": "reportsTo",
                "connectToField": "_id",
                "as": "hierarchy",
                "maxDepth": 1,
            }
        },
    ]

    results = list(collection.aggregate(pipeline))
    hierarchy = results[0]["hierarchy"]
    assert len(hierarchy) == 2  # 3 and 2 (depth 0 and 1)
    ids = {doc["_id"] for doc in hierarchy}
    assert ids == {3, 2}


def test_graph_lookup_depth_field(collection):
    """Test $graphLookup with depthField option."""
    collection, conn = collection
    pipeline = [
        {"$match": {"_id": 4}},
        {
            "$graphLookup": {
                "from": "collection",
                "startWith": "$reportsTo",
                "connectFromField": "reportsTo",
                "connectToField": "_id",
                "as": "hierarchy",
                "depthField": "level",
            }
        },
    ]

    results = list(collection.aggregate(pipeline))
    hierarchy = results[0]["hierarchy"]
    for doc in hierarchy:
        if doc["_id"] == 3:
            assert doc["level"] == 0
        if doc["_id"] == 2:
            assert doc["level"] == 1
        if doc["_id"] == 1:
            assert doc["level"] == 2


def test_graph_lookup_restrict_search(collection):
    """Test $graphLookup with restrictSearchWithMatch option."""
    collection, conn = collection
    pipeline = [
        {"$match": {"_id": 4}},
        {
            "$graphLookup": {
                "from": "collection",
                "startWith": "$reportsTo",
                "connectFromField": "reportsTo",
                "connectToField": "_id",
                "as": "hierarchy",
                "restrictSearchWithMatch": {"dept": "Eng"},
            }
        },
    ]

    results = list(collection.aggregate(pipeline))
    hierarchy = results[0]["hierarchy"]
    # Should only find 3 and 2, as 1 is in "Exec" dept
    assert len(hierarchy) == 2
    ids = {doc["_id"] for doc in hierarchy}
    assert ids == {3, 2}


def test_graph_lookup_tier2(collection):
    """Test $graphLookup explain plan for Tier 2."""
    collection, conn = collection
    # Force Tier 2 by using $lookup before $graphLookup
    # Ensure 'other' collection exists and has data in the SAME database
    db = collection.database
    other = db.other
    other.insert_one({"_id": 4, "note": "Target"})

    pipeline = [
        {"$match": {"_id": 4}},
        {
            "$lookup": {
                "from": "other",
                "localField": "_id",
                "foreignField": "_id",
                "as": "ext",
            }
        },
        {
            "$graphLookup": {
                "from": "collection",
                "startWith": "$reportsTo",
                "connectFromField": "reportsTo",
                "connectToField": "_id",
                "as": "hierarchy",
            }
        },
    ]

    explanation = conn.command(
        "aggregate", "collection", pipeline=pipeline, explain=True
    )
    assert explanation["tier"] == 2

    results = list(collection.aggregate(pipeline))
    print("\nDEBUG results:", results)
    assert len(results) == 1
    assert len(results[0]["hierarchy"]) == 3
    assert results[0]["ext"][0]["note"] == "Target"
