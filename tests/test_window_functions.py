import pytest
from neosqlite import Connection
from neosqlite.collection.query_helper.utils import set_force_fallback


@pytest.fixture
def collection(tmp_path):
    db_path = str(tmp_path / "test_window.db")

    with Connection(db_path) as conn:
        coll = conn.collection
        coll.insert_many(
            [
                {
                    "_id": 1,
                    "name": "A",
                    "score": 100,
                    "dept": "Sales",
                    "date": 1,
                },
                {
                    "_id": 2,
                    "name": "B",
                    "score": 90,
                    "dept": "Sales",
                    "date": 2,
                },
                {
                    "_id": 3,
                    "name": "C",
                    "score": 90,
                    "dept": "Sales",
                    "date": 3,
                },
                {"_id": 4, "name": "D", "score": 80, "dept": "Eng", "date": 4},
                {"_id": 5, "name": "E", "score": 110, "dept": "Eng", "date": 5},
                {"_id": 6, "name": "F", "score": 110, "dept": "Eng", "date": 6},
            ]
        )
        yield coll, conn


def test_window_basic_rank(collection):
    """Test $rank, $denseRank, and documentNumber across tiers."""
    collection, conn = collection
    pipeline = [
        {
            "$setWindowFields": {
                "partitionBy": "$dept",
                "sortBy": {"score": -1},
                "output": {
                    "rank": {"$rank": {}},
                    "denseRank": {"$denseRank": {}},
                    "docNum": {"$documentNumber": {}},
                },
            }
        },
        {"$sort": {"_id": 1}},
    ]

    # Tier 1 (SQL)
    set_force_fallback(False)
    results = list(collection.aggregate(pipeline))
    assert len(results) == 6

    sales_a = next(r for r in results if r["_id"] == 1)
    assert sales_a["rank"] == 1
    assert sales_a["denseRank"] == 1

    sales_b = next(r for r in results if r["_id"] == 2)
    assert sales_b["rank"] == 2
    assert sales_b["denseRank"] == 2

    sales_c = next(r for r in results if r["_id"] == 3)
    assert sales_c["rank"] == 2
    assert sales_c["denseRank"] == 2

    eng_d = next(r for r in results if r["_id"] == 4)
    assert eng_d["rank"] == 3
    assert eng_d["denseRank"] == 2

    # Tier 3 (Python Fallback)
    set_force_fallback(True)
    results_py = list(collection.aggregate(pipeline))
    set_force_fallback(False)
    assert results_py == results


def test_window_shift(collection):
    """Test $shift window function."""
    collection, conn = collection
    pipeline = [
        {
            "$setWindowFields": {
                "sortBy": {"_id": 1},
                "output": {
                    "prev": {
                        "$shift": {"output": "$score", "by": -1, "default": -1}
                    },
                    "next": {
                        "$shift": {"output": "$score", "by": 1, "default": -1}
                    },
                },
            }
        },
        {"$sort": {"_id": 1}},
    ]

    results = list(collection.aggregate(pipeline))
    assert results[0]["prev"] == -1
    assert results[0]["next"] == 90
    assert results[1]["prev"] == 100
    assert results[1]["next"] == 90

    # Tier 3
    set_force_fallback(True)
    results_py = list(collection.aggregate(pipeline))
    set_force_fallback(False)
    assert results_py == results


def test_window_accumulators(collection):
    """Test window accumulators like $avg, $sum, $min, $max, $stdDevPop, $stdDevSamp."""
    collection, conn = collection
    pipeline = [
        {
            "$setWindowFields": {
                "partitionBy": "$dept",
                "sortBy": {"_id": 1},
                "output": {
                    "runningSum": {
                        "$sum": "$score",
                        "window": {"documents": ["unbounded", "current"]},
                    },
                    "movingAvg": {
                        "$avg": "$score",
                        "window": {"documents": [-1, 1]},
                    },
                },
            }
        },
        {"$sort": {"_id": 1}},
    ]

    results = list(collection.aggregate(pipeline))
    # Sales: id 1(100), 2(90), 3(90)
    # runningSum Sales: 1->100, 2->190, 3->280
    assert next(r for r in results if r["_id"] == 1)["runningSum"] == 100
    assert next(r for r in results if r["_id"] == 2)["runningSum"] == 190
    assert next(r for r in results if r["_id"] == 3)["runningSum"] == 280

    # movingAvg id 2: avg(100, 90, 90) = 280/3 = 93.333
    avg2 = next(r for r in results if r["_id"] == 2)["movingAvg"]
    assert abs(avg2 - 93.333) < 0.001

    # Tier 3
    set_force_fallback(True)
    results_py = list(collection.aggregate(pipeline))
    set_force_fallback(False)
    for r1, r2 in zip(results, results_py):
        assert r1["_id"] == r2["_id"]
        assert r1["runningSum"] == r2["runningSum"]
        assert abs(r1["movingAvg"] - r2["movingAvg"]) < 0.001


def test_window_complex_partition(collection):
    """Test window functions with complex partition expressions."""
    collection, conn = collection
    pipeline = [
        {
            "$setWindowFields": {
                "partitionBy": {"$toLower": "$dept"},
                "sortBy": {"_id": 1},
                "output": {"count": {"$sum": 1}},
            }
        }
    ]
    results = list(collection.aggregate(pipeline))
    assert len(results) == 6
    for r in results:
        assert r["count"] == 3


def test_window_explain(collection):
    """Test the explain command on aggregation."""
    collection, conn = collection
    pipeline = [{"$setWindowFields": {"output": {"rank": {"$rank": {}}}}}]
    explanation = conn.command(
        "aggregate", "collection", pipeline=pipeline, explain=True
    )

    assert "tier" in explanation
    assert "type" in explanation
    # For simple window fields, it should be Tier 1
    assert explanation["tier"] == 1
    assert "RANK() OVER" in explanation["sql"]


def test_window_tier2_fallback(collection):
    """Test that $setWindowFields works when combined with Tier-2 only stages."""
    collection, conn = collection
    # $lookup usually forces Tier 2 or 3.
    # We'll create a second collection for lookup.
    collection.database.lookup_coll.insert_one(
        {"dept": "Sales", "manager": "Alice"}
    )

    pipeline = [
        {"$match": {"dept": "Sales"}},
        {
            "$lookup": {
                "from": "lookup_coll",
                "localField": "dept",
                "foreignField": "dept",
                "as": "mgr_info",
            }
        },
        {
            "$setWindowFields": {
                "sortBy": {"score": -1},
                "output": {"rank": {"$rank": {}}},
            }
        },
    ]

    cursor = collection.aggregate(pipeline)
    explanation = conn.command(
        "aggregate", "collection", pipeline=pipeline, explain=True
    )

    # With $lookup now supported in Tier 1 SQL, this should be Tier 1
    assert explanation["tier"] == 1

    results = list(cursor)
    assert len(results) == 3
    assert all("mgr_info" in r for r in results)
    assert all("rank" in r for r in results)


def test_window_unsupported_operator_fallback(collection):
    """Test fallback when an operator is not supported by SQL but is by Python."""
    collection, conn = collection
    # We don't have many of these yet since we implemented most in SQL,
    # but we can check if it raises NotImplementedError in SQL builder and falls back.

    # Forcing a complex expression in $shift that might fail SQL translation
    pipeline = [
        {
            "$setWindowFields": {
                "output": {
                    "complex": {
                        "$shift": {
                            "output": {"$literal": {"a": 1}},  # Complex literal
                            "by": 1,
                        }
                    }
                }
            }
        }
    ]

    # This should still work via Tier 3
    results = list(collection.aggregate(pipeline))
    assert len(results) == 6
    assert results[0]["complex"] == {"a": 1}


def test_window_first_last(collection):
    """Test $first and $last operators in window fields."""
    collection, conn = collection
    pipeline = [
        {
            "$setWindowFields": {
                "partitionBy": "$dept",
                "sortBy": {"date": 1},
                "output": {
                    "firstVal": {"$first": "$score"},
                    "lastVal": {"$last": "$score"},
                },
            }
        },
        {"$sort": {"_id": 1}},
    ]

    # These should use Tier 1 SQL (FIRST_VALUE / LAST_VALUE)
    cursor = collection.aggregate(pipeline)
    explanation = conn.command(
        "aggregate", "collection", pipeline=pipeline, explain=True
    )
    assert explanation["tier"] == 1

    results = list(cursor)
    # Sales: first=100 (date 1), last=90 (date 3)
    assert results[0]["firstVal"] == 100
    assert results[0]["lastVal"] == 90
    # Eng: first=80 (date 4), last=110 (date 6)
    assert results[3]["firstVal"] == 80
    assert results[3]["lastVal"] == 110


def test_window_firstN_lastN(collection):
    """Test $firstN and $lastN operators in window fields."""
    collection, conn = collection
    pipeline = [
        {
            "$setWindowFields": {
                "partitionBy": "$dept",
                "sortBy": {"date": 1},
                "output": {
                    "first2": {"$firstN": {"input": "$score", "n": 2}},
                    "last2": {"$lastN": {"input": "$score", "n": 2}},
                },
            }
        },
        {"$sort": {"_id": 1}},
    ]

    # N operators currently use Tier 3 fallback
    results = list(collection.aggregate(pipeline))

    # Sales: scores [100, 90, 90]
    assert results[0]["first2"] == [100, 90]
    assert results[0]["last2"] == [90, 90]

    # Eng: scores [80, 110, 110]
    assert results[3]["first2"] == [80, 110]
    assert results[3]["last2"] == [110, 110]


def test_window_minN_maxN(collection):
    """Test $minN and $maxN operators in window fields."""
    collection, conn = collection
    pipeline = [
        {
            "$setWindowFields": {
                "partitionBy": "$dept",
                "output": {
                    "min2": {"$minN": {"input": "$score", "n": 2}},
                    "max2": {"$maxN": {"input": "$score", "n": 2}},
                },
            }
        },
        {"$sort": {"_id": 1}},
    ]

    results = list(collection.aggregate(pipeline))

    # Sales: scores [100, 90, 90] -> min2=[90, 90], max2=[100, 90]
    assert sorted(results[0]["min2"]) == [90, 90]
    assert sorted(results[0]["max2"], reverse=True) == [100, 90]

    # Eng: scores [80, 110, 110] -> min2=[80, 110], max2=[110, 110]
    assert sorted(results[3]["min2"]) == [80, 110]
    assert sorted(results[3]["max2"], reverse=True) == [110, 110]


def test_window_n_as_expression(collection):
    """Test using $number as expression in $firstN/$lastN."""
    collection, conn = collection
    # Add test data for dept "C"
    collection.insert_one({"_id": 7, "score": 100, "dept": "C", "count": 1})
    collection.insert_one({"_id": 8, "score": 200, "dept": "C", "count": 1})

    pipeline = [
        {"$match": {"dept": "C"}},
        {
            "$setWindowFields": {
                "sortBy": {"_id": 1},
                "output": {
                    "dynamicFirst": {
                        "$firstN": {"input": "$score", "n": "$count"}
                    }
                },
            }
        },
        {"$sort": {"_id": 1}},
    ]

    results = list(collection.aggregate(pipeline))
    # Should only take 1 element because count=1
    assert results[0]["dynamicFirst"] == [100]
    assert results[1]["dynamicFirst"] == [100]


def test_window_top_bottom(collection):
    """Test $top and $bottom operators in window fields."""
    collection, conn = collection
    pipeline = [
        {
            "$setWindowFields": {
                "partitionBy": "$dept",
                "sortBy": {"score": -1},
                "output": {
                    "topScore": {
                        "$top": {"output": "$score", "sortBy": {"score": -1}}
                    },
                    "bottomScore": {
                        "$bottom": {"output": "$score", "sortBy": {"score": -1}}
                    },
                },
            }
        },
        {"$sort": {"_id": 1}},
    ]

    # 1. SQL Tier
    set_force_fallback(False)
    results = list(collection.aggregate(pipeline))
    assert results[0]["topScore"] == 100  # Sales max
    assert results[0]["bottomScore"] == 90  # Sales min
    assert results[3]["topScore"] == 110  # Eng max
    assert results[3]["bottomScore"] == 80  # Eng min

    # 2. Python Fallback
    set_force_fallback(True)
    results_py = list(collection.aggregate(pipeline))
    set_force_fallback(False)
    assert results_py == results


def test_window_topN_bottomN(collection):
    """Test $topN and $bottomN operators in window fields."""
    collection, conn = collection
    pipeline = [
        {
            "$setWindowFields": {
                "partitionBy": "$dept",
                "sortBy": {"score": -1},
                "output": {
                    "top2": {
                        "$topN": {
                            "n": 2,
                            "sortBy": {"score": -1},
                            "output": "$name",
                        }
                    },
                    "bottom2": {
                        "$bottomN": {
                            "n": 2,
                            "sortBy": {"score": -1},
                            "output": "$name",
                        }
                    },
                },
            }
        },
        {"$sort": {"_id": 1}},
    ]

    # These use Python fallback (Tier 3)
    results = list(collection.aggregate(pipeline))

    # Sales: {A:100, B:90, C:90} sorted desc by score -> [A, B, C] (or [A, C, B])
    assert results[0]["top2"] == ["A", "B"] or results[0]["top2"] == ["A", "C"]
    assert len(results[0]["bottom2"]) == 2
    assert "C" in results[0]["bottom2"]

    # Eng: {D:80, E:110, F:110} sorted desc by score -> [E, F, D]
    assert "E" in results[3]["top2"]
    assert "F" in results[3]["top2"]
    assert results[3]["bottom2"] == ["F", "D"] or results[3]["bottom2"] == [
        "E",
        "D",
    ]


def test_window_addToSet(collection):
    """Test $addToSet operator in window fields."""
    collection, conn = collection
    pipeline = [
        {
            "$setWindowFields": {
                "partitionBy": "$dept",
                "output": {"allScores": {"$addToSet": "$score"}},
            }
        },
        {"$sort": {"_id": 1}},
    ]

    results = list(collection.aggregate(pipeline))
    # Sales scores: [100, 90, 90] -> {90, 100}
    assert set(results[0]["allScores"]) == {90, 100}
    # Eng scores: [80, 110, 110] -> {80, 110}
    assert set(results[3]["allScores"]) == {80, 110}
