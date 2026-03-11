import pytest
from neosqlite import Connection


@pytest.fixture
def collection(tmp_path):
    db_path = str(tmp_path / "test_window_math.db")

    with Connection(db_path) as conn:
        coll = conn.collection
        coll.insert_many(
            [
                {"_id": 1, "time": 1, "x": 10, "y": 100, "cat": "A"},
                {"_id": 2, "time": 2, "x": 20, "y": 200, "cat": "A"},
                {"_id": 3, "time": 3, "x": 40, "y": 300, "cat": "A"},
                {"_id": 4, "time": 1, "x": 5, "y": 50, "cat": "B"},
                {"_id": 5, "time": 2, "x": 15, "y": 150, "cat": "B"},
            ]
        )
        yield coll


def test_window_derivative(collection):
    """Test $derivative operator (Python fallback)."""
    pipeline = [
        {
            "$setWindowFields": {
                "partitionBy": "$cat",
                "sortBy": {"time": 1},
                "output": {
                    "deriv": {
                        "$derivative": {"input": "$x", "unit": "unit"},
                        "window": {"documents": [-1, 0]},
                    }
                },
            }
        },
        {"$sort": {"_id": 1}},
    ]

    results = list(collection.aggregate(pipeline))
    # A: (20-10)/(2-1)=10, (40-20)/(3-2)=20
    assert results[0]["deriv"] is None
    assert results[1]["deriv"] == 10.0
    assert results[2]["deriv"] == 20.0
    # B: (15-5)/(2-1)=10
    assert results[3]["deriv"] is None
    assert results[4]["deriv"] == 10.0


def test_window_integral(collection):
    """Test $integral operator (Python fallback)."""
    pipeline = [
        {
            "$setWindowFields": {
                "partitionBy": "$cat",
                "sortBy": {"time": 1},
                "output": {
                    "integ": {
                        "$integral": {"input": "$x", "unit": "unit"},
                        "window": {"documents": ["unbounded", "current"]},
                    }
                },
            }
        },
        {"$sort": {"_id": 1}},
    ]

    results = list(collection.aggregate(pipeline))
    # A:
    # id 1: 0
    # id 2: (10+20)/2 * (2-1) = 15
    # id 3: 15 + (20+40)/2 * (3-2) = 15 + 30 = 45
    assert results[0]["integ"] == 0.0
    assert results[1]["integ"] == 15.0
    assert results[2]["integ"] == 45.0


def test_window_covariance(collection):
    """Test $covariancePop and $covarianceSamp (Python fallback)."""
    pipeline = [
        {
            "$setWindowFields": {
                "partitionBy": "$cat",
                "output": {
                    "covPop": {"$covariancePop": ["$x", "$y"]},
                    "covSamp": {"$covarianceSamp": ["$x", "$y"]},
                },
            }
        },
        {"$sort": {"_id": 1}},
    ]

    results = list(collection.aggregate(pipeline))
    # For cat A: x=[10, 20, 40], y=[100, 200, 300]
    # mean_x = 70/3 = 23.33, mean_y = 600/3 = 200
    # covPop = ((10-23.33)*(100-200) + (20-23.33)*(200-200) + (40-23.33)*(300-200)) / 3
    #        = ((-13.33)*(-100) + (-3.33)*0 + (16.66)*100) / 3
    #        = (1333.33 + 1666.66) / 3 = 3000 / 3 = 1000
    assert abs(results[0]["covPop"] - 1000.0) < 0.001
    # covSamp = 3000 / (3-1) = 1500
    assert abs(results[0]["covSamp"] - 1500.0) < 0.001


def test_window_ema(collection):
    """Test $expMovingAvg (Python fallback)."""
    pipeline = [
        {
            "$setWindowFields": {
                "partitionBy": "$cat",
                "sortBy": {"time": 1},
                "output": {
                    "ema": {"$expMovingAvg": {"input": "$x", "alpha": 0.5}}
                },
            }
        },
        {"$sort": {"_id": 1}},
    ]

    results = list(collection.aggregate(pipeline))
    # A: x=[10, 20, 40]
    # id 1: 10
    # id 2: 20*0.5 + 10*0.5 = 15
    # id 3: 40*0.5 + 15*0.5 = 20 + 7.5 = 27.5
    assert results[0]["ema"] == 10.0
    assert results[1]["ema"] == 15.0
    assert results[2]["ema"] == 27.5
