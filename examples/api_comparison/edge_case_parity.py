"""Differential checks for edge cases missed by the existing comparisons.

Covers BSON bool/int distinctions, null and missing fields, numeric dotted
keys, nested arrays, ObjectId ``$graphLookup`` keys, index hints, and
validator persistence across reconnection.
"""

import os
import tempfile
import warnings

import neosqlite
from neosqlite.objectid import ObjectId

from .reporter import reporter
from .utils import get_mongo_client, sanitize_for_mongodb

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)

CATEGORY = "Edge Case Parity"


def _run_find(collection, query, **kwargs):
    try:
        return list(collection.find(query, **kwargs))
    except Exception as exc:
        return f"Error: {exc}"


def _run_aggregate(collection, pipeline):
    try:
        return list(collection.aggregate(pipeline))
    except Exception as exc:
        return f"Error: {exc}"


def compare_edge_case_parity():
    """Compare edge-case behavior between NeoSQLite and MongoDB."""
    print("\n=== Edge Case Parity Comparison ===")

    docs = [
        {
            "_id": 1,
            "flag": True,
            "v": None,
            "a": {"1": "doc"},
            "arr": ["x", "arr"],
            "nested": [[1, 2]],
        },
        {
            "_id": 2,
            "flag": 1,
            "v": "text",
            "a": {"1": "other"},
            "arr": ["x", "y"],
            "nested": [[3]],
        },
        {
            "_id": 3,
            "flag": False,
            "v": None,
            "a": {"1": "doc"},
            "arr": ["x", "arr"],
            "nested": [[1, 2]],
        },
    ]
    queries = {
        "bool_true": {"flag": True},
        "bool_one": {"flag": 1},
        "bool_in_int": {"flag": {"$in": [1]}},
        "null": {"v": None},
        "missing": {"v": {"$exists": False}},
        "ne_null": {"v": {"$ne": None}},
        "numeric_document_key": {"a.1": "doc"},
        "numeric_array_index": {"arr.1": "arr"},
        "nested_array_exact": {"nested": [[1, 2]]},
    }

    root, child, grandchild = ObjectId(), ObjectId(), ObjectId()
    nodes = [
        {"_id": root, "name": "root"},
        {"_id": child, "name": "child", "parent": root},
        {"_id": grandchild, "name": "grandchild", "parent": child},
    ]
    graph_pipeline = [
        {"$match": {"_id": grandchild}},
        {
            "$graphLookup": {
                "from": "edge_nodes",
                "startWith": "$parent",
                "connectFromField": "parent",
                "connectToField": "_id",
                "as": "ancestors",
            }
        },
        {"$sort": {"_id": 1}},
    ]

    validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "properties": {
                "role": {"bsonType": "string", "pattern": "^admin$"}
            },
        }
    }

    neo_results = {}
    neo_validator_rejected = False
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = os.path.join(tmp_dir, "edge.db")
        with neosqlite.Connection(db_path) as conn:
            coll = conn.edge_docs
            coll.insert_many(docs)
            coll.create_index([("a", 1)])
            for name, query in queries.items():
                neo_results[name] = _run_find(coll, query)
            neo_results["hint_id"] = _run_find(coll, {}, hint="_id_")
            neo_results["hint_field"] = _run_find(coll, {}, hint=[("a", 1)])
            nodes_coll = conn.edge_nodes
            nodes_coll.insert_many(nodes)
            neo_results["graph_lookup_objectids"] = _run_aggregate(
                nodes_coll, graph_pipeline
            )
            conn.create_collection("edge_validated", validator=validator)
            conn.edge_validated.insert_one({"_id": 1, "role": "admin"})

        # Reopen to verify validator metadata survived reconnection.
        with neosqlite.Connection(db_path) as conn:
            try:
                conn.edge_validated.insert_one({"_id": 2, "role": "bad!"})
            except Exception:
                neo_validator_rejected = True
            else:
                neo_validator_rejected = False

    client = get_mongo_client()
    mongo_results = {}
    mongo_validator_rejected = False
    if client:
        db = client.test_database
        coll = db.edge_docs
        coll.delete_many({})
        coll.insert_many([sanitize_for_mongodb(doc) for doc in docs])
        try:
            coll.create_index([("a", 1)])
        except Exception:
            pass
        for name, query in queries.items():
            mongo_results[name] = _run_find(coll, query)
        mongo_results["hint_id"] = _run_find(coll, {}, hint="_id_")
        mongo_results["hint_field"] = _run_find(coll, {}, hint=[("a", 1)])

        nodes_coll = db.edge_nodes
        nodes_coll.delete_many({})
        nodes_coll.insert_many([sanitize_for_mongodb(doc) for doc in nodes])
        mongo_results["graph_lookup_objectids"] = _run_aggregate(
            nodes_coll, graph_pipeline
        )

        db.drop_collection("edge_validated")
        db.create_collection("edge_validated", validator=validator)
        db.edge_validated.insert_one({"_id": 1, "role": "admin"})
        try:
            db.edge_validated.insert_one({"_id": 2, "role": "bad!"})
        except Exception:
            mongo_validator_rejected = True
        else:
            mongo_validator_rejected = False

    for name in list(queries) + [
        "hint_id",
        "hint_field",
        "graph_lookup_objectids",
    ]:
        reporter.record_comparison(
            CATEGORY,
            name,
            neo_results.get(name),
            mongo_results.get(name),
            skip_reason="MongoDB not available" if not client else None,
        )
    reporter.record_result(
        CATEGORY,
        "validator_survives_reopen",
        neo_validator_rejected == mongo_validator_rejected,
        neo_result=neo_validator_rejected,
        mongo_result=mongo_validator_rejected,
        skip_reason="MongoDB not available" if not client else None,
    )
