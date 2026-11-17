"""Compare MongoDB-compatible ``$meta`` aggregation behavior.

Text score values differ between MongoDB and NeoSQLite's FTS5-backed engine,
so comparisons use stable structural summaries: matched ids, presence and
positivity of the score field, absence of the internal ``_textScore`` field,
and normalized error categories.
"""

from __future__ import annotations

import warnings

import neosqlite

from .reporter import reporter
from .timing import (
    end_mongo_timing,
    end_neo_timing,
    set_accumulation_mode,
    start_mongo_timing,
    start_neo_timing,
)
from .utils import get_mongo_client

warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*NeoSQLite extension.*"
)

_ARTICLES = [
    {"_id": 1, "title": "Python basics", "category": "lang"},
    {"_id": 2, "title": "Advanced Python", "category": "lang"},
    {"_id": 3, "title": "SQL guide", "category": "db"},
]


def _summarize(docs: list[dict], score_field: str) -> dict:
    """Return a comparable summary of scored documents."""
    return {
        "ids": sorted(doc["_id"] for doc in docs),
        "has_score": all(
            score_field in doc
            and isinstance(doc[score_field], (int, float))
            and doc[score_field] > 0
            for doc in docs
        ),
        "no_internal_leak": all("_textScore" not in doc for doc in docs),
    }


def _error_summary(operation) -> str | None:
    """Normalize engine-specific exceptions into shared error categories."""
    try:
        operation()
    except (
        Exception
    ) as exc:  # noqa: BLE001 - both engines raise their own types
        message = str(exc)
        if "text score metadata" in message:
            return "text score metadata required"
        if "Unsupported $meta field" in message:
            return "unsupported meta field"
        return f"unexpected error: {message}"
    return None


def _scored_pipeline(score_field: str) -> list[dict]:
    return [
        {"$match": {"$text": {"$search": "Python"}}},
        {"$addFields": {score_field: {"$meta": "textScore"}}},
        {"$sort": {"_id": 1}},
    ]


def _neo_results() -> dict:
    """Collect NeoSQLite results for every ``$meta`` scenario."""
    results: dict = {}
    with neosqlite.Connection(":memory:") as conn:
        collection = conn.meta_articles
        collection.insert_many([dict(doc) for doc in _ARTICLES])
        collection.create_index("title", fts=True)

        set_accumulation_mode(True)
        start_neo_timing()
        try:
            results["addfields"] = _summarize(
                list(collection.aggregate(_scored_pipeline("relevance"))),
                "relevance",
            )
            results["project"] = _summarize(
                list(
                    collection.aggregate(
                        [
                            {"$match": {"$text": {"$search": "Python"}}},
                            {
                                "$project": {
                                    "title": 1,
                                    "score": {"$meta": "textScore"},
                                }
                            },
                            {"$sort": {"_id": 1}},
                        ]
                    )
                ),
                "score",
            )
            results["index_key_field"] = [
                doc
                for doc in collection.aggregate(
                    [
                        {
                            "$addFields": {
                                "k": {"$meta": "indexKey"},
                                "kept": 1,
                            }
                        },
                        {"$sort": {"_id": 1}},
                    ]
                )
            ]
            results["index_key_nested"] = [
                doc
                for doc in collection.aggregate(
                    [
                        {
                            "$addFields": {
                                "value": {"$add": [{"$meta": "indexKey"}, 1]}
                            }
                        },
                        {"$sort": {"_id": 1}},
                    ]
                )
            ]
            results["no_text"] = _error_summary(
                lambda: list(
                    collection.aggregate(
                        [{"$addFields": {"score": {"$meta": "textScore"}}}]
                    )
                )
            )
            results["unknown_field"] = _error_summary(
                lambda: list(
                    collection.aggregate(
                        [{"$addFields": {"score": {"$meta": "unknownField"}}}]
                    )
                )
            )
        finally:
            end_neo_timing()
    return results


def _mongo_results(client) -> dict:
    """Collect MongoDB results for every ``$meta`` scenario."""
    results: dict = {}
    if client is None:
        return results

    mongo_db = client.test_database
    collection = mongo_db.meta_articles
    collection.delete_many({})
    for index_name in list(collection.index_information()):
        if index_name != "_id_":
            try:
                collection.drop_index(index_name)
            except Exception:  # noqa: BLE001 - best-effort cleanup
                pass
    collection.insert_many([dict(doc) for doc in _ARTICLES])
    collection.create_index([("title", "text")])

    set_accumulation_mode(True)
    start_mongo_timing()
    try:
        results["addfields"] = _summarize(
            list(collection.aggregate(_scored_pipeline("relevance"))),
            "relevance",
        )
        results["project"] = _summarize(
            list(
                collection.aggregate(
                    [
                        {"$match": {"$text": {"$search": "Python"}}},
                        {
                            "$project": {
                                "title": 1,
                                "score": {"$meta": "textScore"},
                            }
                        },
                        {"$sort": {"_id": 1}},
                    ]
                )
            ),
            "score",
        )
        results["index_key_field"] = [
            doc
            for doc in collection.aggregate(
                [
                    {
                        "$addFields": {
                            "k": {"$meta": "indexKey"},
                            "kept": 1,
                        }
                    },
                    {"$sort": {"_id": 1}},
                ]
            )
        ]
        results["index_key_nested"] = [
            doc
            for doc in collection.aggregate(
                [
                    {
                        "$addFields": {
                            "value": {"$add": [{"$meta": "indexKey"}, 1]}
                        }
                    },
                    {"$sort": {"_id": 1}},
                ]
            )
        ]
        results["no_text"] = _error_summary(
            lambda: list(
                collection.aggregate(
                    [{"$addFields": {"score": {"$meta": "textScore"}}}]
                )
            )
        )
        results["unknown_field"] = _error_summary(
            lambda: list(
                collection.aggregate(
                    [{"$addFields": {"score": {"$meta": "unknownField"}}}]
                )
            )
        )
    finally:
        end_mongo_timing()
    return results


def compare_meta_expression() -> None:
    """Compare ``$meta`` aggregation semantics against PyMongo."""
    print("\n=== $meta Expression Comparison ===")

    client = get_mongo_client()
    mongo_available = client is not None

    neo = _neo_results()
    mongo = _mongo_results(client)

    skip = "MongoDB not available" if not mongo_available else None
    comparisons = [
        ("textScore_in_addFields", "addfields"),
        ("textScore_in_project", "project"),
        ("indexKey_field_omitted", "index_key_field"),
        ("indexKey_nested_null", "index_key_nested"),
        ("textScore_without_text_errors", "no_text"),
        ("unknown_meta_field_errors", "unknown_field"),
    ]
    for name, key in comparisons:
        reporter.record_comparison(
            "$meta Expression",
            name,
            neo.get(key),
            mongo.get(key),
            skip_reason=skip,
        )
