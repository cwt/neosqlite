"""Module for comparing text search between NeoSQLite and PyMongo"""

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
    {
        "_id": 1,
        "title": "Database Optimization Guide",
        "subtitle": "Advanced Performance Tuning",
        "body": "Techniques for high throughput query planning in databases",
        "status": "published",
    },
    {
        "_id": 2,
        "title": "Python Programming Intro",
        "subtitle": "Getting Started with Python",
        "body": "Basic syntax, loops, and data structures in Python",
        "status": "published",
    },
    {
        "_id": 3,
        "title": "Full-Text Search in Modern Databases",
        "subtitle": "Indexing and Ranking Algorithms",
        "body": "How BM25 and inverted indexes power search in database systems",
        "status": "draft",
    },
]


def compare_text_search():
    """Compare text search capabilities against PyMongo"""
    print("\n=== Text Search Comparison ===")

    client = get_mongo_client()
    mongo_available = client is not None

    neo_results = {}
    with neosqlite.Connection(":memory:") as neo_conn:
        neo_collection = neo_conn.test_collection
        neo_collection.insert_many([dict(doc) for doc in _ARTICLES])

        set_accumulation_mode(True)
        if mongo_available:
            start_neo_timing()
        try:
            # 1. Create text index on multiple fields
            neo_collection.create_index(
                [("title", "text"), ("subtitle", "text"), ("body", "text")]
            )

            # 2. Text search (regex) - backwards-compatible check
            regex_docs = list(
                neo_collection.find(
                    {"body": {"$regex": "Python", "$options": "i"}}
                )
            )
            neo_results["regex_search"] = len(regex_docs)

            # 3. $text basic search matching multiple fields
            text_docs = list(
                neo_collection.find({"$text": {"$search": "database"}})
            )
            neo_results["text_search_basic"] = sorted(
                [d["_id"] for d in text_docs]
            )

            # 4. $text search combined with regular filter
            filtered_docs = list(
                neo_collection.find(
                    {"$text": {"$search": "database"}, "status": "published"}
                )
            )
            neo_results["text_search_filter"] = sorted(
                [d["_id"] for d in filtered_docs]
            )

            # 5. $text search with textScore projection and sorting
            score_cursor = neo_collection.find(
                {"$text": {"$search": "database"}},
                {"title": 1, "score": {"$meta": "textScore"}},
            ).sort([("score", {"$meta": "textScore"})])
            score_docs = list(score_cursor)
            neo_results["text_score_sort"] = {
                "count": len(score_docs),
                "ids": sorted([d["_id"] for d in score_docs]),
                "has_scores": all(
                    "score" in d and d["score"] > 0 for d in score_docs
                ),
                "is_descending": (
                    score_docs[0]["score"] >= score_docs[1]["score"]
                    if len(score_docs) > 1
                    else False
                ),
                "no_internal": all("_textScore" not in d for d in score_docs),
            }

            # 6. $text search clean projection (without score)
            clean_docs = list(
                neo_collection.find({"$text": {"$search": "Python"}})
            )
            neo_results["clean_projection"] = {
                "ids": [d["_id"] for d in clean_docs],
                "no_internal": all("_textScore" not in d for d in clean_docs),
            }

            # 7. $text search pagination (sort + skip + limit)
            paged_docs = list(
                neo_collection.find(
                    {"$text": {"$search": "database"}},
                    {"title": 1, "score": {"$meta": "textScore"}},
                )
                .sort([("score", {"$meta": "textScore"})])
                .skip(1)
                .limit(1)
            )
            neo_results["pagination"] = {
                "count": len(paged_docs),
                "has_score": (
                    len(paged_docs) == 1
                    and "score" in paged_docs[0]
                    and paged_docs[0]["score"] > 0
                ),
            }

            print(
                f"Neo text search: regex={neo_results['regex_search']}, "
                f"basic={neo_results['text_search_basic']}, "
                f"filter={neo_results['text_search_filter']}, "
                f"score_sort={neo_results['text_score_sort']['count']} docs"
            )
        except Exception as e:
            print(f"Neo text search: Error - {e}")
        finally:
            if mongo_available:
                end_neo_timing()

    mongo_results = {}
    if mongo_available:
        try:
            mongo_db = client.test_database
            mongo_collection = mongo_db.test_collection
            mongo_collection.delete_many({})
            # Drop any existing text index before recreating
            for idx_name in list(mongo_collection.index_information().keys()):
                if idx_name != "_id_":
                    try:
                        mongo_collection.drop_index(idx_name)
                    except Exception:
                        pass
            mongo_collection.insert_many([dict(doc) for doc in _ARTICLES])

            set_accumulation_mode(True)
            start_mongo_timing()
            try:
                # 1. Create text index on multiple fields
                mongo_collection.create_index(
                    [("title", "text"), ("subtitle", "text"), ("body", "text")]
                )

                # 2. Text search (regex) - backwards-compatible check
                regex_docs = list(
                    mongo_collection.find(
                        {"body": {"$regex": "Python", "$options": "i"}}
                    )
                )
                mongo_results["regex_search"] = len(regex_docs)

                # 3. $text basic search matching multiple fields
                text_docs = list(
                    mongo_collection.find({"$text": {"$search": "database"}})
                )
                mongo_results["text_search_basic"] = sorted(
                    [d["_id"] for d in text_docs]
                )

                # 4. $text search combined with regular filter
                filtered_docs = list(
                    mongo_collection.find(
                        {
                            "$text": {"$search": "database"},
                            "status": "published",
                        }
                    )
                )
                mongo_results["text_search_filter"] = sorted(
                    [d["_id"] for d in filtered_docs]
                )

                # 5. $text search with textScore projection and sorting
                score_cursor = mongo_collection.find(
                    {"$text": {"$search": "database"}},
                    {"title": 1, "score": {"$meta": "textScore"}},
                ).sort([("score", {"$meta": "textScore"})])
                score_docs = list(score_cursor)
                mongo_results["text_score_sort"] = {
                    "count": len(score_docs),
                    "ids": sorted([d["_id"] for d in score_docs]),
                    "has_scores": all(
                        "score" in d and d["score"] > 0 for d in score_docs
                    ),
                    "is_descending": (
                        score_docs[0]["score"] >= score_docs[1]["score"]
                        if len(score_docs) > 1
                        else False
                    ),
                    "no_internal": all(
                        "_textScore" not in d for d in score_docs
                    ),
                }

                # 6. $text search clean projection (without score)
                clean_docs = list(
                    mongo_collection.find({"$text": {"$search": "Python"}})
                )
                mongo_results["clean_projection"] = {
                    "ids": [d["_id"] for d in clean_docs],
                    "no_internal": all(
                        "_textScore" not in d for d in clean_docs
                    ),
                }

                # 7. $text search pagination (sort + skip + limit)
                paged_docs = list(
                    mongo_collection.find(
                        {"$text": {"$search": "database"}},
                        {"title": 1, "score": {"$meta": "textScore"}},
                    )
                    .sort([("score", {"$meta": "textScore"})])
                    .skip(1)
                    .limit(1)
                )
                mongo_results["pagination"] = {
                    "count": len(paged_docs),
                    "has_score": (
                        len(paged_docs) == 1
                        and "score" in paged_docs[0]
                        and paged_docs[0]["score"] > 0
                    ),
                }

                print(
                    f"Mongo text search: regex={mongo_results['regex_search']}, "
                    f"basic={mongo_results['text_search_basic']}, "
                    f"filter={mongo_results['text_search_filter']}, "
                    f"score_sort={mongo_results['text_score_sort']['count']} docs"
                )
            except Exception as e:
                print(f"Mongo text search: Error - {e}")
            finally:
                end_mongo_timing()
        except Exception as e:
            print(f"Mongo text search: Setup Error - {e}")

    skip = "MongoDB not available" if not mongo_available else None

    # Record comparisons
    reporter.record_comparison(
        "Text Search",
        "regex_search",
        neo_results.get("regex_search"),
        mongo_results.get("regex_search"),
        skip_reason=skip,
    )
    reporter.record_comparison(
        "Text Search",
        "text_search_basic",
        neo_results.get("text_search_basic"),
        mongo_results.get("text_search_basic"),
        skip_reason=skip,
    )
    reporter.record_comparison(
        "Text Search",
        "text_search_filter",
        neo_results.get("text_search_filter"),
        mongo_results.get("text_search_filter"),
        skip_reason=skip,
    )
    reporter.record_comparison(
        "Text Search",
        "text_score_projection_and_sort",
        neo_results.get("text_score_sort"),
        mongo_results.get("text_score_sort"),
        skip_reason=skip,
    )
    reporter.record_comparison(
        "Text Search",
        "clean_projection_no_leak",
        neo_results.get("clean_projection"),
        mongo_results.get("clean_projection"),
        skip_reason=skip,
    )
    reporter.record_comparison(
        "Text Search",
        "pagination_with_textScore",
        neo_results.get("pagination"),
        mongo_results.get("pagination"),
        skip_reason=skip,
    )
