#!/usr/bin/env python
"""
Demo script for high-priority $expr operators in NeoSQLite.

Shows usage of:
- Date Arithmetic: $dateAdd, $dateSubtract, $dateDiff
- Regex Operations: $regexFind, $regexFindAll
- Array Transformation: $filter, $map, $reduce
"""

import neosqlite


def main():
    # Create in-memory database
    conn = neosqlite.Connection(":memory:")
    collection = conn["demo"]

    # Insert sample documents
    collection.insert_many(
        [
            {
                "name": "Event A",
                "date": "2024-01-15T10:30:00",
                "numbers": [1, 5, 8, 2, 9, 3],
                "text": "Contact us at support@example.com",
            },
            {
                "name": "Event B",
                "date": "2024-06-20T15:45:00",
                "numbers": [10, 20, 30],
                "text": "Call 555-1234 or email sales@company.org",
            },
            {
                "name": "Event C",
                "date": "2024-12-25T20:00:00",
                "numbers": [2, 4, 6, 8],
                "text": "No contact info",
            },
        ]
    )

    print("=" * 70)
    print("NeoSQLite High-Priority $expr Operators Demo")
    print("=" * 70)

    # 1. Date Arithmetic - $dateAdd
    print("\n1. Find events that will be in 2025 after adding 1 year:")
    results = list(
        collection.find(
            {
                "$expr": {
                    "$eq": [
                        {"$year": [{"$dateAdd": ["$date", 1, "year"]}]},
                        2025,
                    ]
                }
            }
        )
    )
    for doc in results:
        print(f"   - {doc['name']}: {doc['date']}")

    # 2. Date Arithmetic - $dateDiff
    print("\n2. Find events more than 150 days from end of 2024:")
    results = list(
        collection.find(
            {
                "$expr": {
                    "$gt": [
                        {"$dateDiff": ["$date", "2024-12-31T23:59:59", "day"]},
                        150,
                    ]
                }
            }
        )
    )
    for doc in results:
        print(f"   - {doc['name']}: {doc['date']}")

    # 3. Array Transformation - $filter
    print("\n3. Find documents with more than 2 numbers > 5:")
    results = list(
        collection.find(
            {
                "$expr": {
                    "$gt": [
                        {
                            "$size": [
                                {
                                    "$filter": {
                                        "input": "$numbers",
                                        "as": "n",
                                        "cond": {"$gt": ["$$n", 5]},
                                    }
                                }
                            ]
                        },
                        2,
                    ]
                }
            }
        )
    )
    for doc in results:
        filtered = [n for n in doc["numbers"] if n > 5]
        print(f"   - {doc['name']}: {doc['numbers']} → {filtered}")

    # 4. Array Transformation - $map
    print("\n4. Find documents where doubled values contain 20:")
    results = list(
        collection.find(
            {
                "$expr": {
                    "$in": [
                        20,
                        {
                            "$map": {
                                "input": "$numbers",
                                "as": "n",
                                "in": {"$multiply": ["$$n", 2]},
                            }
                        },
                    ]
                }
            }
        )
    )
    for doc in results:
        doubled = [n * 2 for n in doc["numbers"]]
        print(f"   - {doc['name']}: {doc['numbers']} → {doubled}")

    # 5. Array Transformation - $reduce
    print("\n5. Find documents where sum of numbers > 15:")
    results = list(
        collection.find(
            {
                "$expr": {
                    "$gt": [
                        {
                            "$reduce": {
                                "input": "$numbers",
                                "initialValue": 0,
                                "in": {"$add": ["$$value", "$$this"]},
                            }
                        },
                        15,
                    ]
                }
            }
        )
    )
    for doc in results:
        total = sum(doc["numbers"])
        print(f"   - {doc['name']}: {doc['numbers']} → sum={total}")

    # 6. Regex - $regexFind
    print("\n6. Find documents containing email addresses:")
    results = list(
        collection.find(
            {
                "$expr": {
                    "$ne": [
                        {
                            "$regexFind": {
                                "input": "$text",
                                "regex": r"\w+@\w+\.\w+",
                            }
                        },
                        None,
                    ]
                }
            }
        )
    )
    for doc in results:
        print(f"   - {doc['name']}: '{doc['text']}'")

    # 7. Regex - $regexFindAll
    print("\n7. Find documents with more than 2 words:")
    results = list(
        collection.find(
            {
                "$expr": {
                    "$gt": [
                        {
                            "$size": [
                                {
                                    "$regexFindAll": {
                                        "input": "$text",
                                        "regex": r"\w+",
                                    }
                                }
                            ]
                        },
                        2,
                    ]
                }
            }
        )
    )
    for doc in results:
        words = doc["text"].split()
        print(f"   - {doc['name']}: {len(words)} words")

    # 8. Combined: $filter + $map
    print("\n8. Complex: Filter numbers > 3, then double them:")
    results = list(
        collection.find(
            {
                "$expr": {
                    "$gt": [
                        {
                            "$size": [
                                {
                                    "$map": {
                                        "input": {
                                            "$filter": {
                                                "input": "$numbers",
                                                "as": "n",
                                                "cond": {"$gt": ["$$n", 3]},
                                            }
                                        },
                                        "as": "n",
                                        "in": {"$multiply": ["$$n", 2]},
                                    }
                                }
                            ]
                        },
                        1,
                    ]
                }
            }
        )
    )
    for doc in results:
        filtered = [n for n in doc["numbers"] if n > 3]
        doubled = [n * 2 for n in filtered]
        print(
            f"   - {doc['name']}: {doc['numbers']} → filter>{3}={filtered} → doubled={doubled}"
        )

    print("\n" + "=" * 70)
    print("Demo completed successfully!")
    print("=" * 70)

    conn.close()


if __name__ == "__main__":
    main()
