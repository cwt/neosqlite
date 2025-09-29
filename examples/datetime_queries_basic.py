#!/usr/bin/env python3
"""
Basic DateTime Query Examples for NeoSQLite

This example demonstrates how to use MongoDB-style datetime queries
with NeoSQLite's datetime query processor.

Compatible with PyMongo datetime query syntax.
"""

from neosqlite import Connection


def basic_datetime_examples():
    """Demonstrate basic datetime query operations."""

    # Connect to in-memory database
    with Connection(":memory:") as db:
        collection = db["events"]

        # Insert sample documents with datetime fields
        events = [
            {
                "name": "User Login",
                "timestamp": "2023-01-15T08:30:00",
                "user_id": "user_001",
                "event_type": "login",
            },
            {
                "name": "Purchase Order",
                "timestamp": "2023-01-15T14:45:30",
                "user_id": "user_002",
                "event_type": "purchase",
                "amount": 99.99,
            },
            {
                "name": "User Logout",
                "timestamp": "2023-01-15T18:20:15",
                "user_id": "user_001",
                "event_type": "logout",
            },
            {
                "name": "System Maintenance",
                "timestamp": "2023-01-16T02:00:00",
                "event_type": "maintenance",
            },
            {
                "name": "Weekly Report",
                "timestamp": "2023-01-22T09:00:00",
                "event_type": "report",
            },
        ]

        # Insert documents
        for event in events:
            collection.insert_one(event)

        print("=== Basic DateTime Query Examples ===\n")

        # Example 1: Find events after a specific datetime
        print("1. Events after Jan 15, 2023 12:00 PM:")
        query1 = {"timestamp": {"$gt": "2023-01-15T12:00:00"}}
        results1 = list(collection.find(query1))
        for event in results1:
            print(f"   - {event['name']}: {event['timestamp']}")
        print()

        # Example 2: Find events in a datetime range
        print("2. Events between Jan 15 and Jan 16:")
        query2 = {
            "timestamp": {
                "$gte": "2023-01-15T00:00:00",
                "$lt": "2023-01-16T00:00:00",
            }
        }
        results2 = list(collection.find(query2))
        for event in results2:
            print(f"   - {event['name']}: {event['timestamp']}")
        print()

        # Example 3: Find events on or before a specific datetime
        print("3. Events on or before Jan 15, 2023 6:00 PM:")
        query3 = {"timestamp": {"$lte": "2023-01-15T18:00:00"}}
        results3 = list(collection.find(query3))
        for event in results3:
            print(f"   - {event['name']}: {event['timestamp']}")
        print()

        # Example 4: Find events with exact datetime match
        print("4. Events at exactly Jan 15, 2023 8:30 AM:")
        query4 = {"timestamp": "2023-01-15T08:30:00"}
        results4 = list(collection.find(query4))
        for event in results4:
            print(f"   - {event['name']}: {event['timestamp']}")
        print()


def datetime_operators_examples():
    """Demonstrate various datetime comparison operators."""

    with Connection(":memory:") as db:
        collection = db["logs"]

        # Insert log entries
        logs = [
            {
                "level": "INFO",
                "timestamp": "2023-03-01T10:15:30",
                "message": "Application started",
            },
            {
                "level": "ERROR",
                "timestamp": "2023-03-01T10:16:45",
                "message": "Database connection failed",
            },
            {
                "level": "WARNING",
                "timestamp": "2023-03-01T10:17:20",
                "message": "High memory usage",
            },
            {
                "level": "INFO",
                "timestamp": "2023-03-01T10:20:00",
                "message": "Memory cleared",
            },
            {
                "level": "ERROR",
                "timestamp": "2023-03-01T10:25:10",
                "message": "Critical system error",
            },
        ]

        for log in logs:
            collection.insert_one(log)

        print("=== DateTime Comparison Operators ===\n")

        # $gt: Greater than
        print("1. Logs after 10:16 AM:")
        gt_results = list(
            collection.find({"timestamp": {"$gt": "2023-03-01T10:16:00"}})
        )
        for log in gt_results:
            print(f"   - [{log['level']}] {log['timestamp']}: {log['message']}")
        print()

        # $gte: Greater than or equal
        print("2. Logs at or after 10:16:45:")
        gte_results = list(
            collection.find({"timestamp": {"$gte": "2023-03-01T10:16:45"}})
        )
        for log in gte_results:
            print(f"   - [{log['level']}] {log['timestamp']}: {log['message']}")
        print()

        # $lt: Less than
        print("3. Logs before 10:20 AM:")
        lt_results = list(
            collection.find({"timestamp": {"$lt": "2023-03-01T10:20:00"}})
        )
        for log in lt_results:
            print(f"   - [{log['level']}] {log['timestamp']}: {log['message']}")
        print()

        # $lte: Less than or equal
        print("4. Logs at or before 10:17:20:")
        lte_results = list(
            collection.find({"timestamp": {"$lte": "2023-03-01T10:17:20"}})
        )
        for log in lte_results:
            print(f"   - [{log['level']}] {log['timestamp']}: {log['message']}")
        print()

        # $ne: Not equal
        print("5. Logs NOT at exactly 10:15:30:")
        ne_results = list(
            collection.find({"timestamp": {"$ne": "2023-03-01T10:15:30"}})
        )
        for log in ne_results:
            print(f"   - [{log['level']}] {log['timestamp']}: {log['message']}")
        print()


def datetime_with_other_conditions():
    """Demonstrate combining datetime queries with other conditions."""

    with Connection(":memory:") as db:
        collection = db["user_activities"]

        # Insert user activity data
        activities = [
            {
                "user_id": "user_001",
                "activity": "login",
                "timestamp": "2023-02-01T09:00:00",
                "session_duration": 1800,
            },
            {
                "user_id": "user_002",
                "activity": "login",
                "timestamp": "2023-02-01T10:30:00",
                "session_duration": 3600,
            },
            {
                "user_id": "user_001",
                "activity": "purchase",
                "timestamp": "2023-02-01T11:15:00",
                "amount": 49.99,
            },
            {
                "user_id": "user_003",
                "activity": "login",
                "timestamp": "2023-02-01T14:20:00",
                "session_duration": 900,
            },
            {
                "user_id": "user_002",
                "activity": "logout",
                "timestamp": "2023-02-01T15:45:00",
                "session_duration": 18000,
            },
            {
                "user_id": "user_001",
                "activity": "login",
                "timestamp": "2023-02-02T08:45:00",
                "session_duration": 2700,
            },
        ]

        for activity in activities:
            collection.insert_one(activity)

        print("=== DateTime Queries with Other Conditions ===\n")

        # Combined conditions
        print("1. User 001 activities after Feb 1, 2023 10:00 AM:")
        query1 = {
            "user_id": "user_001",
            "timestamp": {"$gt": "2023-02-01T10:00:00"},
        }
        results1 = list(collection.find(query1))
        for activity in results1:
            print(f"   - {activity['activity']} at {activity['timestamp']}")
        print()

        # Multiple datetime conditions
        print("2. Activities between specific times on Feb 1:")
        query2 = {
            "timestamp": {
                "$gte": "2023-02-01T10:00:00",
                "$lt": "2023-02-01T16:00:00",
            }
        }
        results2 = list(collection.find(query2))
        for activity in results2:
            print(
                f"   - {activity['user_id']}: {activity['activity']} at {activity['timestamp']}"
            )
        print()

        # DateTime with numerical conditions
        print("3. Long sessions (duration > 1 hour) on Feb 1:")
        query3 = {
            "timestamp": {"$lt": "2023-02-02T00:00:00"},
            "session_duration": {"$gt": 3600},  # More than 1 hour
        }
        results3 = list(collection.find(query3))
        for activity in results3:
            duration_hours = activity.get("session_duration", 0) / 3600
            print(
                f"   - {activity['user_id']}: {duration_hours:.1f}h session at {activity['timestamp']}"
            )
        print()


if __name__ == "__main__":
    print("NeoSQLite DateTime Query Examples")
    print("=" * 40)
    print()

    basic_datetime_examples()
    datetime_operators_examples()
    datetime_with_other_conditions()

    print("All datetime query examples completed successfully!")
