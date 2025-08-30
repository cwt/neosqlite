# coding: utf-8
"""
Explore potential solutions for advanced cases.
"""

import neosqlite
import json


def explore_object_array_solution():
    """Explore potential solution for object arrays with FTS."""
    # Create a connection and collection
    conn = neosqlite.Connection(":memory:")
    collection = conn.test_collection

    # Insert documents with object arrays
    collection.insert_one(
        {
            "_id": 1,
            "author": "Alice",
            "posts": [
                {
                    "title": "Python Performance Tips",
                    "content": "How to optimize Python code for better performance",
                },
                {
                    "title": "Database Design",
                    "content": "Best practices for database design and performance",
                },
            ],
        }
    )

    # Let's see the raw data structure
    cursor = collection.db.execute("SELECT id, data FROM test_collection")
    for row in cursor.fetchall():
        print(f"ID: {row[0]}, Data: {row[1]}")

    # Let's debug what json_each extracts
    print("\n--- Debug json_each ---")
    debug_sql = """
    SELECT json_extract(tc.data, '$.posts') as posts_array,
           je.value as unwound_value,
           json_extract(je.value, '$.content') as content,
           lower(json_extract(je.value, '$.content')) as lower_content
    FROM test_collection tc,
         json_each(json_extract(tc.data, '$.posts')) as je
    """

    try:
        cursor = collection.db.execute(debug_sql)
        results = cursor.fetchall()
        print(f"Debug query found {len(results)} results:")
        for row in results:
            print(f"  Posts array: {row[0]}")
            print(f"  Unwound value: {row[1]}")
            print(f"  Content: {row[2]}")
            print(f"  Lower content: {row[3]}")
    except Exception as e:
        print(f"Debug query failed: {e}")

    # Try manual SQL to see if we can extract and search properly
    print("\n--- Manual SQL Approach ---")
    sql = """
    SELECT tc.id,
           json_set('{}', '$.posts.title', json_extract(je.value, '$.title'),
                   '$.posts.content', json_extract(je.value, '$.content')) as data
    FROM test_collection tc,
         json_each(json_extract(tc.data, '$.posts')) as je
    WHERE lower(json_extract(je.value, '$.content')) LIKE '%performance%'
    """

    try:
        cursor = collection.db.execute(sql)
        results = cursor.fetchall()
        print(f"Manual query found {len(results)} results:")
        for row in results:
            print(f"  ID: {row[0]}, Data: {row[1]}")
            # Try to parse the JSON
            try:
                parsed = json.loads(row[1])
                print(f"    Parsed: {parsed}")
            except:
                print(f"    Could not parse JSON")
    except Exception as e:
        print(f"Manual query failed: {e}")

    # Try a simpler approach - just extract what we need
    print("\n--- Simpler Manual SQL ---")
    sql_simple = """
    SELECT tc.id,
           json_extract(je.value, '$.title') as title,
           json_extract(je.value, '$.content') as content
    FROM test_collection tc,
         json_each(json_extract(tc.data, '$.posts')) as je
    WHERE lower(json_extract(je.value, '$.content')) LIKE '%performance%'
    """

    try:
        cursor = collection.db.execute(sql_simple)
        results = cursor.fetchall()
        print(f"Simple query found {len(results)} results:")
        for row in results:
            print(f"  ID: {row[0]}, Title: {row[1]}, Content: {row[2]}")
    except Exception as e:
        print(f"Simple query failed: {e}")

    # Try to reconstruct the document structure we want for projection
    print("\n--- Reconstruction for Projection ---")
    sql_reconstruct = """
    SELECT tc.id,
           json_set('{}',
                   '$.author', json_extract(tc.data, '$.author'),
                   '$.title', json_extract(je.value, '$.title'),
                   '$.content', json_extract(je.value, '$.content')) as data
    FROM test_collection tc,
         json_each(json_extract(tc.data, '$.posts')) as je
    WHERE lower(json_extract(je.value, '$.content')) LIKE '%performance%'
    """

    try:
        cursor = collection.db.execute(sql_reconstruct)
        results = cursor.fetchall()
        print(f"Reconstruction query found {len(results)} results:")
        for row in results:
            print(f"  ID: {row[0]}")
            try:
                parsed = json.loads(row[1])
                print(f"    Data: {parsed}")
            except Exception as e:
                print(f"    Data: {row[1]} (parse error: {e})")
    except Exception as e:
        print(f"Reconstruction query failed: {e}")


if __name__ == "__main__":
    explore_object_array_solution()
