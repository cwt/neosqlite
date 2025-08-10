# neosqlite

`neosqlite` (new + nosqlite) is a pure Python library that provides a schemaless, `pymongo`-like wrapper for interacting with SQLite databases. The API is designed to be familiar to those who have worked with `pymongo`, providing a simple and intuitive way to work with document-based data in a relational database.

## Features

- **`pymongo`-like API**: A familiar interface for developers experienced with MongoDB.
- **Schemaless Documents**: Store flexible JSON-like documents.
- **Lazy Cursor**: `find()` returns a memory-efficient cursor for iterating over results.
- **Advanced Indexing**: Supports single-key, compound-key, and nested-key indexes.
- **Modern API**: Aligned with modern `pymongo` practices (using methods like `insert_one`, `update_one`, `delete_many`, etc.).

## Quickstart

Here is a quick example of how to use `neosqlite`:

```python
import neosqlite

# Connect to an in-memory database
with neosqlite.Connection(':memory:') as conn:
    # Get a collection
    users = conn.users

    # Insert a single document
    users.insert_one({'name': 'Alice', 'age': 30})

    # Insert multiple documents
    users.insert_many([
        {'name': 'Bob', 'age': 25},
        {'name': 'Charlie', 'age': 35}
    ])

    # Find a single document
    alice = users.find_one({'name': 'Alice'})
    print(f"Found user: {alice}")

    # Find multiple documents and iterate using the cursor
    print("\nAll users:")
    for user in users.find():
        print(user)

    # Update a document
    users.update_one({'name': 'Alice'}, {'$set': {'age': 31}})
    print(f"\nUpdated Alice's age: {users.find_one({'name': 'Alice'})}")

    # Delete documents
    result = users.delete_many({'age': {'$gt': 30}})
    print(f"\nDeleted {result.deleted_count} users older than 30.")

    # Count remaining documents
    print(f"There are now {users.count_documents({})} users.")
```

## Indexes

Indexes can significantly speed up query performance. `neosqlite` supports single-key, compound-key, and nested-key indexes.

```python
# Create a single-key index
users.create_index('age')

# Create a compound index
users.create_index([('name', neosqlite.ASCENDING), ('age', neosqlite.DESCENDING)])

# Create an index on a nested key
users.insert_one({'name': 'David', 'profile': {'followers': 100}})
users.create_index('profile.followers')
```

Indexes are automatically used by `find()` operations where possible. You can also provide a `hint` to force the use of a specific index.

## Sorting

You can sort the results of a `find()` query by chaining the `sort()` method.

```python
# Sort users by age in descending order
for user in users.find().sort('age', neosqlite.DESCENDING):
    print(user)
```

## Contribution and License

This project was originally developed by Shaun Duncan and is now maintained by Chaiwat Suttipongsakul. It is licensed under the MIT license.

Contributions are highly encouraged. If you find a bug, have an enhancement in mind, or want to suggest a new feature, please feel free to open an issue or submit a pull request.
