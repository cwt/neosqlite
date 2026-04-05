# NeoSQLite - NoSQL for SQLite with PyMongo-like API

[![PyPI Version](https://img.shields.io/pypi/v/neosqlite.svg)](https://pypi.org/project/neosqlite/)

`NeoSQLite` (new + nosqlite) is a pure Python library that provides a schemaless, `PyMongo`-like wrapper for interacting with SQLite databases. The API is designed to be familiar to those who have worked with `PyMongo`, providing a simple and intuitive way to work with document-based data in a relational database.

**Keywords**: NoSQL, NoSQLite, SQLite NoSQL, PyMongo alternative, SQLite document database, Python NoSQL, schemaless SQLite, MongoDB-like SQLite

[![NeoSQLite: SQLite with a MongoDB Disguise](https://img.youtube.com/vi/iZXoEjBaFdU/0.jpg)](https://www.youtube.com/watch?v=iZXoEjBaFdU)

## Features

- **`PyMongo`-like API**: A familiar interface for developers experienced with MongoDB.
- **NX-27017**: [MongoDB Wire Protocol Server](packages/nx_27017/README.md) — Use PyMongo with SQLite backend
- **Schemaless Documents**: Store flexible JSON-like documents.
- **Lazy Cursor**: `find()` returns a memory-efficient cursor for iterating over results.
- **Raw Batch Support**: `find_raw_batches()` returns raw JSON data in batches for efficient processing.
- **Advanced Indexing**: Single-key, compound-key, nested-key indexes, and FTS5 text search.
- **ACID Transactions**: Full `ClientSession` API with PyMongo 4.x parity using SQLite SAVEPOINTs.
- **Change Streams**: Native SQLite triggers for `watch()` — no replica set required.
- **Advanced Aggregation**: `$setWindowFields`, `$graphLookup`, `$fill`, streaming `$facet`, and more.
- **Tier-1 SQL Optimization**: Dozens of operators translated to native SQL for 10-100x speedup.
- **Native `$jsonSchema`**: Query filtering and write-time validation via SQLite CHECK constraints.
- **Window Functions**: Complete MongoDB 5.0+ suite (`$rank`, `$top`, `$bottom`, math operators).
- **MongoDB-compatible ObjectId**: Full 12-byte specification with automatic generation.
- **Full GridFS Support**: Modern `GridFSBucket` API plus legacy `GridFS` compatibility.
- **Binary Data**: PyMongo-compatible `Binary` class with UUID support.
- **AutoVacuum & compact**: Reclaim disk space with incremental or full VACUUM.
- **dbStats Command**: MongoDB-compatible statistics with accurate index sizes.
- **SQL Translation Caching**: 10-30% faster for repeated aggregation pipelines and `$expr` queries.
- **Configurable Journal Mode**: WAL (default), DELETE, MEMORY, and more.
- **Security Hardening**: Built-in SQL injection protection via centralized identifier quoting.

See [CHANGELOG.md](CHANGELOG.md) for the full history.

## Latest Release: v1.14.0

NeoSQLite v1.14.0 is a **major performance and API alignment release** that introduces native SQL-tier support for the `$project` aggregation stage and complex `_id` field operators. It also brings a breaking change to the compound index API to strictly align with PyMongo's `(field, direction)` tuple format.

**Important:** By migrating `$project` and `_id` filtering directly into the SQL layer, this release significantly reduces data transfer overhead and improves performance for large datasets.

### Key Features & Fixes

- **SQL-Tier `$project`**: Native database-level projection support (inclusion, exclusion, aliasing).
- **Advanced `_id` SQL Filtering**: Native SQL support for `$in`, `$nin`, `$ne`, and range queries on `_id`.
- **FTS Extension**: Native SQL `$text` search on unwound data (after `$unwind`) via recursive `json_tree` extraction.
- **API Alignment**: `create_index` now strictly requires the PyMongo tuple format for compound indexes.
- **Parity Verified**: 100% bit-for-bit result parity between SQL and Python tiers.

For full details, see [documents/releases/v1.14.0.md](documents/releases/v1.14.0.md).

## Installation

```bash
pip install neosqlite
```

### Optional Extras

```bash
# Enhanced JSON/JSONB support (only needed if your SQLite lacks JSON functions)
pip install neosqlite[jsonb]

# Memory-constrained processing for large result sets
pip install neosqlite[memory-constrained]

# NX-27017 MongoDB Wire Protocol Server
pip install "neosqlite[nx27017]"          # Core
pip install "neosqlite[nx27017-speed]"    # With uvloop (Linux/macOS)
```

## Quickstart

```python
import neosqlite

with neosqlite.Connection(':memory:') as conn:
    users = conn.users

    # Insert
    users.insert_one({'name': 'Alice', 'age': 30})
    users.insert_many([
        {'name': 'Bob', 'age': 25},
        {'name': 'Charlie', 'age': 35}
    ])

    # Find
    alice = users.find_one({'name': 'Alice'})
    for user in users.find():
        print(user)

    # Update
    users.update_one({'name': 'Alice'}, {'$set': {'age': 31}})

    # Delete & Count
    result = users.delete_many({'age': {'$gt': 30}})
    print(f"Remaining: {users.count_documents({})}")
```

## Drop-in Replacement for PyMongo

### 1. Direct API (No MongoDB)

```python
import neosqlite
client = neosqlite.Connection('mydatabase.db')
collection = client.mycollection
collection.insert_one({"name": "test"})
```

### 2. Wire Protocol (NX-27017) — Zero Code Changes

```bash
# Start server
nx-27017 --db ./myapp.db
```

```python
# Then use PyMongo normally — no code changes!
from pymongo import MongoClient
client = MongoClient('mongodb://localhost:27017/')
collection = client.mydatabase.mycollection
collection.insert_one({"name": "test"})  # Works!
```

## PyMongo Compatibility

| Metric | Result |
|--------|--------|
| **Total Tests** | 386 |
| **Passed** | 368 |
| **Skipped** | 18 (architectural differences) |
| **Failed** | 0 |
| **Compatibility** | **100%** |

Skipped tests are due to MongoDB requiring a replica set (change streams, transactions) or NeoSQLite extensions (`$log2`, `$contains`). All comparable APIs pass.

Run the comparison yourself: `./scripts/run-api-comparison.sh`

## Key APIs

### Indexes

```python
# Single-key, compound, nested
users.create_index('age')
users.create_index([('name', neosqlite.ASCENDING), ('age', neosqlite.DESCENDING)])
users.create_index('profile.followers')

# FTS5 text search
users.create_search_index('bio')
```

### Query Operators

`$eq`, `$gt`, `$gte`, `$lt`, `$lte`, `$ne`, `$in`, `$nin`, `$and`, `$or`, `$not`, `$nor`,
`$exists`, `$type`, `$regex`, `$elemMatch`, `$size`, `$mod`,
`$bitsAllSet`, `$bitsAllClear`, `$bitsAnySet`, `$bitsAnyClear`,
`$text` (FTS5), `$jsonSchema`, and more.

### Aggregation Stages

`$match`, `$project`, `$group`, `$sort`, `$skip`, `$limit`, `$unwind`,
`$lookup`, `$facet`, `$bucket`, `$bucketAuto`, `$sample`, `$merge`,
`$setWindowFields`, `$graphLookup`, `$fill`, `$densify`, `$unionWith`,
`$replaceRoot`, `$replaceWith`, `$unset`, `$count`, `$redact`, `$addFields`, `$switch`.

### Transactions

```python
with client.start_session() as session:
    with session.start_transaction():
        users.insert_one({"name": "Alice"}, session=session)
        orders.insert_one({"user": "Alice"}, session=session)
    # Commits on success, rolls back on exception
```

### Change Streams

```python
# Native SQLite triggers — no replica set needed
stream = collection.watch()
for change in stream:
    print(change)
```

### Journal Mode

```python
from neosqlite import Connection, JournalMode

db = Connection("app.db", journal_mode=JournalMode.WAL)  # Default
```

| Mode | Use Case |
|------|----------|
| **WAL** | Best concurrency (default) |
| **DELETE** | Single-file distribution |
| **MEMORY** | Maximum speed, no crash recovery |

## Documentation

| Topic | Link |
|-------|------|
| Release Notes | [documents/releases/](documents/releases/) |
| Changelog | [CHANGELOG.md](CHANGELOG.md) |
| GridFS | [documents/GRIDFS.md](documents/GRIDFS.md) |
| Text Search | [documents/TEXT_SEARCH.md](documents/TEXT_SEARCH.md) |
| Aggregation Optimization | [documents/AGGREGATION_PIPELINE_OPTIMIZATION.md](documents/AGGREGATION_PIPELINE_OPTIMIZATION.md) |
| NX-27017 Server | [packages/nx_27017/README.md](packages/nx_27017/README.md) |
| API Comparison | [examples/api_comparison/README.md](examples/api_comparison/README.md) |

## Contribution and License

This project was originally developed as [shaunduncan/nosqlite](https://github.com/shaunduncan/nosqlite) and was later forked as [plutec/nosqlite](https://github.com/plutec/nosqlite) before becoming NeoSQLite. It is now maintained by Chaiwat Suttipongsakul and is licensed under the MIT license.

Contributions are highly encouraged. If you find a bug, have an enhancement in mind, or want to suggest a new feature, please feel free to open an issue or submit a pull request.
