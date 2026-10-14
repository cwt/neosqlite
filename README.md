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
- **TTL Indexes**: `expireAfterSeconds` expiry with lazy auto-purge and an opt-in background sweeper.
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

See [Release Notes](documents/releases/) for the full history.

## Latest Release: v1.17.2

NeoSQLite v1.17.2 is a **correctness and PyMongo API parity release** resolving gaps surfaced during real-world dogfooding (e.g. against blogging platforms like `neo-blogger`). It introduces native SQLite FTS5 BM25 text relevance scoring and sorting in `find()`, PyMongo-compatible multi-field text index creation syntax, fluent chaining methods on `GridOutCursor`, direct `Connection` instantiation for `GridFSBucket`, safe collection counting on GridFS system tables, and expanded unit test coverage reaching **81.18%**. Fully backward compatible.

**Key Highlights:**
- **FTS5 BM25 Text Relevance Scoring & Sorting** — `find({"$text": {"$search": ...}})` computes true BM25 relevance scores across indexed fields, supports score projection via `{"score": {"$meta": "textScore"}}`, and enables sorting by `[("score", {"$meta": "textScore"})]`.
- **MongoDB-Style Multi-Field Text Index Definitions** — `collection.create_index([("field1", "text"), ("field2", "text")])` creates FTS virtual tables across all specified text fields in a single call, matching PyMongo syntax.
- **Fluent `GridOutCursor` Operations** — Added `.sort()`, `.skip()`, `.limit()`, `.batch_size()`, `.to_list()`, `.clone()`, and `.rewind()` to `GridOutCursor` for idiomatic cursor chaining on GridFS file searches.
- **Direct Connection Support in `GridFSBucket`** — `GridFSBucket(conn)` accepts a `neosqlite.Connection` directly (automatically unwrapping the underlying SQLite database handle) alongside raw database connections (`GridFSBucket(conn.db)`).
- **Safe Document Counting on GridFS System Collections** — Dotted collection access like `conn.fs.files.count_documents()` maps directly onto relational schema columns without raising `no such column: data`.
- **Robust Quality Gates & Differential Testing** — Achieved 100.0% compatibility across 404 comparison cases against MongoDB 8.2.12, passed 3,034 pytest unit tests with **81.18% code coverage**, and verified zero mypy/lint warnings.

For full details, see [documents/releases/v1.17.2.md](documents/releases/v1.17.2.md). Previous release highlights are in [documents/releases/v1.17.1.md](documents/releases/v1.17.1.md).

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
| **Total Tests** | 381 |
| **Passed** | 363 |
| **Skipped** | 18 (architectural differences) |
| **Failed** | 0 |
| **Compatibility** | **100%** |

Skipped tests are due to MongoDB requiring a replica set (change streams, transactions) or NeoSQLite extensions (`$log2`, `$contains`). All comparable APIs pass. Tested against MongoDB 8.x (`mongo:8.2.12` pinned container image).

Run the comparison yourself: `./scripts/run-api-comparison.sh`

### `_id` Field Behavior

NeoSQLite uses a **strict MongoDB-compatible `_id` column**:

- **Integer `_id` values** are stored and queried in the dedicated `_id` column — not redirected to the auto-increment `id` rowid.
- **String `_id` values** are kept verbatim (e.g., `"5"` is a string, not converted to integer `5`).
- **Auto-generated `_id`** produces MongoDB-compatible 12-byte ObjectIds stored in the `_id` column.
- **Legacy collections** (created before the `_id` column existed) fall back to the `id` column via an automatic resolver.
- **Range queries** (`$gt`, `$lt`, etc.) on `_id` use Python evaluation to preserve correct cross-type comparison semantics (MongoDB BSON ordering).

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
| Knowledge Base / Docs | [documents/](documents/) |
| Release Notes | [documents/releases/](documents/releases/) |
| GridFS | [documents/gridfs.md](documents/gridfs.md) |
| Text Search | [documents/text-search.md](documents/text-search.md) |
| Aggregation Optimization | [documents/aggregation-pipeline-optimization.md](documents/aggregation-pipeline-optimization.md) |
| NX-27017 Server | [packages/nx_27017/README.md](packages/nx_27017/README.md) |
| API Comparison | [examples/api_comparison/README.md](examples/api_comparison/README.md) |

## Contributing

Clone the repository:

```bash
git clone https://github.com/cwt/neosqlite.git
cd neosqlite
```

Create and activate a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Then run the test script, which installs all required dependencies automatically:

```bash
./scripts/runtest.sh
```

### Shell Script Compatibility

All shell scripts in this project target **bash 3.2+** for compatibility with macOS, which still ships with bash 3.2.x. Please ensure any contributions to shell scripts remain compatible.

## Contribution and License

This project was originally developed as [shaunduncan/nosqlite](https://github.com/shaunduncan/nosqlite) and was later forked as [plutec/nosqlite](https://github.com/plutec/nosqlite) before becoming NeoSQLite. It is now maintained by Chaiwat Suttipongsakul and is licensed under the MIT license.

Contributions are highly encouraged. If you find a bug, have an enhancement in mind, or want to suggest a new feature, please feel free to open an issue or submit a pull request.
