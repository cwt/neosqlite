# NeoSQLite - NoSQL for SQLite with PyMongo-like API

[![License](https://img.shields.io/pypi/l/neosqlite)](LICENSE)
[![Python Compatibility](https://img.shields.io/pypi/pyversions/neosqlite)](#installation)
[![PyPI Version](https://img.shields.io/pypi/v/neosqlite.svg)](https://pypi.org/project/neosqlite/)
[![Ask DeepWiki](https://img.shields.io/badge/Ask-DeepWiki-blue)](https://deepwiki.com/cwt/neosqlite)<!-- [![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/cwt/neosqlite) -->
[![View Code Wiki](https://assets.codewiki.google/readme-badge/static.svg)](https://codewiki.google/github.com/cwt/neosqlite?utm_source=badge&utm_medium=github&utm_campaign=github.com/cwt/neosqlite)

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
- **Advanced Aggregation**: `$setWindowFields`, `$graphLookup`, `$fill`, streaming `$facet`, `$indexStats`, and more.
- **Tier-1 SQL Optimization**: Dozens of operators translated to native SQL for 10-100x speedup, including BSON-aware `_id` range queries.
- **JavaScript Operators**: `$function` and `$where` run in a sandboxed external Node.js worker — enabled only with a patched, supported Node runtime (see [JavaScript Operators](#javascript-operators)).
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

## Latest Release: v1.18.1

NeoSQLite v1.18.1 is a **cumulative correctness, SQL-tier parity, SQLite 3.26–3.54 compatibility, and runtime modernization release**. It rolls up every change since v1.17.4 and raises the minimum supported Python version to **3.12**.

**Key Highlights:**
- **Cumulative Fixes Since v1.17.4** — Complete kill-switch coverage, Tier-1/Tier-2 SQL parity hardening, expanded Tier-3 promotion, CRUD/write parity, and coverage-driven bug fixes across aggregation, queries, cursors, and writes.
- **SQLite 3.26–3.54 Compatibility & Migration** — Preserves scalar `_id` types via `BLOB` affinity (plus `neosqlite-migration` for legacy `_id TEXT` collections), maintains SQLite 3.26 compatibility (AlmaLinux/RHEL 8), and preserves JSON subtypes on SQLite 3.54+.
- **Python 3.12 Minimum** — Python 3.10 and 3.11 are no longer supported. `nx-27017` v0.9.0 follows the same runtime floor.
- **Python 3.11/3.12 Adoption** — Native `Z` parsing, `datetime.UTC`, `itertools.batched()`, `math.sumprod()`, `asyncio.run(..., loop_factory=...)`, `typing.Self`, `@override`, PEP 695 `type` aliases, PEP 701 f-strings, and `slots=True`.
- **Hardening** — Removed the deprecated `sqlite3.version` report from `serverStatus`, finished fluent-typing coverage, hoisted hot-path imports, and now enforce Python 3.12 Ruff rules.
- **Packaging Fixes** — `pymongo` and `uvloop` are declared by the correct extras, and `pysqlite3-binary` is conditional on Python `< 3.15`.
- **Verification** — 3,122 tests pass (83.64% coverage) on macOS Python 3.14.10 / SQLite 3.54.1, and 3,099 pass with 23 skips (80.98% coverage) on AlmaLinux 8 Python 3.12.15 / SQLite 3.26.0. The NX-27017 suite passes 180 tests with 1 skip on both platforms.

This release is **not backward compatible for Python 3.10/3.11 users**; stay on v1.17.4 for those interpreters. v1.18.0 was tagged but never published on PyPI; v1.18.1 supersedes it. For full details, see [documents/releases/v1.18.1.md](documents/releases/v1.18.1.md). Previous release highlights are in [documents/releases/v1.17.4.md](documents/releases/v1.17.4.md).

## Installation

NeoSQLite supports Python 3.12, 3.13, 3.14, and 3.15.

SQLite 3.26+ is supported. JSONB, aggregate ORDER BY, `RETURNING`, and the
3.35+ math functions are used when available; older SQLite builds fall back to
the Python tier or register compatible SQL functions automatically.

The optional JavaScript operators (`$function`, `$where`) need an external
Node.js executable on `PATH`. **Node v26.13.1 (LTS) is preferred.** See
[JavaScript Operators](#javascript-operators) for the full version gate.

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

> The `jsonb` extra installs `pysqlite3-binary` only on Linux x86_64 with
> Python `< 3.14`, where upstream publishes `manylinux_x86_64` wheels
> (cp312-cp313). On Python 3.14/3.15, macOS, Windows, or ARM the extra
> installs no additional driver; the standard library SQLite is used.

### Migrating legacy `_id TEXT` databases

Databases created by NeoSQLite 1.17.4 and earlier on non-JSONB SQLite store
`_id` values in a `TEXT` column. That loses the distinction between values such
as integer `1` and string `"1"`. The migration command copies the database to a
new file and rebuilds those collection tables with `_id BLOB`:

```bash
neosqlite-migration old.db new.db
```

The source file is never modified. Use `--dry-run` to list affected tables,
and `--overwrite` to replace an existing destination atomically. After the
migration succeeds, point the application at `new.db`. The command preserves
collection indexes and triggers; tables with foreign keys, extra columns, or
non-standard schemas are left untouched.

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
| **Total Tests** | 439 |
| **Passed** | 420 |
| **Skipped** | 19 (architectural differences) |
| **Failed** | 0 |
| **Compatibility** | **100%** |

Skipped tests are due to MongoDB requiring a replica set (change streams, transactions), NeoSQLite extensions (`$log2`, `$contains`), or JavaScript operators when no supported Node.js runtime is installed. All comparable APIs pass. Tested against MongoDB 8.x (`mongo:8.2.12` pinned container image).

Run the comparison yourself: `./scripts/run-api-comparison.sh`

### `_id` Field Behavior

NeoSQLite uses a **strict MongoDB-compatible `_id` column**:

- **Integer `_id` values** are stored and queried in the dedicated `_id` column — not redirected to the auto-increment `id` rowid.
- **String `_id` values** are kept verbatim (e.g., `"5"` is a string, not converted to integer `5`).
- **Auto-generated `_id`** produces MongoDB-compatible 12-byte ObjectIds stored in the `_id` column.
- **Legacy collections** (created before the `_id` column existed) fall back to the `id` column via an automatic resolver.
- **Range queries** (`$gt`, `$lt`, etc.) on `_id` are compiled to native SQL with BSON type guards, so only values of the operand's BSON type match — matching MongoDB semantics without a Python fallback.

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
`$text` (FTS5), `$jsonSchema`, and `$where` (JavaScript, requires Node.js — see below).

### JavaScript Operators

The `$function` aggregation expression and the `$where` query predicate execute
JavaScript in a **sandboxed out-of-process Node.js worker**. No Python binding
or native extension is required; Node.js is discovered on `PATH` at runtime.

**Node v26.13.1 (LTS) is preferred.** Only patched, supported release lines are
accepted:

| Node.js line | Minimum patched version |
|--------------|-------------------------|
| 22.x (Maintenance LTS) | 22.23.5 |
| 24.x (Active LTS) | 24.22.3 |
| **26.x (LTS, preferred)** | **26.13.1** |

EOL lines (18, 20, odd-numbered 21/23/25, and older) and unpatched releases of
active lines are **rejected**. These minimums address CVE-2026-111497
("Node2Shell") and the November 2026 HTTP/2 and TLS security fixes.

> If `node` is missing, end-of-life, or older than the gate, the JavaScript
> operators are **not enabled** — `$function` and `$where` keep raising
> `NotImplementedError` with a message explaining which Node version is
> required. No other functionality is affected.

```python
# Requires a supported Node.js on PATH (e.g. v26.13.1)
docs.aggregate([
    {"$addFields": {
        "total": {
            "$function": {
                "body": "function(a, b) { return a * b; }",
                "args": ["$price", "$qty"],
                "lang": "js",
            }
        }
    }}
])

docs.find({"$where": "this.price > 100"})
```

The worker starts with `node --permission` (file, network, and child-process
access denied) and an **empty environment**, so `process.env` cannot expose
parent-process secrets. Each evaluation is bounded by a 5-second timeout and a
512 MB V8 heap cap on all platforms, with an additional `RLIMIT_AS` cap on
Linux. The timeout is per evaluation, so a query matching many documents can
take proportionally longer. The worker is only used for these two operators;
`$accumulator` and Atlas-only `$script` remain unsupported.

### Aggregation Stages

`$match`, `$project`, `$group`, `$sort`, `$skip`, `$limit`, `$unwind`,
`$lookup`, `$facet`, `$bucket`, `$bucketAuto`, `$sample`, `$merge`,
`$setWindowFields`, `$graphLookup`, `$fill`, `$densify`, `$unionWith`,
`$replaceRoot`, `$replaceWith`, `$unset`, `$count`, `$redact`, `$addFields`, `$switch`,
`$set` (alias for `$addFields`), `$sortByCount`, `$indexStats` (MongoDB-shaped index metadata).

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
