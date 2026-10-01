# NX-27017

> "To Boldy Go Where No SQLite Has Gone Before!"

A MongoDB Wire Protocol Server backed by SQLite. In plain English: we turned a simple SQLite file into a MongoDB server. Yes, this is exactly as crazy as it sounds.

## Wait, What?

You know how SQLite is that tiny database that just... works? And you wish you could point your PyMongo app at it without rewriting everything?

That's NX-27017. It speaks MongoDB's wire protocol, stores everything in SQLite, and pretends nothing is wrong.

> **Note:** The `NX` stands for "NeoSQLite Experimental" - our little NX-class starship of database adapters. 🚀

## Requirements

- Python 3.10+
- **pymongo** - Yes, the real pymongo. It includes `bson` so no separate `bson` package needed. *cough* not to be confused with the other `bson` package on PyPi *cough*

```bash
pip install pymongo neosqlite
```

## Quick Start

```bash
# Run with in-memory storage (gone when you stop)
nx-27017 --db memory

# Run with a file (persistent)
nx-27017 --db ./myapp.db

# Run with specific journal mode (WAL is default)
nx-27017 --db ./myapp.db -j DELETE

# Daemon mode
nx-27017 -d --db ./myapp.db
```

## Command Line Options

| Option | Description |
|--------|-------------|
| `--db DB_PATH` | SQLite database (default: nx-27017.db, use `memory` for RAM). A directory enables multi-DB mode |
| `--data-dir DIR` | Multi-database directory: one `<db>.db` file per logical database, created on demand. Takes precedence over `--db` |
| `--single-db-compat` | Force legacy single-file behavior (all logical DBs share one connection) |
| `--host HOST` | Bind address (default: 127.0.0.1) |
| `-p PORT` | Port (default: 27017) |
| `-j MODE` | SQLite journal mode (default: WAL). Modes: WAL, DELETE, TRUNCATE, PERSIST, MEMORY, OFF |
| `-d` | Run as daemon |
| `--stop` | Stop daemon |
| `--status` | Check if running |
| `--fts5-tokenizer NAME=PATH` | Load FTS5 tokenizer (can be repeated for multiple tokenizers) |
| `-v` | Verbose logging |

### Journal Mode

NX-27017 supports configurable SQLite journal modes via `-j` or `--journal-mode`:

```bash
# WAL mode (default) - best concurrency
nx-27017 --db ./myapp.db -j WAL

# DELETE mode - traditional rollback journal
nx-27017 --db ./myapp.db -j DELETE

# MEMORY mode - journal in RAM (fast but no crash recovery)
nx-27017 --db ./myapp.db -j MEMORY
```

For more details on journal modes, see the [NeoSQLite documentation](../../README.md#journal-mode-configuration).

### FTS5 Tokenizer

For databases with FTS5 custom tokenizers (e.g., ICU tokenizer):

```bash
nx-27017 --db myapp.db --fts5-tokenizer icu=/path/to/libfts5_icu.so
# Multiple tokenizers:
nx-27017 --db myapp.db --fts5-tokenizer icu=/path.so --fts5-tokenizer other=/other.so
```

## Try It Out

```bash
# Terminal 1: Start the server
nx-27017 --db memory -v

# Terminal 2: Connect with mongosh
mongosh mongodb://127.0.0.1:27017

# In mongosh:
db.users.insertOne({ name: "Picard", rank: "Captain" })
db.users.insertOne({ name: "Riker", rank: "Commander" })
db.users.find()
```

## What Works

| Category | Commands |
|----------|----------|
| **Handshake** | `ping`, `ismaster`, `hello`, `buildInfo` |
| **CRUD** | `insert`, `find`, `update`, `delete`, `replace_one` with `writeErrors`, `upserted` ids, `arrayFilters` |
| **Bulk** | Collection `bulk_write` and client-level `bulkWrite` (MongoDB 8.0+ verb, wire version 25) |
| **Aggregation** | `aggregate`, `count` (with limit/skip), `distinct` with all common stages including `$collStats` |
| **Cursors** | Real `getMore`/`killCursors` pagination (`batchSize`, 16MB batch cap) |
| **Collections** | `create`, `drop`, `renameCollection`, `dropDatabase`, `listCollections`, `listCollectionNames`, `listDatabases` |
| **Indexes** | `createIndexes`, `listIndexes`, `dropIndexes`, `createSearchIndexes`, `updateSearchIndex`, `dropSearchIndex`, `listSearchIndexes` (FTS-backed) |
| **GridFS** | Full legacy + bucket API over `fs.files`/`fs.chunks` (upload/download/rename/delete/versions) |
| **Sessions** | `startSession`, `endSessions`, `commitTransaction`, `abortTransaction` |
| **Transactions** | Single-database ACID via `with_transaction` / explicit commit/abort (no cross-DB atomicity) |
| **Change Streams** | `$changeStream` with resume tokens, `$match` filtering, `fullDocument` modes, backed by SQLite triggers |
| **Query Features** | `hint`, `min`, `max`, `sort`, `skip`, `limit`, `projection`, `comment`, `collation` (accepted) |
| **Statistics** | `serverStatus`, `dbStats`, `collStats`, `$collStats` aggregation |
| **Durability** | `writeConcern` mapped per database (`w:0`→OFF, `w:1`→NORMAL, `j:true`→FULL) |

### GridFS Support

NX-27017 supports GridFS operations via the MongoDB wire protocol:

```python
from pymongo import MongoClient
from gridfs import GridFS

client = MongoClient('mongodb://localhost:27017/')
db = client.my_database
fs = GridFS(db)

# Upload
file_id = fs.put(b"Hello GridFS!", filename="hello.txt")

# Download
content = fs.get(file_id).read()

# List and delete
for f in fs.find():
    print(f.filename, f.length)
fs.delete(file_id)
```

## Async Usage (PyMongo Async API)

NX-27017 is the async story for NeoSQLite: point `AsyncMongoClient` at it
and every `await` works with no wrapper library.

```python
import asyncio
from pymongo import AsyncMongoClient

async def main():
    client = AsyncMongoClient("mongodb://127.0.0.1:27017/")
    coll = client.mydb.users
    await coll.insert_one({"name": "Picard"})
    async for doc in coll.find({}):
        print(doc)
    async with client.start_session() as session:
        await session.start_transaction()
        await coll.insert_one({"name": "Riker"}, session=session)
        await session.commit_transaction()

asyncio.run(main())
```

Caveats:

- `AsyncMongoClient` is single-event-loop (a PyMongo rule, not NX's).
  Don't share it across threads or loops.
- Transactions are single-database and SQLite-serializable (stronger
  than MongoDB's snapshot isolation, but no cross-DB atomicity).
- `w:majority` / `w:2+` and `wtimeoutMS` are accepted without effect;
  `w:0`, `w:1`, `j:true` tune SQLite durability per database.
- Invalid hints are ignored (real MongoDB errors); non-default-locale
  collation order and `maxTimeMS` enforcement are best-effort.
- Single documents over 16MB are returned whole rather than rejected.
- `OP_QUERY` is a frozen legacy fallback (find-only); all modern
  drivers use `OP_MSG`.

## What Doesn't (Yet)

- Replication & sharding (coming never™ — This is NX-class, not NCC-1701!)
- `find_raw_batches` / `aggregate_raw_batches` (client-side only, no wire verb exists)
- Generic dotted collection names (core SQLite quoting rejects dots;
  only GridFS `fs.files`/`fs.chunks` are mapped)
- Cross-database transactions

## API Compatibility

NX-27017 passes **372 MongoDB API compatibility tests** (358 passed, 14 skipped, 0 failed) when compared against PyMongo's expected behavior. This includes:

- All CRUD operations
- Query operators ($eq, $gt, $gte, $lt, $lte, $ne, $in, $nin, $exists, $type, $all, $size, $regex, $nor, etc.)
- Update operators ($set, $inc, $push, $pull, $addToSet, $pop, etc.)
- Aggregation stages ($match, $group, $sort, $limit, $skip, $project, $unwind, $lookup, $facet, $collStats, etc.)
- Index operations (including text search indexes)
- Cursor methods (hint, min, max, sort)
- Wire protocol message parsing (OP_MSG)
- GridFS operations
- Session management
- Change streams via SQLite-trigger-based `watch()`
- Statistics commands (`serverStatus`, `dbStats`, `collStats`, `$collStats` aggregation)

## Architecture

```text
PyMongo Client ←→ NX-27017 (Wire Protocol) ←→ SQLite (via NeoSQLite)
                      ↓
            "A database inside a database?"
            "It's more like... a database wearing a database costume."
```

## Why Though?

Honestly? Because we could. And because sometimes you want:

- One file = one database
- Zero setup
- A MongoDB-shaped interface to SQLite
- The satisfaction of doing something ridiculous that somehow works
- **Maximum dogfooding**: Testing NX-27017 with real PyMongo, which talks to NX-27017, which uses NeoSQLite, which pretends to be PyMongo, which talks to SQLite. It's dogfood all the way down.

## License

Part of the NeoSQLite project. Use freely, modify liberally, blame no one.

---

> **NX-27017**: Not The Final Frontier of SQLite Possibility.
