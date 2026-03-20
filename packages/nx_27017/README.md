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

# Daemon mode
nx-27017 -d --db ./myapp.db
```

## Command Line Options

| Option | Description |
|--------|-------------|
| `--db DB_PATH` | SQLite database (default: nx-27017.db, use `memory` for RAM) |
| `--host HOST` | Bind address (default: 127.0.0.1) |
| `-p PORT` | Port (default: 27017) |
| `-d` | Run as daemon |
| `--stop` | Stop daemon |
| `--status` | Check if running |
| `--fts5-tokenizer NAME=PATH` | Load FTS5 tokenizer (can be repeated for multiple tokenizers) |
| `-v` | Verbose logging |

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
| **CRUD** | `insert`, `find`, `update`, `delete`, `replace_one` |
| **Aggregation** | `aggregate`, `count`, `distinct` with all common stages |
| **Collections** | `create`, `drop`, `renameCollection`, `listCollections`, `listCollectionNames` |
| **Indexes** | `createIndexes`, `listIndexes`, `dropIndexes`, `listSearchIndexes` |
| **Sessions** | `startSession`, `endSessions` |
| **Query Features** | `hint`, `min`, `max`, `sort`, `skip`, `limit`, `projection` |

## What Doesn't (Yet)

- Replication & sharding (coming never™)
- Change streams via replica set (Note: SQLite-trigger-based `watch()` is supported!)
- GridFS (MongoDB-specific file storage)
- Transactions (requires replica set)
- `find_raw_batches` with batch_size (requires cursor state management)

## API Compatibility

NX-27017 passes the **376 MongoDB API compatibility tests** (359 passed, 17 skipped) when compared against PyMongo's expected behavior. This includes:

- All CRUD operations
- Query operators ($eq, $gt, $gte, $lt, $lte, $ne, $in, $nin, $exists, $type, $all, $size, $regex, $nor, etc.)
- Update operators ($set, $inc, $push, $pull, $addToSet, $pop, etc.)
- Aggregation stages ($match, $group, $sort, $limit, $skip, $project, $unwind, $lookup, $facet, etc.)
- Index operations (including text search indexes)
- Cursor methods (hint, min, max, sort)

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

## License

Part of the NeoSQLite project. Use freely, modify liberally, blame no one.

---

> **NX-27017**: Not The Final Frontier of SQLite Possibility.
