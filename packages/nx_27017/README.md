# NX-27017

**NeoSQLite Experimental Project 27017** - *"To Boldy Go Where No SQLite Has Gone Before!"*

A MongoDB Wire Protocol Server backed by SQLite. In plain English: we turned a simple SQLite file into a MongoDB server. Yes, this is exactly as crazy as it sounds.

## Wait, What?

You know how SQLite is that tiny database that just... works? And you wish you could point your PyMongo app at it without rewriting everything?

That's NX-27017. It speaks MongoDB's wire protocol, stores everything in SQLite, and pretends nothing is wrong.

> **Note:** The `NX` stands for "NeoSQLite Experimental" - our little Starship Enterprise of database adapters. 🚀

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
| `-v` | Verbose logging |

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
| **CRUD** | `insert`, `find`, `update`, `delete` |
| **Aggregation** | `aggregate`, `count`, `distinct` |
| **Collections** | `create`, `drop`, `renameCollection` |
| **Indexes** | `createIndexes`, `listIndexes`, `dropIndexes` |
| **Sessions** | `startSession`, `endSessions` |

## What Doesn't (Yet)

- Full aggregation pipeline (some stages missing)
- Replication & sharding (coming never™)
- Change streams via replica set (we do have SQLite-trigger-based watch though!)

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

> "NX-27017: Not The Final Frontier of SQLite Possibility."*
