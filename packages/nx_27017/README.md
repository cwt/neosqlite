# NX-27017

**NeoSQLite Experimental Project 27017** - A MongoDB Wire Protocol Server backed by SQLite.

## Overview

NX-27017 allows MongoDB clients to connect and perform operations while data is actually stored in SQLite databases. This provides MongoDB protocol compatibility for applications while leveraging SQLite's simplicity and portability.

## Features

- **MongoDB Wire Protocol Compatible**: Supports OP_MSG and OP_QUERY opcodes
- **SQLite Backend**: All data stored in SQLite databases
- **Daemon Mode**: Run as a background service with `-d`
- **Multi-Database Support**: Automatic database creation per MongoDB database
- **Common Commands**: Supports ping, isMaster, hello, insert, find, update, delete, aggregate, and more

## Installation

NX-27017 requires Python 3.10+ and the following dependencies:

- `neosqlite` - SQLite wrapper with MongoDB-like API
- `pymongo` / `bson` - BSON encoding/decoding

## Usage

### Run in Foreground

```bash
# Default (uses nx-27017.db)
python nx_27017.py

# In-memory database
python nx_27017.py --db memory

# Custom database file
python nx_27017.py --db /path/to/database.db

# Custom host and port
python nx_27017.py --host 0.0.0.0 -p 27018
```

### Run as Daemon

```bash
# Start daemon
python nx_27017.py -d

# Start daemon with custom database
python nx_27017.py -d --db /data/mongo.db

# Check status
python nx_27017.py --status

# Stop daemon
python nx_27017.py --stop

# Verbose logging
python nx_27017.py -d --verbose
```

### Command Line Options

| Option | Description |
|--------|-------------|
| `--db DB_PATH` | SQLite database path (default: nx-27017.db, use 'memory' for in-memory) |
| `--host HOST` | Host to bind to (default: 127.0.0.1) |
| `-p, --port PORT` | Port to listen on (default: 27017) |
| `-d, --daemon` | Run as a background daemon |
| `--stop` | Stop the running daemon |
| `--status` | Check if daemon is running |
| `--log-file LOG_FILE` | Log file path (default: /tmp/nx_27017.log) |
| `--pid-file PID_FILE` | PID file path (default: /tmp/nx_27017.pid) |
| `-v, --verbose` | Enable debug logging |

## Connecting with MongoDB Shell

```bash
# Connect with mongosh
mongosh mongodb://127.0.0.1:27017

# Or with legacy mongo shell
mongo mongodb://127.0.0.1:27017
```

## Supported MongoDB Commands

### Handshake & Discovery
- `ping`
- `ismaster` / `isMaster`
- `hello`
- `buildInfo` / `buildinfo`
- `serverStatus`
- `whatsmyuri`
- `dbStats` / `dbstats`
- `collStats` / `collstats`

### CRUD Operations
- `insert` - Insert documents
- `find` - Query documents (with filter, sort, limit, skip)
- `update` - Update documents (with upsert support)
- `delete` - Delete documents

### Aggregation
- `aggregate` - Aggregation pipeline (limited stage support)
- `count` - Count documents
- `distinct` - Get distinct values

### Schema Management
- `create` - Create collection
- `drop` - Drop collection

### Session Management
- `endSessions`

## Example Usage

```javascript
// Connect and use with any MongoDB client
use testdb;

// Insert documents
db.users.insertOne({ name: "Alice", age: 30 });
db.users.insertMany([
  { name: "Bob", age: 25 },
  { name: "Charlie", age: 35 }
]);

// Query documents
db.users.find({ age: { $gt: 28 } });

// Update documents
db.users.updateOne(
  { name: "Alice" },
  { $set: { age: 31 } }
);

// Aggregate
db.users.aggregate([
  { $match: { age: { $gte: 30 } } },
  { $count: "total" }
]);
```

## Architecture

```
MongoDB Client → Wire Protocol (TCP:27017) → NeoSQLiteHandler → SQLite (via neosqlite)
```

## Limitations

- Not all MongoDB commands are supported
- Aggregation pipeline has limited stage support
- No replication or sharding support
- Wire version: 17-21 (MongoDB 6.x compatible)

## Files

- `nx_27017.py` - Main server implementation
- `tests/` - Unit tests
- `README.md` - This file

## License

Part of the NeoSQLite project. See the main project LICENSE for details.

## Acknowledgments

- Built on top of [NeoSQLite](https://github.com/neosqlite/neosqlite)
- MongoDB wire protocol implementation inspired by official MongoDB documentation
