# NX-27017 Architecture

## Overview

NX-27017 is a **MongoDB Wire Protocol Server** backed by SQLite. It allows any
MongoDB client (PyMongo, mongosh, Node.js MongoDB driver, etc.) to connect and
interact with SQLite as if it were a real MongoDB server—no code changes
required.

## The Big Picture

### Background: NeoSQLite

The foundation of this project is **NeoSQLite**, which provides a
PyMongo-compatible API that operates on SQLite instead of MongoDB:

```text
Scenario A: Traditional MongoDB Stack
[MongoDB Python App] → [PyMongo] → (Wire Protocol) → [MongoDB Server]

Scenario B: NeoSQLite (Direct Python API)
[MongoDB Python App] → [NeoSQLite] → (Python Calls) → [SQLite]
```

NeoSQLite is a drop-in replacement for PyMongo. It's proven to work 100% via
NeoSQLite's functional test suite.

### NX-27017: The Wire Protocol Server

NX-27017 takes the next logical step: reverse engineering the MongoDB wire
protocol. Instead of just providing a PyMongo-compatible API, NX-27017:

1. Listens on port 27017 (MongoDB's default port)
2. Accepts raw MongoDB wire protocol messages
3. Translates them to NeoSQLite/Python calls
4. Converts NeoSQLite results back to wire protocol responses
5. Sends responses back to the client

```text
Scenario C: NX-27017 (Wire Protocol Server)
[MongoDB Client]→[PyMongo]→(Wire Protocol)→[NX-27017]→[NeoSQLite]→[SQLite]
                                          ↑
                         "A virtual MongoDB running on SQLite"
```

## Architecture Layers

```text
┌─────────────────────────────────────────────────────────────────┐
│                    MongoDB Client (PyMongo, mongosh, etc.)      │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼ (Wire Protocol over TCP)
┌─────────────────────────────────────────────────────────────────┐
│                      NX-27017 Server                            │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │              Wire Protocol Handler                        │  │
│  │  - Parse incoming BSON messages                           │  │
│  │  - Route to appropriate command handler                   │  │
│  │  - Serialize responses back to wire protocol format       │  │
│  └───────────────────────────────────────────────────────────┘  │
│                              │                                  │
│                              ▼                                  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │              Command Handlers                             │  │
│  │  - Handshake: ping, ismaster, hello, buildInfo            │  │
│  │  - CRUD: insert, find, update, delete                     │  │
│  │  - Aggregation: aggregate, count, distinct                │  │
│  │  - Collections: create, drop, renameCollection            │  │
│  │  - Indexes: createIndexes, listIndexes, dropIndexes       │  │
│  │  - Sessions: startSession, endSessions                    │  │
│  └───────────────────────────────────────────────────────────┘  │
│                              │                                  │
│                              ▼                                  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │              NeoSQLite Adapter                            │  │
│  │  - Translates MongoDB operations to SQLite                │  │
│  │  - Handles BSON ↔ SQLite type conversions                 │  │
│  │  - Manages collections, indexes, and queries              │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                         SQLite Database                         │
│  - In-memory (:memory:) or file-based (myapp.db)                │
│  - Full SQL support including FTS5 with custom tokenizers       │
└─────────────────────────────────────────────────────────────────┘
```

## Data Flow

### 1. Client Connection

```text
Client (PyMongo/mongosh)
    │
    │ TCP connection to localhost:27017
    ▼
NX-27017 Server
    │
    │ Accepts connection, creates session
    ▼
Ready for wire protocol messages
```

### 2. Command Execution (e.g., `insertOne`)

```text
Client sends: OP_MSG with insert command
    │
    ▼
Wire Protocol Handler
    │ Parse BSON message
    │ Extract: namespace, documents, options
    ▼
Command Handler (insert)
    │ Validate parameters
    │ Prepare operation
    ▼
NeoSQLite Adapter
    │ Convert BSON document to SQLite row
    │ Execute: INSERT INTO collection_name ...
    ▼
SQLite Database
    │ Store data
    │ Return result
    ▼
NeoSQLite Adapter
    │ Convert SQLite result to MongoDB format
    │ Build write concern response
    ▼
Wire Protocol Handler
    │ Serialize response to BSON
    │ Wrap in OP_MSG format
    ▼
Client receives: { ok: 1, nInserted: 1, _id: ObjectId(...) }
```

### 3. Query Execution (e.g., `find`)

```text
Client sends: OP_MSG with find command + query filter
    │
    ▼
Wire Protocol Handler
    │ Parse BSON, extract query parameters
    ▼
Command Handler (find)
    │ Build query plan
    │ Apply filters, projections, sort
    ▼
NeoSQLite Adapter
    │ Translate MongoDB query to SQL
    │ Execute: SELECT ... FROM collection_name WHERE ...
    ▼
SQLite Database
    │ Return matching rows
    ▼
NeoSQLite Adapter
    │ Convert SQLite rows to BSON documents
    │ Handle cursors for large result sets
    ▼
Wire Protocol Handler
    │ Stream results via OP_MSG or OP_REPLY
    ▼
Client receives: Cursor with matching documents
```

## See Also

- [README.md](README.md)
  Quick start guide and usage examples
- [NeoSQLite Documentation](https://neosqlite.readthedocs.io/en/latest/modules.html)
  Core library docs
- [MongoDB Wire Protocol](https://www.mongodb.com/docs/manual/reference/mongodb-wire-protocol/)
  Official protocol specification
