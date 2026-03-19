# AutoVacuum and Database Maintenance Guide

> **NeoSQLite 1.11+**

---

## Table of Contents

- [AutoVacuum Basics](#autovacuum-basics)
- [AutoVacuum Modes Explained](#autovacuum-modes-explained)
- [Using AutoVacuum in NeoSQLite](#using-autovacuum-in-neosqlite)
- [Migration: Changing AutoVacuum on Existing Databases](#migration-changing-autovacuum-on-existing-databases)
- [Manual Vacuum Operations](#manual-vacuum-operations)
- [MongoDB compact Command](#mongodb-compact-command)
- [freeSpaceTargetMB Explained](#freespacetargetmb-explained)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)

---

## AutoVacuum Basics

**AutoVacuum** is a SQLite setting that controls how the database reclaims disk space after you delete data.

Think of it like cleaning up after a party:

| Mode | Analogy |
|------|---------|
| **NONE** | You never clean up. Empty bottles and plates pile up. The room stays big forever. |
| **FULL** | You clean up immediately after every guest leaves. Tidy, but you're constantly cleaning. |
| **INCREMENTAL** | You note where the mess is, and clean up when it makes sense. Balanced approach. |

When you delete documents from a NeoSQLite collection, the underlying SQLite database has free space. AutoVacuum determines what happens to that space.

---

## Why Should You Care?

### Disk Space

Without auto_vacuum (`NONE` mode), your database file **never shrinks**. Delete 10 GB of data? The file stays 10 GB larger, even though the space is "free" inside.

### Performance

- **FULL** mode can slow down delete operations because SQLite cleans up after each deletion.
- **INCREMENTAL** mode tracks freed space and vacuums efficiently when needed.
- **NONE** mode has fast deletes but may waste disk space.

### Compatibility

If you're opening an existing SQLite database that was created with different settings, NeoSQLite needs to handle the mismatch gracefully. That's where **migration** comes in.

---

## AutoVacuum Modes Explained

| Mode | Value | Behavior | Best For |
|------|-------|----------|----------|
| **NONE** | `0` | Never reclaim space automatically. Database file never shrinks. | Read-only databases, temporary databases |
| **FULL** | `1` | Automatically vacuum after deletions. Reclaims space immediately. | Databases with frequent deletes where disk space is critical |
| **INCREMENTAL** | `2` | Track freed pages, vacuum on demand. Balanced approach. | **Most applications** (this is the NeoSQLite default) |

### NONE Mode

```python
conn = neosqlite.Connection("mydb.db", auto_vacuum=neosqlite.AutoVacuumMode.NONE)
```

- ✅ Fastest write operations
- ✅ No vacuum overhead
- ❌ Database file never shrinks
- ❌ Can waste significant disk space over time

### FULL Mode

```python
conn = neosqlite.Connection("mydb.db", auto_vacuum=neosqlite.AutoVacuumMode.FULL)
```

- ✅ Always optimal disk space usage
- ✅ Database file shrinks automatically
- ❌ Slower delete operations
- ❌ More I/O overhead

### INCREMENTAL Mode (Default)

```python
# This is the default - you don't need to specify it
conn = neosqlite.Connection("mydb.db")
```

- ✅ Good balance of performance and space
- ✅ Vacuum only when beneficial
- ✅ NeoSQLite default for new databases
- ⚠️ Slightly more complex internal tracking

---

## Using AutoVacuum in NeoSQLite

### Creating a New Database

```python
import neosqlite

# Default: INCREMENTAL mode
conn = neosqlite.Connection("myapp.db")

# Explicitly set mode using integer
conn = neosqlite.Connection("myapp.db", auto_vacuum=2)  # INCREMENTAL

# Explicitly set mode using string
conn = neosqlite.Connection("myapp.db", auto_vacuum="INCREMENTAL")

# Use the AutoVacuumMode class (recommended)
conn = neosqlite.Connection(
    "myapp.db",
    auto_vacuum=neosqlite.AutoVacuumMode.INCREMENTAL
)
```

### Available Modes

```python
from neosqlite import AutoVacuumMode

AutoVacuumMode.NONE        # 0
AutoVacuumMode.FULL        # 1
AutoVacuumMode.INCREMENTAL # 2 (default)
```

### In-Memory Databases

In-memory databases (`:memory:`) don't need auto_vacuum configuration:

```python
# AutoVacuum setting applies but no migration occurs
conn = neosqlite.Connection(":memory:", auto_vacuum=neosqlite.AutoVacuumMode.FULL)
```

---

## Migration: Changing AutoVacuum on Existing Databases

### The Problem

SQLite requires `auto_vacuum` to be set **before any tables are created**. If you try to change it on an existing database with:

```sql
PRAGMA auto_vacuum = 1;
```

**Nothing happens.** The setting is ignored.

To actually change auto_vacuum on an existing database, you must:

1. Export all data
2. Create a new database with the correct setting
3. Import all data
4. Replace the old database

NeoSQLite automates this process with **safe migration**.

---

### How Migration Works

When you open an existing database with a different auto_vacuum setting:

```text
┌─────────────────────────────────────────────────────────────┐
│  1. Check if current auto_vacuum matches requested mode     │
│     └─ If same: Skip migration (no changes made)            │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  2. Checkpoint WAL files (ensure all data is in main file)  │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  3. Create timestamped backups of:                          │
│     - database.db                                           │
│     - database.db-wal (if exists)                           │
│     - database.db-shm (if exists)                           │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  4. Open backup, set new auto_vacuum, VACUUM INTO new file   │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  5. Replace original database with new vacuumed file        │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  6. Clean up WAL files and backups                          │
│     ✅ Migration complete!                                  │
└─────────────────────────────────────────────────────────────┘
```

### Safety Features

- **Opt-in**: Migration only runs when you enable it (see below)
- **No-op when matching**: If modes already match, file is never touched
- **Full backup**: All related files backed up before changes
- **Atomic replacement**: Uses temp file + move operation
- **Rollback on error**: If anything fails, original database is restored

---

### Enabling Migration

Migration is **disabled by default** to prevent unexpected behavior. Enable it with an environment variable:

```bash
export AUTOVACUUM_MIGRATION=1
python your_app.py
```

Or in Python before opening the connection:

```python
import os
os.environ["AUTOVACUUM_MIGRATION"] = "1"

import neosqlite
conn = neosqlite.Connection("existing.db", auto_vacuum=neosqlite.AutoVacuumMode.FULL)
```

### Migration Behavior

| Scenario | Migration Enabled | Result |
|----------|------------------|--------|
| New database | Any | Created with requested mode (no migration needed) |
| Existing DB, same mode | Yes/No | No migration (modes already match) |
| Existing DB, different mode | **No** (default) | Opens with **current** mode (requested mode ignored) |
| Existing DB, different mode | **Yes** | Migrates to requested mode |

---

## Manual Vacuum Operations

### Full VACUUM

If you're using NONE mode and need to reclaim disk space, you can manually trigger a full vacuum:

```python
# Using db.command() (PyMongo-compatible)
result = conn.command("vacuum")
print(result)  # {'ok': 1, 'message': 'VACUUM completed'}

# Or using raw SQLite
conn.db.execute("VACUUM")
```

This rewrites the entire database to a new file, copies data back, and deletes the old one. Can temporarily require 2x disk space.

---

### Incremental Vacuum

SQLite's **incremental vacuum** allows reclaiming free pages in small chunks rather than one large operation. This is useful when:

- You want to vacuum in batches (scheduled maintenance)
- You want to avoid long database locks
- You don't want temporary 2x disk space requirement

```python
# Vacuum 100 pages at a time
conn.db.execute("PRAGMA incremental_vacuum(100)")
conn.db.commit()
```

How it works:
- **Full VACUUM**: Rewrites entire DB, requires 2x space, all-or-nothing
- **Incremental vacuum(N)**: Reclaims N pages at a time, constant memory, can be done in batches

> **Note:** SQLite's incremental vacuum can only reclaim pages from the *end* of the database file. It doesn't compact fragmented free space in the middle. For complete reclamation, use full VACUUM.

---

## MongoDB compact Command

NeoSQLite implements the MongoDB `compact` command for API compatibility:

```python
# Compact without options (MongoDB default behavior)
result = conn.command("compact", "collection_name")
# Returns: {'bytesFreed': 12345, 'ok': 1}

# Dry run - estimate without actually compacting
result = conn.command("compact", "collection_name", dryRun=True)
# Returns: {'estimatedBytesFreed': 12345, 'ok': 1}

# With threshold (NeoSQLite extension)
result = conn.command("compact", "collection_name", freeSpaceTargetMB=1)
# Only runs if free space >= 1MB
```

### MongoDB → SQLite Mapping

| MongoDB Option | NeoSQLite Behavior |
|----------------|-------------------|
| `compact` (collection name) | Ignored - SQLite operates on entire database |
| `dryRun` | Returns estimate without running |
| `force` | Ignored - always available in SQLite |
| `freeSpaceTargetMB` | See section below |
| `comment` | Ignored |

### Output Format

```python
# Full compact
{'bytesFreed': 27859, 'ok': 1}

# Dry run
{'estimatedBytesFreed': 27859, 'ok': 1}
```

---

## freeSpaceTargetMB Explained

NeoSQLite extends MongoDB's `freeSpaceTargetMB` parameter to serve **two purposes**:

### 1. Threshold (MongoDB behavior)

Only run compaction if free space >= `freeSpaceTargetMB`:

```python
# Only compact if free space >= 20MB (MongoDB default)
result = conn.command("compact", "collection", freeSpaceTargetMB=20)
# If free space < 20MB: {'bytesFreed': 0, 'ok': 1}
```

### 2. Batch Size (NeoSQLite extension)

When running incremental vacuum, use this as the batch size:

```python
# freeSpaceTargetMB=1 means:
#   - Threshold: only run if free >= 1MB
#   - Batch: vacuum 1MB worth of pages per iteration

result = conn.command("compact", "collection", freeSpaceTargetMB=1)
# Internally: loops incremental_vacuum(256) until all free pages reclaimed
```

### Behavior Matrix

| Scenario | Behavior |
|----------|----------|
| No `freeSpaceTargetMB` | Full VACUUM (all-or-nothing, like MongoDB) |
| `freeSpaceTargetMB=20` (default) | Only runs if free >= 20MB |
| `freeSpaceTargetMB=1` | Runs if free >= 1MB, uses incremental vacuum in 1MB batches |
| `freeSpaceTargetMB=0` | Always runs (threshold is 0) |

### Why Extend?

- **MongoDB compat**: Default 20MB threshold matches MongoDB behavior
- **Flexibility**: Users can choose threshold + batch size in one parameter
- **Performance**: Incremental vacuum avoids long locks and 2x disk space

---

## Best Practices

### For New Applications

**Use the default INCREMENTAL mode:**

```python
conn = neosqlite.Connection("myapp.db")
```

This provides the best balance for most workloads.

### For Existing Databases

**If your database already has data:**

1. **Don't change auto_vacuum unless necessary**. If it's working, leave it.
2. **Test migration on a copy first:**

   ```bash
   cp production.db test_migration.db
   export AUTOVACUUM_MIGRATION=1
   python test_with_migration.py
   ```

3. **Schedule migration during maintenance windows** for large databases.

### For Scheduled Maintenance

Use `compact` with `freeSpaceTargetMB` for periodic maintenance:

```python
# Daily maintenance - compact if at least 100MB can be reclaimed
conn.command("compact", "my_collection", freeSpaceTargetMB=100)

# Or with smaller batches for more control
conn.command("compact", "my_collection", freeSpaceTargetMB=10)
```

### For Manual Vacuum Control

If you prefer manual control over automatic vacuum (using NONE mode):

```python
# Connect with NONE mode
conn = neosqlite.Connection("myapp.db", auto_vacuum=neosqlite.AutoVacuumMode.NONE)

# Manual vacuum when needed (e.g., during maintenance)
conn.command("vacuum")
# Or: conn.db.execute("VACUUM")
```

This approach gives you:
- Maximum write performance (no automatic vacuum overhead)
- Full control over when vacuuming happens
- Predictable I/O patterns (vacuum during scheduled maintenance)

### Monitoring

Check your current auto_vacuum mode:

```python
conn = neosqlite.Connection("myapp.db")
mode_value = conn.db.execute("PRAGMA auto_vacuum").fetchone()[0]
mode_name = neosqlite.AutoVacuumMode.to_string(mode_value)
print(f"AutoVacuum mode: {mode_name}")
```

Check free pages (space available for reclamation):

```python
free_pages = conn.db.execute("PRAGMA freelist_count").fetchone()[0]
page_size = conn.db.execute("PRAGMA page_size").fetchone()[0]
print(f"Free space: {free_pages * page_size / 1024 / 1024:.2f} MB")
```

---

## Troubleshooting

### "Database file is locked" during migration

**Cause:** Another process is using the database.

**Solution:**
1. Close all connections to the database
2. Ensure no other applications are accessing it
3. Check for WAL files: `ls -la mydb.db*`
4. If WAL files exist, the database wasn't closed properly

---

### Migration seems to hang

**Cause:** Large database or slow disk.

**Solution:**
1. Check disk space (migration needs ~2x database size temporarily)
2. Monitor progress: `ls -lh mydb.db*` (temp files should appear)
3. For very large databases (>10 GB), consider scheduling migration during off-hours

---

### "Disk I/O error" during migration

**Cause:** Insufficient disk space or permission issues.

**Solution:**
1. Ensure you have at least 2x the database size in free space
2. Check file permissions on the database directory
3. Verify the database isn't on a read-only filesystem

---

### My database didn't migrate even with AUTOVACUUM_MIGRATION=1

**Possible causes:**

1. **Modes already match**: Check current mode:

   ```python
   conn.db.execute("PRAGMA auto_vacuum").fetchone()[0]
   ```

2. **In-memory database**: Migration is skipped for `:memory:` databases.

3. **Cloned connection**: Internal cloned connections skip migration.

---

### Rollback happened - where are my backups?

**Good news:** If migration failed, your original database was restored automatically.

**Check logs** for the specific error. Common causes:
- Insufficient disk space
- Permission denied on temp directory
- Database corruption (run `PRAGMA integrity_check`)

---

### Compact returns 0 bytes freed but I know there's free space

**Cause:** `freeSpaceTargetMB` threshold not met.

**Solution:**

```python
# Check free space
free_pages = conn.db.execute("PRAGMA freelist_count").fetchone()[0]
page_size = conn.db.execute("PRAGMA page_size").fetchone()[0]
free_mb = free_pages * page_size / 1024 / 1024
print(f"Free: {free_mb:.2f} MB")

# Run with lower threshold
conn.command("compact", "collection", freeSpaceTargetMB=0)
```

---

## API Reference

### AutoVacuumMode Class

```python
from neosqlite import AutoVacuumMode

# Constants
AutoVacuumMode.NONE        # 0
AutoVacuumMode.FULL        # 1
AutoVacuumMode.INCREMENTAL # 2

# Validate input (accepts int or string)
AutoVacuumMode.validate(0)           # Returns: 0
AutoVacuumMode.validate("FULL")      # Returns: 1
AutoVacuumMode.validate("incremental")  # Returns: 2

# Convert to string
AutoVacuumMode.to_string(0)  # Returns: "NONE"
AutoVacuumMode.to_string(1)  # Returns: "FULL"
AutoVacuumMode.to_string(2)  # Returns: "INCREMENTAL"
```

### Connection Parameters

```python
neosqlite.Connection(
    "mydb.db",
    auto_vacuum=neosqlite.AutoVacuumMode.INCREMENTAL,  # Default
    journal_mode="WAL",  # Recommended
    # ... other parameters
)
```

### Command: vacuum

```python
# Full vacuum
result = conn.command("vacuum")
# Returns: {'ok': 1, 'message': 'VACUUM completed'}
```

### Command: compact

```python
# Full compact
result = conn.command("compact", "collection_name")
# Returns: {'bytesFreed': <bytes>, 'ok': 1}

# Dry run
result = conn.command("compact", "collection_name", dryRun=True)
# Returns: {'estimatedBytesFreed': <bytes>, 'ok': 1}

# With threshold and incremental
result = conn.command("compact", "collection_name", freeSpaceTargetMB=1)
# Returns: {'bytesFreed': <bytes>, 'ok': 1}
```

### Environment Variable

```bash
AUTOVACUUM_MIGRATION=1   # Enable automatic migration
AUTOVACUUM_MIGRATION=0   # Disable (default)
```

---

## See Also

- [SQLite AutoVacuum Documentation](https://www.sqlite.org/pragma.html#pragma_auto_vacuum)
- [SQLite VACUUM Command](https://www.sqlite.org/lang_vacuum.html)
- [MongoDB compact Command](https://www.mongodb.com/docs/manual/reference/command/compact/)
- [NeoSQLite Journal Mode Guide](./JOURNAL_MODE.md) (if available)

---

**Last Updated:** March 2026  
**NeoSQLite Version:** 1.11+
