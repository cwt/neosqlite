# AutoVacuum and Migration Guide

> **NeoSQLite 1.11+**

---

## Table of Contents

- [What is AutoVacuum?](#what-is-autovacuum)
- [Why Should You Care?](#why-should-you-care)
- [AutoVacuum Modes Explained](#autovacuum-modes-explained)
- [Using AutoVacuum in NeoSQLite](#using-autovacuum-in-neosqlite)
- [Migration: Changing AutoVacuum on Existing Databases](#migration-changing-autovacuum-on-existing-databases)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)

---

## What is AutoVacuum?

**AutoVacuum** is a SQLite setting that controls how the database reclaims disk space after you delete data.

Think of it like cleaning up after a party:

- **NONE**: You never clean up. Empty bottles and plates pile up. The room stays big forever.
- **FULL**: You clean up immediately after every guest leaves. Tidy, but you're constantly cleaning.
- **INCREMENTAL**: You note where the mess is, and clean up when it makes sense. Balanced approach.

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
│  4. Open backup, set new auto_vacuum, VACUUM INTO new file  │
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

### Example: Migrating an Existing Database

You have an existing database created with `auto_vacuum=NONE`:

```python
# Step 1: Create database with NONE mode (simulating existing DB)
import sqlite3
db = sqlite3.connect("myapp.db")
db.execute("PRAGMA auto_vacuum=0")  # NONE
db.execute("CREATE TABLE users (id INTEGER, name TEXT)")
db.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
db.commit()
db.close()

# Step 2: Enable migration and open with NeoSQLite
import os
os.environ["AUTOVACUUM_MIGRATION"] = "1"

import neosqlite
conn = neosqlite.Connection("myapp.db", auto_vacuum=neosqlite.AutoVacuumMode.INCREMENTAL)

# Database is now migrated to INCREMENTAL mode with all data preserved!
print(conn.db.execute("SELECT * FROM users").fetchall())
# [(1, 'Alice'), (2, 'Bob')]
```

---

### Migration Performance

Migration time depends on database size:

| Database Size | Approximate Migration Time |
|---------------|---------------------------|
| 1 MB | < 1 second |
| 100 MB | 1-3 seconds |
| 1 GB | 10-30 seconds |
| 10 GB | 2-5 minutes |

**Note:** Migration is a one-time operation per database when changing modes.

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

### For Development

**Enable migration in development to catch issues early:**

```bash
# In your .env or development config
AUTOVACUUM_MIGRATION=1
```

### For Production

**Be explicit about your auto_vacuum setting:**

```python
# Document your choice
conn = neosqlite.Connection(
    "production.db",
    auto_vacuum=neosqlite.AutoVacuumMode.INCREMENTAL,  # Balanced space/performance
    journal_mode="WAL"  # Recommended for concurrent workloads
)
```

### Monitoring

Check your current auto_vacuum mode:

```python
conn = neosqlite.Connection("myapp.db")
mode_value = conn.db.execute("PRAGMA auto_vacuum").fetchone()[0]
mode_name = neosqlite.AutoVacuumMode.to_string(mode_value)
print(f"AutoVacuum mode: {mode_name}")
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

### Environment Variable

```bash
AUTOVACUUM_MIGRATION=1   # Enable automatic migration
AUTOVACUUM_MIGRATION=0   # Disable (default)
```

---

## See Also

- [SQLite AutoVacuum Documentation](https://www.sqlite.org/pragma.html#pragma_auto_vacuum)
- [SQLite VACUUM Command](https://www.sqlite.org/lang_vacuum.html)
- [NeoSQLite Journal Mode Guide](./JOURNAL_MODE.md) (if available)

---

**Last Updated:** March 2026  
**NeoSQLite Version:** 1.11+
