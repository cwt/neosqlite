# GridFS Table Migration Guide

## Overview

Starting from NeoSQLite version 1.3.0, GridFS collections use underscore-based table names (e.g., `fs_files`, `fs_chunks`) instead of dot-based names (e.g., `fs.files`, `fs.chunks`) for better SQLite compatibility.

## Automatic Migration

NeoSQLite automatically detects and migrates existing GridFS tables when you first access them:

- **Detection**: Checks for tables with dot-based names (e.g., `fs.files`)
- **Migration**: Renames tables to underscore-based names (e.g., `fs_files`)
- **Fallback**: If migration fails, continues using old table names for backward compatibility

Migration happens transparently during GridFS initialization and requires no user action.

## Manual Migration (If Needed)

If you need to migrate manually or encounter issues:

```sql
-- For default 'fs' bucket
ALTER TABLE fs.files RENAME TO fs_files;
ALTER TABLE fs.chunks RENAME TO fs_chunks;

-- For custom bucket names (replace 'mybucket' with your bucket name)
ALTER TABLE mybucket.files RENAME TO mybucket_files;
ALTER TABLE mybucket.chunks RENAME TO mybucket_chunks;
```

## What Changed

- **Before**: `fs.files`, `fs.chunks` (caused SQLite parsing issues)
- **After**: `fs_files`, `fs_chunks` (clean, compatible)
- **Benefit**: Enables PyMongo-like `db.fs.files` syntax

## Metadata Column Changes

### New Databases (1.3.0+)
- Metadata column created as `JSONB` type (when SQLite supports it)
- Stored in efficient CBOR binary format
- Better performance for JSON operations

### Existing Databases (Upgraded)
- Metadata column remains `TEXT` (no schema alteration)
- **Gradual migration**: Individual rows migrate to JSONB when metadata is updated
- Uses `jsonb_set()` for updates, which stores as JSONB
- `json()` wrapper ensures both TEXT and JSONB read correctly

### Example
```sql
-- Old database metadata column (unchanged after upgrade)
CREATE TABLE fs_files (
    ...
    metadata TEXT  -- Still TEXT after upgrade
);

-- After updating a row's metadata with jsonb_set()
-- That row is now stored as JSONB/CBOR internally
-- But reads the same via API thanks to json() wrapper
```

## Backward Compatibility

- Existing databases are automatically migrated
- No data loss during migration
- Old table access still works if migration fails
- New databases use underscore naming from the start

## Troubleshooting

If you encounter issues:

1. Check that your SQLite version supports `ALTER TABLE RENAME`
2. Ensure no other processes are accessing the database during migration
3. Verify table names in `sqlite_master`: `SELECT name FROM sqlite_master WHERE type='table'`

Migration is one-time and safe - it only runs when old tables are detected.