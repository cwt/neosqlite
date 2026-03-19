# NX-27017 Sub-Projects

This directory contains sub-projects/packages that are part of the NX-27017 ecosystem but maintained separately from the main neosqlite package.

## Packages

### nx-27017

MongoDB Wire Protocol Server backed by SQLite.

**Location:** `packages/nx_27017/`

**Installation:**
```bash
cd packages/nx_27017
pip install -e .
```

**Usage:**
```bash
# Run server
nx-27017 --db memory

# Run as daemon
nx-27017 -d

# Check status
nx-27017 --status

# Stop daemon
nx-27017 --stop
```

**Development:**
```bash
cd packages/nx_27017
pip install -e ".[dev]"
pytest tests/
```

## Structure

```
packages/
└── nx_27017/
    ├── src/
    │   └── nx_27017/
    │       ├── __init__.py
    │       └── nx_27017.py    # Main server implementation
    ├── tests/
    │   └── test_nx_27017.py
    ├── pyproject.toml
    ├── README.md
    └── .gitignore
```

## Why Sub-Projects?

Sub-projects allow us to:
- Maintain separate version numbers
- Have independent dependencies
- Test and release independently
- Keep the main neosqlite package focused
