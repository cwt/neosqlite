---
type: index
title: "NeoSQLite Knowledge Base"
description: "Root index and progressive disclosure catalog of architecture, design, API specifications, and runbooks for NeoSQLite."
tags:
  - index
  - architecture
  - documentation
  - neosqlite
timestamp: 2026-09-13T00:00:00Z
version: "1.16.2"
lifecycle: active
---

# NeoSQLite Knowledge Base

Welcome to the NeoSQLite structured knowledge base, organized according to the **Google Open Knowledge Format (OKF) v0.2** specification. NeoSQLite is an embedded, serverless MongoDB-compatible document database implemented on top of SQLite using a three-tier architecture:

1. **Tier 1 (SQL CTE)**: Direct SQL translation using Common Table Expressions and SQLite JSON functions for 10–100x speedup.
2. **Tier 2 (Virtual Tables & Extensions)**: SQLite FTS5 for text search, SpatiaLite / virtual tables, and temporary table hash-joins.
3. **Tier 3 (Authoritative Python Fallback)**: Full-fidelity in-memory Python pipeline evaluation ensuring 100% MongoDB semantic correctness.

---

## Directory Catalog

### 1. Architecture & Optimization

| Document | Type | Description |
| :--- | :--- | :--- |
| [Advanced Index-Aware Optimization](./advanced-index-aware-optimization.md) | `architecture_guideline` | Techniques for leveraging SQLite indexes across query patterns. |
| [Aggregation Pipeline Optimization](./aggregation-pipeline-optimization.md) | `architecture_guideline` | Translating pipeline stages to native SQLite CTE queries with benchmarks. |
| [Performance Optimization Guide](./performance-optimization.md) | `architecture_guideline` | Comprehensive tuning guide covering pragmas, indexing, and benchmarks. |
| [API Development Strategy](./api-development-strategy.md) | `architecture_guideline` | Strategic roadmap and execution phases for PyMongo API coverage. |

### 2. Core Features & API Specifications

| Document | Type | Description |
| :--- | :--- | :--- |
| [GridFS Documentation](./gridfs.md) | `api_spec` | Specification and guide for `GridFS` and `GridFSBucket` implementations. |
| [Text Search in NeoSQLite](./text-search.md) | `api_spec` | Full-text search using SQLite FTS5, `$text`, and custom tokenizers. |
| [Change Streams with watch()](./watch.md) | `api_spec` | Change stream support via native SQLite triggers without replica sets. |
| [TTL Indexes and Document Expiry](./ttl-indexes.md) | `api_spec` | `expireAfterSeconds` declaration with lazy and background expiry. |
| [ObjectId Implementation](./objectid-implementation.md) | `api_spec` | 12-byte BSON-compatible ObjectId and binary SQLite storage format. |
| [Aggregation Expressions Guide](./aggregation-expression-guide.md) | `api_spec` | Reference and usage guide for expression operators in aggregation. |
| [PyMongo API Comparison](./pymongo-api-comparison.md) | `reference` | Comprehensive method-by-method PyMongo compatibility matrix. |

### 3. Internal Implementation & Subsystems

| Document | Type | Description |
| :--- | :--- | :--- |
| [$expr Operator Implementation](./expr-implementation.md) | `design_doc` | Tiered SQL translation and Python evaluation fallback for `$expr`. |
| [Facet Implementation](./facet-implementation.md) | `design_doc` | Running multiple independent sub-pipelines via `$facet`. |
| [$lookup Implementation](./lookup-implementation.md) | `design_doc` | Join stages across CTE, temporary table, and Python fallback tiers. |
| [json_each() Enhancements](./json-each-enhancements.md) | `design_doc` | Leveraging SQLite `json_each()` virtual table for array query operations. |
| [SQL Translation Caching](./translation-cache.md) | `design_doc` | LRU caching mechanism for pre-translated queries and pipelines. |
| [Temporary Table Aggregation Breakdown](./temp-table-breakdown.md) | `design_doc` | Modular refactor architecture for temporary table aggregation. |
| [Hybrid Text Search Processing](./hybrid-text-search-complete.md) | `design_doc` | Specification for hybrid FTS5 text search combined with aggregation. |

### 4. Operations, Testing & Debugging

| Document | Type | Description |
| :--- | :--- | :--- |
| [Database Maintenance Guide](./database-maintenance.md) | `runbook` | Auto-vacuum, compaction, journal mode configuration, and `dbStats`. |
| [Testing Strategy](./testing-strategy.md) | `policy` | Testing methodology, coverage standards, and differential MongoDB testing. |
| [Force Fallback Kill Switch](./force-fallback-kill-switch.md) | `guide` | Using `NEOSQLITE_FORCE_FALLBACK` for debugging and benchmarking. |
| [API Feasibility Assessment](./api-feasibility-assessment.md) | `evaluation` | Technical feasibility analysis of PyMongo APIs mapped to SQLite. |
| [API Analysis Summary](./analysis-summary.md) | `evaluation` | High-level evaluation of API coverage and implementation status. |

---

## Subdirectories

- **[Release Notes](./releases/index.md)**: Version-by-version release notes and changelogs for all releases (v1.0.0 through v1.16.2).
- **[Roadmap & Future Plans (TODO)](./todo/index.md)**: Proposals and implementation plans for geospatial query support and vector search integration.

---

## Bundle Governance & History

- **[Bundle Modification Log](./log.md)**: Chronological update log tracking structural changes to this knowledge base.
