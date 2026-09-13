---
type: index
title: "NeoSQLite Release Notes Index"
description: "Chronological release history and version changelogs for NeoSQLite."
tags:
  - index
  - releases
  - changelog
timestamp: 2026-09-13T00:00:00Z
version: "1.16.0"
lifecycle: active
---

# NeoSQLite Release Notes

This directory contains individual release notes and detailed changelogs for each version of NeoSQLite.

## Release History

| Version | Release Date | Highlights & Summary |
| :--- | :--- | :--- |
| [v1.16.0](./v1.16.0.md) | 2026-09-13 | NeoSQLite v1.16.0 is a feature release delivering PyMongo modern API parity, TTL index expiry, and change-stream hardening for SQLite-backed job queues. |
| [v1.15.2](./v1.15.2.md) | 2026-09-08 | NeoSQLite v1.15.2 is a correctness, stability, and documentation release fixing SQL-tier bugs in $lookup and array updates, hardening NX-27017, and updating dependencies. |
| [v1.15.1](./v1.15.1.md) | 2026-08-26 | NeoSQLite v1.15.1 is a focused correctness patch fixing ten SQL-tier bugs (across five areas) surfaced while verifying v1.15.0 against real MongoDB. All fixes are backward compa... |
| [v1.15.0](./v1.15.0.md) | 2026-08-26 | NeoSQLite v1.15.0 is a major correctness and parity release resolving all 86 findings from a deep code audit, alongside 16 new SQL-tier expression converters, Tier-1 $group acti... |
| [v1.14.15](./v1.14.15.md) | 2026-07-01 | NeoSQLite v1.14.15 is a bug-fix release that addresses SQL injection vectors, data integrity issues, and correctness bugs identified since v1.14.14. |
| [v1.14.14](./v1.14.14.md) | 2026-04-29 | NeoSQLite v1.14.14 is a security hardening, code quality, and modernization release that patches SQL injection vectors, improves transaction atomicity, and modernizes the codeba... |
| [v1.14.13](./v1.14.13.md) | 2026-04-16 | NeoSQLite v1.14.13 is a bug fix and maintenance release that ensures compatibility with newer SQLite versions (3.45.0+) and restores the full index comparison suite in the API b... |
| [v1.14.12](./v1.14.12.md) | 2026-04-15 | NeoSQLite v1.14.12 is a bug fix release that corrects the CTE-based array operators implementation introduced in v1.14.11, fixing a \ |
| [v1.14.11](./v1.14.11.md) | 2026-04-15 | NeoSQLite v1.14.11 is a performance and correctness release that adds efficient CTE-based SQL support for array operators ($in, $nin, $all), implements native regex operators, a... |
| [v1.14.10](./v1.14.10.md) | 2026-04-14 | NeoSQLite v1.14.10 is a bug fix release that resolves the $in/$nin operator bug for both find() and aggregation pipelines, adds proper MongoDB array semantics for comparison ope... |
| [v1.14.9](./v1.14.9.md) | 2026-04-13 | NeoSQLite v1.14.9 is a bug fix release that resolves two critical aggregation pipeline bugs and improves $expr operator compatibility with MongoDB. |
| [v1.14.8](./v1.14.8.md) | 2026-04-13 | NeoSQLite v1.14.8 is a bug fix release that resolves a critical $facet stage failure when JSONB is enabled. |
| [v1.14.7](./v1.14.7.md) | 2026-04-13 | NeoSQLite v1.14.7 is a bug fix release that resolves three critical aggregation pipeline failures causing application crashes when processing corrupted or malformed data, plus s... |
| [v1.14.6](./v1.14.6.md) | 2026-04-13 | NeoSQLite v1.14.6 is a bug fix release that resolves a critical $facet aggregation stage failure in the Tier 2 (temporary table) processing path. |
| [v1.14.5](./v1.14.5.md) | 2026-04-13 | NeoSQLite v1.14.5 is a feature enhancement release that adds native FTS5 BM25 text relevance scoring to aggregation pipelines and enables $push/$addToSet with nested object expr... |
| [v1.14.4](./v1.14.4.md) | 2026-04-12 | NeoSQLite v1.14.4 is a comprehensive bug fix release that resolves 5 critical aggregation pipeline issues including $lookup returning incorrect types, ObjectId parameter binding... |
| [v1.14.3](./v1.14.3.md) | 2026-04-12 | NeoSQLite v1.14.3 is a bug fix and performance optimization release that resolves SQL errors when using $addFields with complex expressions (like $filter, $map, $reduce) after $... |
| [v1.14.2](./v1.14.2.md) | 2026-04-06 | NeoSQLite v1.14.2 is a bug fix and performance optimization release that resolves compound index sort order issues in the NX-27017 wire protocol and improves write performance t... |
| [v1.14.1](./v1.14.1.md) | 2026-04-06 | NeoSQLite v1.14.1 is a correctness and compatibility release that eliminates major limitations in the SQL evaluation tier. It introduces Static Type Inference to perfectly match... |
| [v1.14.0](./v1.14.0.md) | 2026-04-05 | NeoSQLite v1.14.0 is a major performance and API alignment release that introduces native SQL-tier support for the $project aggregation stage and complex _id field operators. It... |
| [v1.13.11](./v1.13.11.md) | 2026-04-03 | NeoSQLite v1.13.11 is a bug fix and code quality release that resolves critical JSONB function mismatches affecting sort and min/max clause builders, fixes multiple SQL generati... |
| [v1.13.10](./v1.13.10.md) | 2026-04-01 | NeoSQLite v1.13.10 is a bug fix and code quality release that resolves 28 bugs across all severity levels, modernizes code with Python 3.10 pattern matching, and improves reliab... |
| [v1.13.9](./v1.13.9.md) | 2026-03-26 | NeoSQLite v1.13.9 is a code organization and architecture release that splits the monolithic NX-27017 module (2705 lines) into focused, maintainable components and adds full $ch... |
| [v1.13.8](./v1.13.8.md) | 2026-03-25 | NeoSQLite v1.13.8 is a security and reliability release that fixes SQL injection vulnerabilities, adds context manager support for cursors, and improves transaction cleanup reli... |
| [v1.13.7](./v1.13.7.md) | 2026-03-24 | NeoSQLite v1.13.7 is a bug fix release that improves MongoDB compatibility by adding $collStats aggregation stage support and fixing edge cases in aggregation pipeline parsing a... |
| [v1.13.6](./v1.13.6.md) | 2026-03-23 | NeoSQLite v1.13.6 is a bug fix release that improves MongoDB compatibility for string operators and SQL projections. |
| [v1.13.5](./v1.13.5.md) | 2026-03-22 | NeoSQLite v1.13.5 is a performance release that introduces O(n+m) hash join optimization for $lookup aggregation, along with memory-aware adaptive query planning. |
| [v1.13.4](./v1.13.4.md) | 2026-03-21 | NeoSQLite v1.13.4 is a bug fix release that adds GridFS support to NX-27017, along with performance improvements, configurable journal modes, and stability enhancements. |
| [v1.13.0](./v1.13.0.md) | 2026-03-20 | NeoSQLite v1.13.0 is a major feature release that introduces NX-27017, a MongoDB Wire Protocol Server backed by SQLite. This allows MongoDB clients (including PyMongo) to connec... |
| [v1.12.1](./v1.12.1.md) | 2026-03-19 | NeoSQLite v1.12.1 is a bug fix release that addresses PyMongo 4.16+ compatibility for Cursor.min(), Cursor.max(), and Cursor.hint() methods. |
| [v1.12.0](./v1.12.0.md) | 2026-03-19 | NeoSQLite v1.12.0 introduces AutoVacuum Support with Transparent Migration, MongoDB-Compatible compact Command, dbStats Command, and Additional Maintenance Commands (wal_checkpo... |
| [v1.11.0](./v1.11.0.md) | 2026-03-18 | NeoSQLite v1.11.0 introduces SQL Translation Caching for aggregation pipelines and $expr queries, delivering 10-30% performance improvements for repeated query patterns. This re... |
| [v1.10.1](./v1.10.1.md) | 2026-03-17 | NeoSQLite v1.10.1 is a maintenance release that explicitly clarifies support for Change Streams and multi-document transactions in documentation and benchmark reports. |
| [v1.10.0](./v1.10.0.md) | 2026-03-16 | Release notes and changelog for NeoSQLite v1.10.0. |
| [v1.9.2](./v1.9.2.md) | 2026-03-14 | Release notes and changelog for NeoSQLite v1.9.2. |
| [v1.9.1](./v1.9.1.md) | 2026-03-13 | Release notes and changelog for NeoSQLite v1.9.1. |
| [v1.9.0](./v1.9.0.md) | 2026-03-13 | Release notes and changelog for NeoSQLite v1.9.0. |
| [v1.8.0](./v1.8.0.md) | 2026-03-12 | Release notes and changelog for NeoSQLite v1.8.0. |
| [v1.7.0](./v1.7.0.md) | 2026-03-12 | Release notes and changelog for NeoSQLite v1.7.0. |
| [v1.6.1](./v1.6.1.md) | 2026-03-12 | This is a documentation and completion release that achieves a significant milestone: |
| [v1.6.0](./v1.6.0.md) | 2026-03-12 | This is a quality-focused release that achieves two major milestones: |
| [v1.5.0](./v1.5.0.md) | 2026-03-12 | This is a feature-complete release that brings NeoSQLite to 99% MongoDB $expr operator compatibility (119 out of 120 operators implemented). This release completes all missing m... |
| [v1.4.0](./v1.4.0.md) | 2026-03-12 | This is a major feature release that introduces the MongoDB $expr operator framework with a sophisticated three-tier evaluation architecture (SQL → Temp Tables → Python fallback... |
| [v1.3.2](./v1.3.2.md) | 2026-03-12 | This is a major feature release that completes the GridFS implementation by adding all previously missing convenience methods and introducing enhanced metadata features. NeoSQLi... |
| [v1.3.1](./v1.3.1.md) | 2026-03-12 | This is a bug fix release that enables full PyMongo-style db.fs.files collection access by automatically delegating GridFS operations to the GridFSBucket API. This includes find... |
| [v1.3.0](./v1.3.0.md) | 2026-02-24 | This release introduces major enhancements to GridFS functionality with PyMongo-style nested access syntax, automatic legacy table migration, and several new aggregation pipelin... |
| [v1.2.3](./v1.2.3.md) | 2026-02-19 | This release fixes a critical bug in ObjectId type conversion for reference fields, restoring full MongoDB-compatible API behavior for queries on fields like parent_post, author... |
| [v1.2.2](./v1.2.2.md) | 2026-03-12 | This release enhances the $elemMatch operator to support simple value matching in JSON arrays, making it fully compatible with MongoDB's behavior. The release maintains full bac... |
| [v1.2.1](./v1.2.1.md) | 2026-03-12 | This is a minor enhancement release that includes internal improvements and bug fixes for better ObjectId handling, change streams, and code quality. The release maintains full ... |
| [v1.2.0](./v1.2.0.md) | 2026-03-12 | This release introduces sophisticated datetime query processing capabilities with enhanced JSON path parsing, specialized datetime indexing, and a three-tier fallback mechanism ... |
| [v1.1.2](./v1.1.2.md) | 2026-03-12 | This is a significant enhancement release that adds full GridFS support with MongoDB-compatible ObjectId functionality. The release includes a complete GridFS implementation tha... |
| [v1.1.1](./v1.1.1.md) | 2026-03-12 | This is a minor enhancement release that improves the robustness of NeoSQLite by automatically detecting and correcting common ID type mismatches between integer IDs and ObjectI... |
| [v1.1.0](./v1.1.0.md) | 2026-03-12 | This release introduces MongoDB-compatible ObjectId support to NeoSQLite, providing full 12-byte ObjectId generation, storage, and interchangeability with PyMongo. The release a... |
| [v1.0.0](./v1.0.0.md) | 2026-03-12 | This release marks a significant milestone for NeoSQLite with the official v1.0.0 stable release. The release includes critical bug fixes, performance improvements, enhanced JSO... |
