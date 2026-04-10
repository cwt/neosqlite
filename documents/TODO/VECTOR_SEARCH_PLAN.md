# Vector Search Integration - Final Plan

Related GitHub issue: <https://github.com/cwt/neosqlite/issues/68>

## Executive Summary

After analyzing the GitHub Copilot conversation and validating against the actual NeoSQLite codebase, **the proposed vector search integration is technically feasible and well-aligned with the existing architecture**. This document presents the validated analysis, identifies necessary adjustments, and proposes a concrete implementation plan.

---

## 1. Validation of Copilot Analysis

### Accurate Claims

1. **sqlite-vec is production-ready**: Confirmed - v0.1.9 released March 31, 2026 on PyPI with wheels for Windows, macOS, and Linux
2. **MongoDB `$vectorSearch` API**: Correct - MongoDB uses aggregation stage with `queryVector`, `path`, `index`, `numCandidates`, `limit`, `similarity` parameters
3. **Three-tier architecture**: Accurate - NeoSQLite has Tier 1 (SQL), Tier 2 (temp tables), Tier 3 (Python fallback)
4. **Existing infrastructure**: Correct - the codebase already has:
   - `sql_tier_aggregator.py` (1,674 lines) with stage-based optimization
   - `IndexManager` for index lifecycle management
   - `Binary.VECTOR_SUBTYPE = 9` constant already defined
   - Extension loading mechanism in `connection.py`

### Required Adjustments

1. **File structure mismatch**: Copilot suggested `vector_index_manager.py` but the actual pattern is to extend `IndexManager` class in `collection/index_manager.py`
2. **sqlite-vec distance metrics**: Only `l1`, `l2`, and `cosine` are currently supported in vec0. MongoDB's `euclidean` maps to `l2`. MongoDB's `dotProduct` is **not yet supported** by sqlite-vec.
3. **SQL translation**: The proposed SQL using `MATCH` operator is correct for sqlite-vec, but score calculation needs adjustment:
   - sqlite-vec returns `distance` (lower = better)
   - MongoDB returns `score` (higher = better)
   - Conversion formula depends on similarity metric (see Section 7)
4. **Extension loading**: sqlite-vec provides both `sqlite_vec.load(db)` Python helper and standard `load_extension()` path. We should prefer the Python helper when available.

---

## 2. Technical Architecture

### 2.1 Component Integration Points

```text
neosqlite/
├── connection.py                    # MODIFY: optional sqlite-vec loading
├── binary.py                        # already has VECTOR_SUBTYPE constant
├── collection/
│   ├── __init__.py                  # MODIFY: add vector methods
│   ├── index_manager.py             # MODIFY: add vector index support
│   ├── sql_tier_aggregator.py       # MODIFY: add $vectorSearch stage
│   └── expr_evaluator/              # MODIFY: add $meta operator
└── _sqlite.py                       # already supports extension loading
```

### 2.2 sqlite-vec Extension Loading

**Current mechanism** (from `connection.py` lines 116-118):

```python
self.db.enable_load_extension(True)
self.db.execute(f"SELECT load_extension('{path}')")
```

**Proposed approach**:
- Make sqlite-vec **optional dependency** (not required for base functionality)
- Load extension lazily when first vector index is created
- Prefer `import sqlite_vec; sqlite_vec.load(db)` when the package is installed
- Fallback to `db.load_extension()` for environments with pre-compiled extension
- Graceful degradation: raise informative error if extension not available

### 2.3 Vector Index Storage

**sqlite-vec virtual table pattern**:

```sql
CREATE VIRTUAL TABLE vec_embedding_index USING vec0(
    embedding float[1536] distance_metric=cosine
)
```

**Supported distance metrics**: `l1`, `l2`, `cosine` (sqlite-vec v0.1.x)
**MongoDB API mapping**:
- MongoDB `cosine` -> sqlite-vec `distance_metric=cosine`
- MongoDB `euclidean` -> sqlite-vec `distance_metric=l2`
- MongoDB `dotProduct` -> **not yet supported** by sqlite-vec

**Vector insertion** (JSON array format):

```sql
INSERT INTO vec_embedding_index(rowid, embedding)
VALUES (1, '[0.1, 0.2, 0.3, ...]');
```

**Linking to documents**:
- Each vec table row has implicit `rowid`
- Map to NeoSQLite document `id` column (the internal SQLite row id)
- Sync on insert/update/delete operations

### 2.4 Aggregation Pipeline Integration

**Current support** (from `sql_tier_aggregator.py`):
- Stage dispatch via `_build_stage_sql()` method (line 834)
- Uses `match` statement for stage routing
- Returns `(sql_template, params)` tuple

**Proposed `$vectorSearch` handler**:

```python
case "$vectorSearch":
    return self._build_vector_search_sql(stage_spec, prev_stage, context)
```

**SQL generation pattern** (Tier 1 optimization):

```sql
WITH vec_results AS (
    SELECT rowid, distance
    FROM vec_embedding_index
    WHERE embedding MATCH ?
    ORDER BY distance ASC
    LIMIT ?
)
SELECT v.rowid, c.data,
       (1.0 - v.distance) AS vectorSearchScore
FROM vec_results v
JOIN collection c ON c.id = v.rowid
ORDER BY vectorSearchScore DESC
```

---

## 3. Implementation Plan

### Phase 1: Foundation & Infrastructure (Priority: HIGH)

#### Task 1.1: Add sqlite-vec as Optional Dependency
- **File**: `pyproject.toml`
- **Action**: Add `sqlite-vec = { version = "^0.1.9", optional = true }`
- **Extra**: Add `vector = ["sqlite-vec"]` to `[tool.poetry.extras]`

#### Task 1.2: Extend IndexManager for Vector Indexes
- **File**: `collection/index_manager.py`
- **Methods to add**:
  - `create_vector_index(field, dimensions, similarity="cosine", index_name=None)`
  - `drop_vector_index(index_name)`
  - `list_vector_indexes()`
- **Validation**:
  - Check sqlite-vec extension loaded
  - Validate dimensions (positive integer)
  - Validate similarity (`cosine`, `euclidean`/`l2`, `l1` only; reject `dotProduct` with informative error)

#### Task 1.3: Add Vector Index Synchronization
- **File**: `collection/__init__.py`
- **Modify methods**:
  - `insert_one()` - sync vector after insert
  - `insert_many()` - batch sync vectors
  - `update_one()` / `update_many()` - update vectors
  - `delete_one()` / `delete_many()` - remove vectors
- **Strategy**: Use SQLite triggers or explicit Python sync calls

### Phase 2: Aggregation Pipeline Support (Priority: HIGH)

#### Task 2.1: Add `$vectorSearch` Stage Handler
- **File**: `collection/sql_tier_aggregator.py`
- **Actions**:
  - Add `"$vectorSearch"` to `SUPPORTED_STAGES` set
  - Add `case "$vectorSearch"` to `_build_stage_sql()` match statement
  - Implement `_build_vector_search_sql(spec, prev_stage, context)` method

#### Task 2.2: Implement SQL Generation for Vector Search
- **Input spec** (MongoDB format):

  ```python
  {
      "$vectorSearch": {
          "queryVector": [0.1, 0.2, ...],
          "path": "embedding",
          "index": "embedding_idx",
          "numCandidates": 100,
          "limit": 10,
          "similarity": "cosine"
      }
  }
  ```

- **Output SQL**: CTE with vec virtual table query + JOIN to documents
- **Parameters**: `queryVector` as MATCH parameter, `limit` for LIMIT clause

#### Task 2.3: Add `$meta: "vectorSearchScore"` Support
- **File**: `collection/expr_evaluator/__init__.py`
- **Context**: In `$project` stage, handle `{"score": {"$meta": "vectorSearchScore"}}`
- **Implementation**: Map to the calculated score column from vector search CTE

### Phase 3: Testing & Validation (Priority: HIGH)

#### Task 3.1: Unit Tests
- **File**: `tests/test_vector_search.py`
- **Test cases**:
  - Create/drop vector index
  - Insert documents with vector fields
  - Vector search with different similarity metrics
  - Score calculation correctness
  - Edge cases: null vectors, dimension mismatch, empty collection

#### Task 3.2: Integration Tests
- **Test scenarios**:
  - Vector search + `$match` filter pipeline
  - Vector search + `$project` with score
  - Vector search + `$limit` combination
  - Bulk insert with vector sync performance

#### Task 3.3: API Compatibility Tests
- Verify PyMongo-compatible method signatures
- Test error messages match MongoDB format
- Validate return types (cursors, results)

### Phase 4: Documentation & Examples (Priority: MEDIUM)

#### Task 4.1: API Documentation
- **File**: `documents/VECTOR_SEARCH.md`
- **Content**:
  - API reference for `create_vector_index()`
  - Aggregation pipeline examples
  - Similarity metric comparison
  - Performance considerations

#### Task 4.2: Example Scripts
- **Directory**: `examples/`
- **Scripts**:
  - `vector_search_basic.py` - Simple semantic search
  - `vector_search_rag.py` - RAG application example
  - `vector_search_benchmark.py` - Performance comparison

#### Task 4.3: Update README
- Add vector search to feature list
- Include quick start example
- Mention sqlite-vec as optional dependency

### Phase 5: Advanced Features (Priority: LOW - Future)

#### Task 5.1: Approximate Nearest Neighbors (ANN)
- Explore sqlite-vec ANN indexing options
- Add `index_type` parameter to `create_vector_index()`

#### Task 5.2: Batch Vector Operations
- Optimize bulk vector inserts
- Add vector-specific bulk API

#### Task 5.3: Metadata Filtering
- Support `$filter` in `$vectorSearch` stage
- Combine vector search with document field filters

#### Task 5.4: Wire Protocol Support
- Add `$vectorSearch` to NX-27017 MongoDB Wire Protocol server

---

## 4. Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|-----------|
| sqlite-vec API unstable | Low | Medium | Pin to specific version, write adapter layer |
| Performance degradation | Low | High | Benchmark early, use SQL tier optimization |
| Breaking changes to NeoSQLite | Very Low | High | Keep feature isolated, no changes to core data model |
| Extension loading failures | Medium | Medium | Graceful error messages, optional dependency |
| Cross-platform compatibility | Medium | Medium | Test on Windows, macOS, Linux; provide fallback |

---

## 5. Success Criteria

### Must Have (Phase 1-3)
- [ ] Vector index creation/deletion API
- [ ] `$vectorSearch` aggregation stage works
- [ ] Score calculation matches MongoDB semantics
- [ ] All tests passing (existing + new)
- [ ] No breaking changes to existing functionality

### Nice to Have (Phase 4)
- [ ] Complete documentation
- [ ] Working examples
- [ ] Basic benchmarks

### Future Work (Phase 5)
- [ ] ANN indexing
- [ ] Metadata filtering
- [ ] Wire protocol support
- [ ] dotProduct support (when sqlite-vec adds it)

---

## 6. Recommended Next Steps

1. **Approve this plan** - Review and validate the approach
2. **Create feature branch** - `feature/vector-search`
3. **Start Phase 1** - Begin with foundation work (dependencies, IndexManager)
4. **Prototype early** - Get basic vector search working before polishing API
5. **Iterate based on feedback** - Adjust implementation as needed

---

### 7. Technical Notes

### sqlite-vec Supported Metrics and Score Conversion

sqlite-vec v0.1.x supports three distance metrics:

| Metric | sqlite-vec | MongoDB equivalent | Distance range | Score conversion |
|--------|-----------|-------------------|----------------|------------------|
| Cosine | `cosine` | `cosine` | [0, 2] | `score = 1 - (distance / 2)` |
| L2 (Euclidean) | `l2` | `euclidean` | [0, infinity) | `score = 1 / (1 + distance)` |
| L1 | `l1` | N/A (NeoSQLite extension) | [0, infinity) | `score = 1 / (1 + distance)` |

**Note**: MongoDB's `dotProduct` is not yet supported by sqlite-vec. If a user requests `dotProduct`, raise an informative error suggesting `cosine` as an alternative.

### MongoDB API Compatibility
- MongoDB requires explicit `numCandidates` parameter
- NeoSQLite can make `numCandidates` optional (default to `limit` * 10)
- MongoDB's `index` field maps to sqlite-vec virtual table name

### Performance Considerations
- Vector search is O(n) without ANN indexing
- For collections >100k documents, recommend ANN or hybrid approach
- SQL tier optimization provides 10-100x speedup over Python fallback

---

## Appendix: Example API Usage

```python
import neosqlite
from sentence_transformers import SentenceTransformer

# Setup
db = neosqlite.Connection("app.db")
docs = db.documents

# Create vector index (new API)
docs.create_vector_index("embedding", dimensions=384, similarity="cosine")

# Insert documents with embeddings
encoder = SentenceTransformer("all-MiniLM-L6-v2")
docs.insert_many([
    {
        "text": "The quick brown fox",
        "embedding": encoder.encode("The quick brown fox").tolist()
    },
    {
        "text": "Jumps over the lazy dog",
        "embedding": encoder.encode("Jumps over the lazy dog").tolist()
    }
])

# Vector search via aggregation pipeline (MongoDB-compatible API)
query = "fast animal"
query_vec = encoder.encode(query).tolist()

results = list(docs.aggregate([
    {
        "$vectorSearch": {
            "queryVector": query_vec,
            "path": "embedding",
            "index": "vec_embedding_index",
            "numCandidates": 100,
            "limit": 5
        }
    },
    {
        "$project": {
            "text": 1,
            "score": {"$meta": "vectorSearchScore"}
        }
    }
]))
# Output: [{"_id": ..., "text": "The quick brown fox", "score": 0.92}, ...]

# List vector indexes
docs.list_vector_indexes()
# Output: [{"name": "vec_embedding_index", "field": "embedding", ...}]

# Drop vector index
docs.drop_vector_index("vec_embedding_index")
```
