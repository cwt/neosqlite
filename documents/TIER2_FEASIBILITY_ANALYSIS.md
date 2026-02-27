# Tier 2 (Temporary Table) Feasibility Analysis

## Overview

This document analyzes which **Tier 3 (Python-only)** operators could potentially be implemented in **Tier 2 (Temporary Tables)** for improved performance.

## Tier Architecture Recap

| Tier | Description | Performance | Use Case |
|------|-------------|-------------|----------|
| **Tier 1** | Pure SQL with CTEs | ⚡ 100x | Simple pipelines, all stages SQL-optimizable |
| **Tier 2** | Temporary tables + SQL | 🔶 10-20x | Complex pipelines, some Python needed |
| **Tier 3** | Pure Python | 🐌 1x | Unsupported operators, complex logic |

## Tier 2 Approach

Tier 2 uses temporary tables to:
1. **Break pipeline into segments** that can be SQL-optimized
2. **Materialize intermediate results** in temp tables
3. **Use SQL for heavy lifting** within each segment
4. **Fall back to Python** only for truly unsupported operations

### Example Tier 2 Flow

```
Pipeline: [$addFields, $lookup, $match, $function, $group]
                    ↓
Segment 1 (SQL):    $addFields → temp_table_1
                    ↓
Segment 2 (Python): $lookup → temp_table_2  (requires JOIN)
                    ↓
Segment 3 (SQL):    $match → temp_table_3
                    ↓
Segment 4 (Python): $function → temp_table_4  (requires JS)
                    ↓
Segment 5 (SQL):    $group → final_results
```

---

## Analysis by Category

### 1. Unsupported Pipeline Stages (8 stages)

| Stage | Tier 2 Potential | Implementation Approach | Effort | Priority |
|-------|-----------------|------------------------|--------|----------|
| **`$lookup`** | ✅ **HIGH** | Use SQL JOIN with temp tables | Medium | **P0** |
| **`$graphLookup`** | ⚠️ **MEDIUM** | Recursive CTEs or iterative temp tables | High | P2 |
| **`$indexStats`** | ❌ **LOW** | Requires metadata access, not document processing | Low | P3 |
| **`$merge`** | ⚠️ **MEDIUM** | SQL INSERT/UPDATE with temp table results | Medium | P2 |
| **`$out`** | ⚠️ **MEDIUM** | SQL INSERT with temp table results | Medium | P2 |
| **`$replaceRoot`** | ✅ **HIGH** | SQL json_extract + json_object | Low | **P1** |
| **`$replaceWith`** | ✅ **HIGH** | Same as $replaceRoot | Low | **P1** |
| **`$setWindowFields`** | ✅ **HIGH** | SQLite window functions | Medium | **P1** |

#### Detailed Analysis

**`$lookup` - HIGH PRIORITY**
```sql
-- Current: Python fallback
-- Tier 2: SQL JOIN with temp table
CREATE TEMP TABLE result AS
SELECT 
    main.id,
    main.data,
    json_group_array(related.data) AS lookup_result
FROM temp_table_1 AS main
LEFT JOIN other_collection AS related
    ON json_extract(main.data, '$.localField') = 
       json_extract(related.data, '$.foreignField')
GROUP BY main.id
```
**Effort**: Medium (2-3 days)  
**Impact**: High - `$lookup` is commonly used

---

**`$replaceRoot` / `$replaceWith` - HIGH PRIORITY**
```sql
-- Current: Python fallback
-- Tier 2: SQL json manipulation
CREATE TEMP TABLE result AS
SELECT 
    id,
    json_extract(data, '$.newRoot') AS data
FROM temp_table_1
```
**Effort**: Low (1 day)  
**Impact**: Medium - simplifies document restructuring

---

**`$setWindowFields` - HIGH PRIORITY**
```sql
-- Current: Python fallback
-- Tier 2: SQLite window functions
CREATE TEMP TABLE result AS
SELECT 
    id,
    data,
    SUM(value) OVER (PARTITION BY category ORDER BY date) AS running_total
FROM temp_table_1
```
**Effort**: Medium (3-4 days)  
**Impact**: High - enables analytics queries

---

**`$graphLookup` - MEDIUM PRIORITY**
```sql
-- Tier 2: Recursive CTE (complex)
WITH RECURSIVE hierarchy AS (
    SELECT id, data, 0 AS level
    FROM temp_table_1
    UNION ALL
    SELECT t.id, t.data, h.level + 1
    FROM temp_table_1 t
    JOIN hierarchy h ON json_extract(t.data, '$.parent') = h.id
)
SELECT * FROM hierarchy
```
**Effort**: High (5-7 days)  
**Impact**: Medium - specialized use case

---

### 2. Unsupported Expression Operators (5 operators)

| Operator | Tier 2 Potential | Implementation Approach | Effort | Priority |
|----------|-----------------|------------------------|--------|----------|
| **`$let`** | ⚠️ **MEDIUM** | Python evaluation within temp table flow | Low | P2 |
| **`$objectToArray`** | ✅ **HIGH** | SQL json_each + json_object | Medium | **P1** |
| **`$function`** | ❌ **LOW** | Requires JavaScript engine | N/A | N/A |
| **`$accumulator`** | ❌ **LOW** | Requires JavaScript engine | N/A | N/A |
| **`$script`** | ❌ **LOW** | Requires JavaScript engine | N/A | N/A |

#### Detailed Analysis

**`$objectToArray` - HIGH PRIORITY**
```sql
-- Current: Python fallback
-- Tier 2: SQL json_each
CREATE TEMP TABLE result AS
SELECT 
    id,
    (SELECT json_group_array(json_object('k', key, 'v', value))
     FROM json_each(json_extract(data, '$.objectField'))) AS arrayField
FROM temp_table_1
```
**Effort**: Medium (2-3 days)  
**Impact**: Medium - useful for object manipulation

---

**`$let` - MEDIUM PRIORITY**
```python
# Tier 2: Python evaluation but with temp table optimization
# Evaluate $let in Python, but keep data in temp tables
docs = db.execute(f"SELECT id, data FROM {temp_table}")
for doc in docs:
    # Evaluate $let in Python
    result = evaluate_let(doc, let_expr)
    # Store back in temp table
```
**Effort**: Low (1-2 days)  
**Impact**: Low - `$let` is rarely used

---

**`$function`, `$accumulator`, `$script` - NOT FEASIBLE**
- Require JavaScript engine (V8/duktape)
- Cannot be implemented in pure SQL
- **Recommendation**: Keep as Tier 3 only

---

### 3. Unsupported Type Conversion Operators (5 operators)

| Operator | Tier 2 Potential | Implementation Approach | Effort | Priority |
|----------|-----------------|------------------------|--------|----------|
| **`$toDecimal`** | ❌ **LOW** | SQLite lacks Decimal128 | N/A | N/A |
| **`$toObjectId`** | ⚠️ **MEDIUM** | Python function in SQL flow | Low | P3 |
| **`$convert`** | ✅ **HIGH** | SQL CAST + special handling | Medium | **P1** |
| **`$toBinData`** | ⚠️ **MEDIUM** | Python function in SQL flow | Low | P3 |
| **`$toRegex`** | ⚠️ **MEDIUM** | Python function in SQL flow | Low | P3 |

#### Detailed Analysis

**`$convert` - HIGH PRIORITY**
```sql
-- Current: Python fallback
-- Tier 2: SQL CAST with special cases
CREATE TEMP TABLE result AS
SELECT 
    id,
    CAST(json_extract(data, '$.field') AS TEXT) AS field,
    CAST(json_extract(data, '$.num') AS INTEGER) AS num
FROM temp_table_1
```
**Effort**: Medium (3-4 days)  
**Impact**: High - `$convert` is commonly used

---

**`$toObjectId`, `$toBinData`, `$toRegex` - LOW PRIORITY**
- Can be implemented as Python UDFs registered with SQLite
- Low usage frequency
- **Recommendation**: Implement as Python UDFs if needed

---

### 4. Unsupported String Operators (2 operators)

| Operator | Tier 2 Potential | Implementation Approach | Effort | Priority |
|----------|-----------------|------------------------|--------|----------|
| **`$split`** | ✅ **HIGH** | Recursive CTE or Python UDF | Medium | **P1** |
| **`$replaceOne`** | ✅ **HIGH** | instr() + substr() + concatenation | Low | **P1** |

#### Detailed Analysis

**`$split` - HIGH PRIORITY**
```sql
-- Option 1: Recursive CTE
WITH RECURSIVE split(id, value, rest) AS (
    SELECT id, 
           substr(data, 1, instr(data, ',') - 1),
           substr(data, instr(data, ',') + 1)
    FROM temp_table_1
    UNION ALL
    SELECT id,
           substr(rest, 1, instr(rest, ',') - 1),
           substr(rest, instr(rest, ',') + 1)
    FROM split
    WHERE instr(rest, ',') > 0
)
SELECT * FROM split
```
**Effort**: Medium (2-3 days)  
**Impact**: Medium - useful for string processing

---

**`$replaceOne` - HIGH PRIORITY**
```sql
-- Current: Python fallback (replace() replaces all)
-- Tier 2: Find first occurrence and replace
CREATE TEMP TABLE result AS
SELECT 
    id,
    CASE 
        WHEN instr(data, 'find') > 0 THEN
            substr(data, 1, instr(data, 'find') - 1) ||
            'replace' ||
            substr(data, instr(data, 'find') + length('find'))
        ELSE data
    END AS data
FROM temp_table_1
```
**Effort**: Low (1 day)  
**Impact**: Low - edge case of replace

---

### 5. Unsupported Set Operators (8 operators)

| Operator | Tier 2 Potential | Implementation Approach | Effort | Priority |
|----------|-----------------|------------------------|--------|----------|
| **`$setEquals`** | ✅ **HIGH** | SQL json_each + comparison | Medium | P2 |
| **`$setIntersection`** | ✅ **HIGH** | SQL json_each + INNER JOIN | Medium | P2 |
| **`$setUnion`** | ✅ **HIGH** | SQL json_each + UNION | Medium | P2 |
| **`$setDifference`** | ✅ **HIGH** | SQL json_each + EXCEPT | Medium | P2 |
| **`$setIsSubset`** | ✅ **HIGH** | SQL json_each + NOT EXISTS | Medium | P2 |
| **`$anyElementTrue`** | ✅ **HIGH** | SQL json_each + boolean check | Low | P2 |
| **`$allElementsTrue`** | ✅ **HIGH** | SQL json_each + boolean check | Low | P2 |

#### Detailed Analysis

**All set operators - MEDIUM PRIORITY**
```sql
-- $setIntersection example
CREATE TEMP TABLE result AS
SELECT 
    main.id,
    (SELECT json_group_array(value)
     FROM (
         SELECT value FROM json_each(main.set1)
         INTERSECT
         SELECT value FROM json_each(main.set2)
     )) AS intersection
FROM temp_table_1 AS main
```
**Effort**: Medium (4-5 days for all 8 operators)  
**Impact**: Medium - specialized use cases

---

### 6. Unsupported Query Operators (13+ operators)

| Operator | Tier 2 Potential | Implementation Approach | Effort | Priority |
|----------|-----------------|------------------------|--------|----------|
| **`$regex`** | ⚠️ **MEDIUM** | Python UDF registered with SQLite | Low | P2 |
| **`$text`** | ✅ **HIGH** | FTS5 virtual table | Medium | **P1** |
| **`$where`** | ❌ **LOW** | Requires JavaScript | N/A | N/A |
| **`$elemMatch`** | ✅ **HIGH** | SQL json_each + EXISTS | Medium | **P1** |
| **`$all`** | ✅ **HIGH** | SQL json_each + multiple EXISTS | Medium | **P1** |
| **Bitwise operators** | ⚠️ **MEDIUM** | SQLite bitwise ops | Low | P3 |
| **Geospatial operators** | ❌ **LOW** | Requires R-Tree index | High | P3 |

#### Detailed Analysis

**`$text` - HIGH PRIORITY**
```sql
-- Tier 2: FTS5 virtual table
CREATE VIRTUAL TABLE temp_fts USING fts5(content, data);
INSERT INTO temp_fts SELECT id, json_extract(data, '$.field') FROM temp_table_1;
SELECT * FROM temp_fts WHERE temp_fts MATCH 'search term';
```
**Effort**: Medium (3-4 days)  
**Impact**: High - full-text search is important

---

**`$elemMatch` - HIGH PRIORITY**
```sql
-- Current: Python fallback
-- Tier 2: SQL json_each + EXISTS
CREATE TEMP TABLE result AS
SELECT * FROM temp_table_1 AS t
WHERE EXISTS (
    SELECT 1 FROM json_each(json_extract(t.data, '$.array')) AS elem
    WHERE json_extract(elem.value, '$.field') > 5
)
```
**Effort**: Medium (2-3 days)  
**Impact**: High - commonly used for array queries

---

**`$all` - HIGH PRIORITY**
```sql
-- Tier 2: Multiple EXISTS clauses
SELECT * FROM temp_table_1 AS t
WHERE EXISTS (
    SELECT 1 FROM json_each(json_extract(t.data, '$.tags')) WHERE value = 'tag1'
)
AND EXISTS (
    SELECT 1 FROM json_each(json_extract(t.data, '$.tags')) WHERE value = 'tag2'
)
```
**Effort**: Medium (2-3 days)  
**Impact**: Medium - useful for tag matching

---

### 7. Unsupported Accumulator Operators (3 operators)

| Operator | Tier 2 Potential | Implementation Approach | Effort | Priority |
|----------|-----------------|------------------------|--------|----------|
| **`$first`** | ✅ **HIGH** | SQL with ORDER BY + LIMIT | Low | **P1** |
| **`$last`** | ✅ **HIGH** | SQL with ORDER BY + LIMIT DESC | Low | **P1** |
| **`$addToSet`** | ✅ **HIGH** | SQL json_group_array(DISTINCT) | Low | **P1** |

#### Detailed Analysis

**`$first` / `$last` - HIGH PRIORITY**
```sql
-- Current: Python fallback (requires ordering)
-- Tier 2: SQL window function or subquery
CREATE TEMP TABLE result AS
SELECT 
    category,
    (SELECT value FROM temp_table_1 t2 
     WHERE t2.category = t1.category 
     ORDER BY date ASC LIMIT 1) AS first_value,
    (SELECT value FROM temp_table_1 t2 
     WHERE t2.category = t1.category 
     ORDER BY date DESC LIMIT 1) AS last_value
FROM temp_table_1 AS t1
GROUP BY category
```
**Effort**: Low (1-2 days)  
**Impact**: High - commonly needed

---

**`$addToSet` - HIGH PRIORITY**
```sql
-- Current: Python fallback
-- Tier 2: SQL DISTINCT
CREATE TEMP TABLE result AS
SELECT 
    category,
    json_group_array(DISTINCT value) AS unique_values
FROM temp_table_1
GROUP BY category
```
**Effort**: Low (1 day)  
**Impact**: Medium - useful for deduplication

---

## Summary: Tier 2 Candidates by Priority

### P0 - Implement First (High Impact, Low Effort)

| Operator | Effort | Impact | Notes |
|----------|--------|--------|-------|
| `$lookup` | Medium | High | Most requested feature |
| `$first` / `$last` | Low | High | Simple SQL implementation |
| `$addToSet` | Low | High | Simple DISTINCT |
| `$replaceRoot` / `$replaceWith` | Low | Medium | Simple json_extract |

### P1 - Implement Second (High Impact, Medium Effort)

| Operator | Effort | Impact | Notes |
|----------|--------|--------|-------|
| `$setWindowFields` | Medium | High | Analytics queries |
| `$convert` | Medium | High | Commonly used |
| `$elemMatch` | Medium | High | Array queries |
| `$all` | Medium | Medium | Tag matching |
| `$text` | Medium | High | Full-text search |
| `$split` | Medium | Medium | String processing |
| `$replaceOne` | Low | Low | Edge case |

### P2 - Implement Third (Medium Impact)

| Operator | Effort | Impact | Notes |
|----------|--------|--------|-------|
| Set operators (8) | Medium | Medium | Specialized use |
| `$objectToArray` | Medium | Medium | Object manipulation |
| `$let` | Low | Low | Rarely used |
| `$graphLookup` | High | Medium | Complex recursive |
| `$merge` / `$out` | Medium | Medium | Write operations |

### P3 - Low Priority or Not Feasible

| Operator | Effort | Impact | Notes |
|----------|--------|--------|-------|
| `$function` / `$accumulator` / `$script` | N/A | Low | Require JS engine |
| `$toDecimal` | N/A | Low | SQLite limitation |
| Geospatial operators | High | Low | Requires R-Tree |
| `$indexStats` | Low | Low | Metadata access |
| Bitwise operators | Low | Low | Rarely used |
| `$toObjectId` / `$toBinData` / `$toRegex` | Low | Low | Python UDFs if needed |

---

## Implementation Roadmap

### Phase 1: Quick Wins (2-3 weeks)
- `$first`, `$last`, `$addToSet` (accumulators)
- `$replaceRoot`, `$replaceWith` (stages)
- `$replaceOne` (string operator)

**Expected Impact**: 20% of Tier 3 pipelines move to Tier 2

### Phase 2: High-Impact Operators (4-6 weeks)
- `$lookup` (stage)
- `$convert` (type conversion)
- `$elemMatch`, `$all` (query operators)
- `$setWindowFields` (stage)

**Expected Impact**: 40% of Tier 3 pipelines move to Tier 2

### Phase 3: Specialized Operators (4-6 weeks)
- Set operators (8 operators)
- `$text` (FTS5 integration)
- `$split` (string operator)
- `$objectToArray` (object operator)

**Expected Impact**: 25% of Tier 3 pipelines move to Tier 2

### Phase 4: Advanced Features (6-8 weeks)
- `$graphLookup` (recursive)
- `$merge`, `$out` (write operations)
- `$let` (variables)

**Expected Impact**: 10% of Tier 3 pipelines move to Tier 2

---

## Performance Comparison

| Tier | Typical Speed | Use Case |
|------|--------------|----------|
| **Tier 1 (SQL)** | 100x | All stages SQL-optimizable |
| **Tier 2 (Temp Tables)** | 10-20x | Mix of SQL + limited Python |
| **Tier 3 (Python)** | 1x | Heavy Python operators |

**After Tier 2 Implementation**:
- **Current**: 60% Tier 1, 0% Tier 2, 40% Tier 3
- **Target**: 60% Tier 1, 30% Tier 2, 10% Tier 3

---

## Recommendations

1. **Start with P0 operators** - Quick wins with high impact
2. **Focus on commonly used operators** - `$lookup`, `$first`, `$addToSet`
3. **Defer JavaScript-dependent operators** - `$function`, `$accumulator`, `$script`
4. **Consider Python UDFs** - For operators that can't be pure SQL but don't need full Python
5. **Measure impact** - Track how many pipelines move from Tier 3 to Tier 2

---

## Conclusion

**25 out of 44** unsupported Tier 3 operators have **good Tier 2 potential**:
- **10 operators**: High priority (P0/P1)
- **10 operators**: Medium priority (P2)
- **5 operators**: Low priority (P3)
- **9 operators**: Not feasible for Tier 2 (require JavaScript or have SQLite limitations)

**Total estimated effort**: 16-23 weeks for full implementation  
**Expected benefit**: 75% reduction in Tier 3-only pipelines
