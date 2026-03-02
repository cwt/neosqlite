# Missing APIs in API Comparison Tests

This document lists all NeoSQLite features that are **implemented** but **NOT tested** in the API comparison package (`examples/api_comparison/`).

**Goal**: Add tests for all missing APIs to achieve comprehensive coverage.

---

## Package Structure

The comparison tests have been refactored into a modular package structure:

```
examples/api_comparison/
├── __init__.py              # Package initialization
├── reporter.py              # CompatibilityReporter class
├── runner.py                # Test orchestration
├── utils.py                 # Utility functions (MongoDB connection)
├── crud.py                  # CRUD operations
├── query_operators.py       # Query operators
├── expr_operators.py        # $expr operators (core)
├── expr_additional.py       # Additional $expr operators
├── expr_extended.py         # Extended $expr operators
├── expr_complete.py         # Complete $expr coverage
├── expr_success.py          # $expr success stories
├── update_operators.py      # Update operators (core)
├── update_additional.py     # Additional update operators
├── update_modifiers.py      # Update modifiers
├── aggregation_stages.py    # Aggregation stages (core)
├── aggregation_additional.py     # Additional aggregation
├── aggregation_stages_additional.py  # Additional stages
├── aggregation_stages_extended.py    # Extended stages
├── aggregation_cursor.py    # Aggregation cursor methods
├── array_operators.py       # Array operators
├── string_operators.py      # String operators
├── math_operators.py        # Math operators
├── date_operators.py        # Date operators
├── object_operators.py      # Object operators
├── collection_methods.py    # Collection methods
├── collection_methods_additional.py  # Additional collection methods
├── database_methods.py      # Database methods
├── cursor_operations.py     # Cursor operations
├── cursor_methods.py        # Cursor methods
├── index_operations.py      # Index operations
├── bulk_operations.py       # Bulk operations
├── bulk_executors.py        # Bulk operation executors
├── find_modify.py           # Find and modify
├── distinct.py              # Distinct operations
├── binary_operations.py     # Binary data support
├── nested_queries.py        # Nested field queries
├── raw_batches.py           # Raw batch operations
├── change_streams.py        # Change streams
├── text_search.py           # Text search
├── gridfs_operations.py     # GridFS operations
├── objectid_ops.py          # ObjectId operations
├── type_operator.py         # $type operator
├── mod_operator.py          # $mod operator
├── search_index.py          # Search index operations
├── reindex.py               # Reindex operations
├── elemmatch.py             # $elemMatch operator
└── MISSING-APIS.md          # This file
```

---

---

## Summary

| Category | Missing Count | Priority |
|----------|--------------|----------|
| Aggregation Operators | 45+ | High |
| Aggregation Stages | 3 | High |
| Update Operators | 5 | High |
| Query Operators | 2 | Medium |
| Collection Methods | 5+ | Medium |
| Database Methods | 5 | Medium |
| Special Features | 7+ | Low |
| **Total** | **72+** | - |

---

## 1. Aggregation Operators (45+ missing)

### 1.1 Array Transformation Operators ⭐ HIGH PRIORITY
- [x] `$filter` - Filters array based on condition (tested in array_operators.py)
- [x] `$map` - Transforms array elements (tested in array_operators.py)
- [x] `$reduce` - Reduces array to single value (tested in array_operators.py)
- [x] `$slice` - Returns subset of array (tested in array_operators.py)
- [x] `$indexOfArray` - Returns index of element in array (tested in array_operators.py)

### 1.2 Set Operators ⭐ HIGH PRIORITY
- [ ] `$setEquals` - Compares two arrays for set equality
- [ ] `$setIntersection` - Returns intersection of arrays
- [ ] `$setUnion` - Returns union of arrays
- [ ] `$setDifference` - Returns difference of arrays
- [ ] `$setIsSubset` - Checks if one array is subset of another
- [ ] `$anyElementTrue` - Returns true if any element is true
- [ ] `$allElementsTrue` - Returns true if all elements are true

### 1.3 String Operators ⭐ HIGH PRIORITY
- [x] `$trim` - Trims whitespace from both ends (tested in string_operators.py)
- [x] `$ltrim` - Trims whitespace from left (tested in string_operators.py)
- [x] `$rtrim` - Trims whitespace from right (tested in string_operators.py)
- [ ] `$indexOfBytes` - Returns byte position of substring
- [ ] `$indexOfCP` - Returns code point position of substring
- [x] `$regexFind` - Finds first regex match (tested in string_operators.py)
- [ ] `$regexFindAll` - Finds all regex matches
- [x] `$split` - Splits string by delimiter (tested in string_operators.py)
- [x] `$replaceAll` - Replaces all occurrences of substring (tested in string_operators.py)
- [x] `$replaceOne` - Replaces first occurrence of substring (tested in string_operators.py)
- [x] `$strLenCP` - Returns string length in code points (tested in string_operators.py)

### 1.4 Date Operators ⭐ HIGH PRIORITY
- [x] `$dateAdd` - Adds time to date (tested in date_operators.py)
- [x] `$dateSubtract` - Subtracts time from date (tested in date_operators.py)
- [x] `$dateDiff` - Calculates difference between dates (tested in date_operators.py)
- [x] `$week` - Returns week of year (tested in date_operators.py)
- [x] `$isoDayOfWeek` - Returns ISO day of week (tested in date_operators.py)
- [x] `$isoWeek` - Returns ISO week number (tested in date_operators.py)
- [x] `$millisecond` - Returns milliseconds (tested in date_operators.py)

### 1.5 Object Operators ⭐ MEDIUM PRIORITY
- [x] `$mergeObjects` - Merges multiple objects (tested in object_operators.py)
- [x] `$getField` - Gets field value from object (tested in object_operators.py)
- [x] `$setField` - Sets field value in object (tested in object_operators.py)
- [x] `$unsetField` - Removes field from object (tested in object_operators.py)
- [x] `$objectToArray` - Converts object to array (tested in object_operators.py)

### 1.6 Type Conversion Operators ⭐ MEDIUM PRIORITY
- [ ] `$toLong` - Converts to 64-bit integer
- [ ] `$toDecimal` - Converts to decimal
- [ ] `$toObjectId` - Converts to ObjectId
- [ ] `$toBinData` - Converts to Binary
- [ ] `$toRegex` - Converts to regex pattern
- [ ] `$convert` - General type conversion

### 1.7 Math Operators ⭐ MEDIUM PRIORITY
- [x] `$pow` - Raises a number to an exponent (tested in math_operators.py)
- [x] `$sqrt` - Calculates square root (tested in math_operators.py)
- [x] `$exp` - Calculates e^x (tested in math_operators.py)

### 1.8 Advanced Trigonometric Operators ⭐ LOW PRIORITY
- [x] `$asinh` - Inverse hyperbolic sine (tested in math_operators.py)
- [x] `$acosh` - Inverse hyperbolic cosine (tested in math_operators.py)
- [x] `$atanh` - Inverse hyperbolic tangent (tested in math_operators.py)

### 1.9 Angle Conversion Operators ⭐ LOW PRIORITY
- [x] `$degreesToRadians` - Converts degrees to radians (tested in math_operators.py)
- [x] `$radiansToDegrees` - Converts radians to degrees (tested in math_operators.py)

### 1.10 Conditional Operators ⭐ MEDIUM PRIORITY
- [x] `$switch` - Multi-branch conditional (tested in aggregation_additional.py)

### 1.11 NeoSQLite Extensions ⭐ LOW PRIORITY
- [x] `$log2` - Base-2 logarithm (NeoSQLite extension) (tested in math_operators.py)
- [x] `$sigmoid` - Sigmoid function (NeoSQLite extension) (tested in math_operators.py)

---

## 2. Aggregation Pipeline Stages (3 missing)

### 2.1 Core Stages ⭐ HIGH PRIORITY
- [ ] `$count` - Counts documents in pipeline
- [x] `$facet` - Creates multi-faceted aggregation (tested in aggregation_stages_additional.py)
- [x] `$unwind` - Unwinds array fields (tested in aggregation_additional.py)

---

## 3. Update Operators (5 missing)

### 3.1 Array Update Operators ⭐ HIGH PRIORITY
- [x] `$push` - Adds element to array (tested in update_additional.py)
- [x] `$addToSet` - Adds unique element to array (tested in update_additional.py)
- [x] `$pull` - Removes elements from array (tested in update_additional.py)
- [x] `$pop` - Removes first/last element from array (tested in update_additional.py)
- [x] `$currentDate` - Sets field to current date (tested in update_additional.py)

---

## 4. Query Operators (2 missing)

### 4.1 Logical & Text Search ⭐ MEDIUM PRIORITY
- [x] `$nor` - Logical NOR operation (tested in query_operators.py)
- [x] `$text` - Full-text search with FTS5 (tested in query_operators.py)

---

## 5. Collection Methods (5+ missing)

### 5.1 Collection APIs ⭐ MEDIUM PRIORITY
- [x] `Collection.options()` - Returns collection options/metadata (tested in collection_methods.py)
- [ ] `Collection.watch()` - Change streams (mentioned but skipped)
- [x] `Collection.rename()` - Renames collection (tested in collection_methods.py)

### 5.2 Cursor Methods
- [ ] `AggregationCursor.use_quez()` - Enable compressed queue processing

---

## 6. Database Methods (5 missing) ⭐ MEDIUM PRIORITY

- [x] `Connection.get_collection()` - Gets collection reference (tested in database_methods.py)
- [x] `Connection.create_collection()` - Creates new collection (tested in database_methods.py)
- [x] `Connection.list_collection_names()` - Lists all collections (tested in database_methods.py)
- [x] `Connection.drop_collection()` - Drops a collection (tested in database_methods.py)
- [x] `Connection.rename_collection()` - Renames a collection (tested in database_methods.py)

---

## 7. Special Features (7+ missing)

### 7.1 GridFS Enhanced Features ⭐ LOW PRIORITY
- [ ] `GridFSBucket.get_last_version()` - Get latest version of file
- [ ] `GridFSBucket.list()` - List all files
- [ ] `GridFSBucket.find_one()` - Find single file
- [ ] `GridFSBucket.get()` - Alias for open_download_stream
- [ ] Content type support - `content_type` field
- [ ] Aliases support - `aliases` field for files

### 7.2 Binary Data Support ⭐ LOW PRIORITY
- [ ] `Binary.from_uuid()` - Create Binary from UUID
- [ ] `Binary.as_uuid()` - Convert Binary to UUID
- [ ] Binary subtypes - Different binary subtypes (UUID, FUNCTION, etc.)

### 7.3 Advanced Features ⭐ LOW PRIORITY
- [ ] Custom FTS5 Tokenizers - Language-specific tokenizers (ICU, Thai, etc.)
- [ ] Memory-constrained processing with `use_quez()`

---

## Implementation Plan

### Phase 1: High Priority (Core Functionality)
1. Aggregation stages: `$count`, `$facet`, `$unwind`
2. Update operators: `$push`, `$addToSet`, `$pull`, `$pop`
3. Array operators: `$filter`, `$map`, `$reduce`, `$slice`
4. Set operators: `$setIntersection`, `$setUnion`, `$setDifference`
5. String operators: `$trim`, `$regexFind`, `$split`
6. Date operators: `$dateAdd`, `$dateSubtract`, `$dateDiff`

### Phase 2: Medium Priority (Advanced Features)
1. Object operators: `$mergeObjects`, `$getField`, `$setField`
2. Type conversion: `$convert`, `$toLong`, `$toDecimal`
3. Math operators: `$pow`, `$sqrt`, `$exp`
4. Conditional: `$switch`
5. Query operators: `$nor`, `$text`
6. Collection/Database methods

### Phase 3: Low Priority (Specialized/Extensions)
1. Advanced trigonometric functions
2. Angle conversion operators
3. NeoSQLite extensions (`$log2`, `$sigmoid`)
4. GridFS enhanced features
5. Binary data UUID methods
6. Custom tokenizers

---

## Notes

- Operators marked with ⭐ are prioritized by usage frequency and importance
- Some operators may have SQL tier limitations that should be documented
- NeoSQLite extensions should be tested but marked as skips when comparing with MongoDB
- Known limitations should be moved to the "Skipped Tests" section rather than failing

---

## Progress Tracking

### Phase 0: Refactoring (COMPLETED ✅)
- [x] Extracted 42 comparison functions into separate modules
- [x] Created package structure with reporter, runner, and utils
- [x] Updated main entry point to use the package
- [x] All modules import correctly

### Phase 1: High Priority (Core Functionality)
- [x] Aggregation stages: `$facet`, `$unwind` (partial - $count still missing)
- [x] Update operators: `$push`, `$addToSet`, `$pull`, `$pop`, `$currentDate`
- [x] Array operators: `$filter`, `$map`, `$reduce`, `$slice`, `$indexOfArray`
- [ ] Set operators: `$setIntersection`, `$setUnion`, `$setDifference`
- [x] String operators: `$trim`, `$ltrim`, `$rtrim`, `$regexFind`, `$split`, `$replaceAll`, `$replaceOne`, `$strLenCP`
- [x] Date operators: `$dateAdd`, `$dateSubtract`, `$dateDiff`, `$week`, `$isoDayOfWeek`, `$isoWeek`, `$millisecond`

### Phase 2: Medium Priority (Advanced Features)
- [x] Object operators: `$mergeObjects`, `$getField`, `$setField`, `$unsetField`, `$objectToArray`
- [ ] Type conversion: `$convert`, `$toLong`, `$toDecimal`
- [x] Math operators: `$pow`, `$sqrt`, `$exp`
- [x] Conditional: `$switch`
- [x] Query operators: `$nor`, `$text`
- [x] Collection/Database methods

### Phase 3: Low Priority (Specialized/Extensions)
- [x] Advanced trigonometric functions ($asinh, $acosh, $atanh)
- [x] Angle conversion operators ($degreesToRadians, $radiansToDegrees)
- [x] NeoSQLite extensions (`$log2`, `$sigmoid`)
- [ ] GridFS enhanced features
- [ ] Binary data UUID methods
- [ ] Custom tokenizers

---

## Summary

- **Total Missing**: ~25
- **Added**: ~47
- **Remaining**: ~25
- **Coverage**: ~65%

### Next Steps

1. Choose a category from Phase 1
2. Add the missing operator tests to the appropriate module
3. Update this document and mark as completed
4. Run the comparison script to verify
