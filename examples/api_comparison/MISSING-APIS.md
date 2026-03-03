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

## Latest Update: March 2, 2026

**Test Results Summary:**
- **Total Tests Run**: 266
- **Passed**: 262
- **Skipped**: 4 (known limitations/not implemented)
- **Failed**: 0
- **Compatibility**: **100.0%**

---

## Summary

| Category | Missing Count | Priority | Status |
|----------|--------------|----------|--------|
| Aggregation Operators | 0 | - | ✅ Complete |
| Aggregation Stages | 0 | - | ✅ Complete |
| Update Operators | 0 | - | ✅ Complete |
| Query Operators | 0 | - | ✅ Complete |
| Collection Methods | 1 | Low | Mostly Complete |
| Database Methods | 0 | - | ✅ Complete |
| Special Features | 0 | - | ✅ Complete |
| **Total** | **~1** | - | **~99.6% Coverage** |

---

## 1. Aggregation Operators (✅ Complete)

### 1.1 Array Transformation Operators ⭐ HIGH PRIORITY
- [x] `$filter` - Filters array based on condition (tested in array_operators.py)
- [x] `$map` - Transforms array elements (tested in array_operators.py)
- [x] `$reduce` - Reduces array to single value (tested in array_operators.py)
- [x] `$slice` - Returns subset of array (tested in array_operators.py)
- [x] `$indexOfArray` - Returns index of element in array (tested in array_operators.py)

### 1.2 Set Operators ⭐ HIGH PRIORITY
- [x] `$setEquals` - Compares two arrays for set equality (tested in expr_complete.py)
- [x] `$setIntersection` - Returns intersection of arrays (tested in expr_complete.py)
- [x] `$setUnion` - Returns union of arrays (tested in expr_complete.py)
- [x] `$setDifference` - Returns difference of arrays (tested in expr_complete.py)
- [x] `$setIsSubset` - Checks if one array is subset of another (tested in expr_complete.py)
- [x] `$anyElementTrue` - Returns true if any element is true (tested in expr_complete.py)
- [x] `$allElementsTrue` - Returns true if all elements are true (tested in expr_complete.py)

### 1.3 String Operators ⭐ HIGH PRIORITY
- [x] `$trim` - Trims whitespace from both ends (tested in string_operators.py)
- [x] `$ltrim` - Trims whitespace from left (tested in string_operators.py)
- [x] `$rtrim` - Trims whitespace from right (tested in string_operators.py)
- [x] `$indexOfBytes` - Returns byte position of substring (tested in expr_complete.py)
- [x] `$indexOfCP` - Returns code point position of substring (tested in string_operators.py)
- [x] `$regexFind` - Finds first regex match (tested in string_operators.py)
- [x] `$regexFindAll` - Finds all regex matches (tested in string_operators.py)
- [x] `$regexMatch` - Tests regex match (tested in expr_complete.py)
- [x] `$split` - Splits string by delimiter (tested in string_operators.py)
- [x] `$replaceAll` - Replaces all occurrences of substring (tested in string_operators.py)
- [x] `$replaceOne` - Replaces first occurrence of substring (tested in string_operators.py)
- [x] `$strLenCP` - Returns string length in code points (tested in string_operators.py)
- [x] `$strLenBytes` - Returns string length in bytes (tested in expr.py)

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
- [x] `$toString` - Converts to string (tested in expr.py)
- [x] `$toInt` - Converts to integer (tested in expr.py)
- [x] `$toDouble` - Converts to double (tested in expr.py)
- [x] `$toBool` - Converts to boolean (tested in expr.py)
- [x] `$toUpper` - Converts to uppercase (tested in expr.py)
- [x] `$toLower` - Converts to lowercase (tested in expr.py)
- [x] `$toLong` - Converts to 64-bit integer (tested in expr_complete.py)
- [x] `$toDecimal` - Converts to decimal (tested in expr_complete.py)
- [x] `$toObjectId` - Converts to ObjectId (tested in expr_complete.py)
- [x] `$toBinData` - Converts to Binary (tested in expr_complete.py)
- [x] `$toRegex` - Converts to regex pattern (tested in expr_complete.py)
- [x] `$convert` - General type conversion (tested in expr_complete.py)

### 1.7 Math Operators ⭐ MEDIUM PRIORITY
- [x] `$pow` - Raises a number to an exponent (tested in math_operators.py)
- [x] `$sqrt` - Calculates square root (tested in math_operators.py)
- [x] `$exp` - Calculates e^x (tested in math_operators.py)
- [x] `$abs` - Absolute value (tested in expr.py)
- [x] `$ceil` - Ceiling (tested in expr.py)
- [x] `$floor` - Floor (tested in expr.py)
- [x] `$trunc` - Truncate (tested in expr.py)
- [x] `$ln` - Natural logarithm (tested in expr.py)
- [x] `$log` - Logarithm (tested in expr.py)
- [x] `$log10` - Base-10 logarithm (tested in expr.py)
- [x] `$round` - Round (tested in expr_success.py)

### 1.8 Advanced Trigonometric Operators ⭐ LOW PRIORITY
- [x] `$sin` - Sine (tested in expr.py)
- [x] `$cos` - Cosine (tested in expr.py)
- [x] `$tan` - Tangent (tested in expr.py)
- [x] `$asin` - Inverse sine (tested in expr.py)
- [x] `$acos` - Inverse cosine (tested in expr.py)
- [x] `$atan` - Inverse tangent (tested in expr.py)
- [x] `$atan2` - Two-argument inverse tangent (tested in expr.py)
- [x] `$asinh` - Inverse hyperbolic sine (tested in math_operators.py)
- [x] `$acosh` - Inverse hyperbolic cosine (tested in math_operators.py)
- [x] `$atanh` - Inverse hyperbolic tangent (tested in math_operators.py)
- [x] `$sinh` - Hyperbolic sine (tested in expr.py)
- [x] `$cosh` - Hyperbolic cosine (tested in expr.py)
- [x] `$tanh` - Hyperbolic tangent (tested in expr.py)

### 1.9 Angle Conversion Operators ⭐ LOW PRIORITY
- [x] `$degreesToRadians` - Converts degrees to radians (tested in math_operators.py)
- [x] `$radiansToDegrees` - Converts radians to degrees (tested in math_operators.py)

### 1.10 Conditional Operators ⭐ MEDIUM PRIORITY
- [x] `$switch` - Multi-branch conditional (tested in aggregation_additional.py)
- [x] `$cond` - If-then-else conditional (tested in expr.py)
- [x] `$ifNull` - If-null conditional (tested in expr_extended.py)

### 1.11 NeoSQLite Extensions ⭐ LOW PRIORITY
- [x] `$log2` - Base-2 logarithm (NeoSQLite extension) (tested in math_operators.py)
- [x] `$sigmoid` - Sigmoid function (NeoSQLite extension) (tested in math_operators.py)

### 1.12 Other Expression Operators ⭐ MEDIUM PRIORITY
- [x] `$cmp` - Compare two values (tested in expr_extended.py)
- [x] `$concat` - Concatenate strings (tested in expr_additional.py)
- [x] `$arrayElemAt` - Get element at index from array (tested in expr_additional.py)
- [x] `$literal` - Return literal value (tested in expr_complete.py)
- [x] `$isArray` - Check if value is array (tested in expr_success.py)
- [x] `$type` - Get type of value (tested in expr.py)

---

## 2. Aggregation Pipeline Stages (✅ Complete)

### 2.1 Core Stages ⭐ HIGH PRIORITY
- [x] `$count` - Counts documents in pipeline (tested in aggregation_extended.py)
- [x] `$facet` - Creates multi-faceted aggregation (tested in aggregation_stages_additional.py)
- [x] `$unwind` - Unwinds array fields (tested in aggregation_additional.py)

### 2.2 Additional Stages ⭐ MEDIUM PRIORITY
- [x] `$match` - Filters documents (tested in aggregation_stages.py)
- [x] `$project` - Projects fields (tested in aggregation_stages.py)
- [x] `$addFields` - Adds new fields (tested in aggregation_stages.py)
- [x] `$group` - Groups documents (tested in aggregation_stages.py)
- [x] `$sort` - Sorts documents (tested in aggregation_stages.py)
- [x] `$skip` - Skips documents (tested in aggregation_stages.py)
- [x] `$limit` - Limits documents (tested in aggregation_stages.py)
- [x] `$sample` - Random sample (tested in aggregation_stages_additional.py)
- [x] `$lookup` - Left outer join (tested in aggregation_stages_additional.py)
- [x] `$replaceRoot` - Replace root document (tested in aggregation_extended.py)
- [x] `$replaceWith` - Replace with expression (tested in aggregation_extended.py)
- [x] `$unset` - Remove fields (tested in aggregation_extended.py)

---

## 3. Update Operators (4 missing - modifiers)

### 3.1 Core Update Operators ⭐ HIGH PRIORITY
- [x] `$set` - Sets field value (tested in update_operators.py)
- [x] `$unset` - Removes field (tested in update_operators.py)
- [x] `$inc` - Increments field (tested in update_operators.py)
- [x] `$mul` - Multiplies field (tested in update_operators.py)
- [x] `$min` - Minimum value (tested in update_operators.py)
- [x] `$max` - Maximum value (tested in update_operators.py)
- [x] `$rename` - Renames field (tested in update_operators.py)
- [x] `$setOnInsert` - Sets on insert only (tested in update_operators.py)

### 3.2 Array Update Operators ⭐ HIGH PRIORITY
- [x] `$push` - Adds element to array (tested in update_additional.py)
- [x] `$addToSet` - Adds unique element to array (tested in update_additional.py)
- [x] `$pull` - Removes elements from array (tested in update_additional.py)
- [x] `$pop` - Removes first/last element from array (tested in update_additional.py)
- [x] `$currentDate` - Sets field to current date (tested in update_additional.py)

### 3.3 Update Modifiers ✅ NOW IMPLEMENTED
- [x] `$push $each` - Add multiple elements to array (tested in update_modifiers.py)
- [x] `$push $position` - Add element at specific position (tested in update_modifiers.py)
- [x] `$push $slice` - Slice array after push (tested in update_modifiers.py)
- [x] `$bit` - Bitwise update operator (tested in update_modifiers.py)

---

## 4. Query Operators (✅ Complete)

### 4.1 Comparison Operators ⭐ HIGH PRIORITY
- [x] `$eq` - Equals (tested in expr.py)
- [x] `$ne` - Not equals (tested in expr.py)
- [x] `$gt` - Greater than (tested in expr.py)
- [x] `$gte` - Greater than or equals (tested in expr.py)
- [x] `$lt` - Less than (tested in expr.py)
- [x] `$lte` - Less than or equals (tested in expr.py)

### 4.2 Logical & Text Search ⭐ MEDIUM PRIORITY
- [x] `$and` - Logical AND (tested in expr.py)
- [x] `$or` - Logical OR (tested in expr.py)
- [x] `$nor` - Logical NOR (tested in query_operators.py)
- [x] `$not` - Logical NOT (tested in expr_success.py)
- [x] `$text` - Full-text search with FTS5 (tested in query_operators.py)

### 4.3 Other Query Operators ⭐ MEDIUM PRIORITY
- [x] `$mod` - Modulo operator (tested in mod_operator.py)
- [x] `$elemMatch` - Array element match (tested in elemmatch.py)
- [x] `$type` - Type operator (tested in type_operator.py)

---

## 5. Collection Methods (2 missing)

### 5.1 Collection APIs ⭐ MEDIUM PRIORITY
- [x] `Collection.find()` - Find documents (tested in crud.py)
- [x] `Collection.find_one()` - Find single document (tested in crud.py)
- [x] `Collection.insert_one()` - Insert single document (tested in crud.py)
- [x] `Collection.insert_many()` - Insert multiple documents (tested in crud.py)
- [x] `Collection.update_one()` - Update single document (tested in crud.py)
- [x] `Collection.update_many()` - Update multiple documents (tested in crud.py)
- [x] `Collection.replace_one()` - Replace single document (tested in crud.py)
- [x] `Collection.delete_one()` - Delete single document (tested in crud.py)
- [x] `Collection.delete_many()` - Delete multiple documents (tested in crud.py)
- [x] `Collection.count_documents()` - Count documents (tested in crud.py)
- [x] `Collection.estimated_document_count()` - Estimated count (tested in crud.py)
- [x] `Collection.options()` - Returns collection options/metadata (tested in collection_methods.py)
- [x] `Collection.rename()` - Renames collection (tested in collection_methods.py)
- [x] `Collection.drop()` - Drops collection (tested in collection_additional.py)
- [x] `Collection.database` - Database property (tested in collection_additional.py)
- [ ] `Collection.watch()` - Change streams (tested - skipped, requires replica set)

### 5.2 Cursor Methods ⭐ MEDIUM PRIORITY
- [x] `Cursor.limit()` - Limit results (tested in cursor.py)
- [x] `Cursor.skip()` - Skip results (tested in cursor.py)
- [x] `Cursor.sort()` - Sort results (tested in cursor.py)
- [x] `Cursor.count()` - Count results (tested in cursor.py)
- [x] `Cursor.batch_size()` - Batch size (tested in cursor_methods.py)
- [x] `Cursor.hint()` - Index hint (tested in cursor_methods.py)
- [x] `Cursor.__next__()` - Iterator (tested in cursor.py)
- [x] `Cursor.__iter__()` - Iterator (tested in cursor.py)
- [x] `AggregationCursor.batch_size()` - Aggregation batch size (tested in aggregation_cursor.py)
- [x] `AggregationCursor.allow_disk_use()` - Allow disk use (tested in aggregation_cursor.py)

---

## 6. Database Methods (✅ Complete)

- [x] `Connection.get_collection()` - Gets collection reference (tested in database_methods.py)
- [x] `Connection.create_collection()` - Creates new collection (tested in database_methods.py)
- [x] `Connection.list_collection_names()` - Lists all collections (tested in database_methods.py)
- [x] `Connection.drop_collection()` - Drops a collection (tested in database_methods.py)
- [x] `Connection.rename_collection()` - Renames a collection (tested in database_methods.py)
- [x] `Connection.create_database()` - Creates database (via collection operations)
- [x] `Connection.drop_database()` - Drops database (via collection.drop())

---

## 7. Special Features (7+ missing)

### 7.1 Index Operations ⭐ HIGH PRIORITY
- [x] `Collection.create_index()` - Create index (tested in index_operations.py)
- [x] `Collection.create_indexes()` - Create multiple indexes (tested in index_operations.py)
- [x] `Collection.list_indexes()` - List indexes (tested in index_operations.py)
- [x] `Collection.index_information()` - Get index info (tested in index_operations.py)
- [x] `Collection.drop_index()` - Drop index (tested in index_operations.py)
- [x] `Collection.drop_indexes()` - Drop all indexes (tested in index_operations.py)
- [x] `Collection.reindex()` - Reindex collection (tested in reindex.py)
- [x] `Collection.create_search_index()` - Create search index (tested in search_index.py)
- [x] `Collection.list_search_indexes()` - List search indexes (tested in search_index.py)
- [x] `Collection.update_search_index()` - Update search index (tested in search_index.py)
- [x] `Collection.drop_search_index()` - Drop search index (tested in search_index.py)

### 7.2 Find and Modify Operations ⭐ HIGH PRIORITY
- [x] `Collection.find_one_and_delete()` - Find and delete (tested in find_modify.py)
- [x] `Collection.find_one_and_replace()` - Find and replace (tested in find_modify.py)
- [x] `Collection.find_one_and_update()` - Find and update (tested in find_modify.py)

### 7.3 Bulk Operations ⭐ HIGH PRIORITY
- [x] `Collection.bulk_write()` - Bulk write operations (tested in bulk_operations.py)
- [x] `Collection.initialize_ordered_bulk_op()` - Ordered bulk (tested in bulk_executors.py)
- [x] `Collection.initialize_unordered_bulk_op()` - Unordered bulk (tested in bulk_executors.py)

### 7.4 GridFS Operations ⭐ LOW PRIORITY ✅ COMPLETE
- [x] `GridFSBucket.upload_from_stream()` - Upload file (tested in gridfs_operations.py)
- [x] `GridFSBucket.download_to_stream()` - Download file (tested in gridfs_operations.py)
- [x] `GridFSBucket.find()` - Find files (tested in gridfs_operations.py)
- [x] `GridFSBucket.delete()` - Delete file (tested in gridfs_operations.py)
- [x] `GridFSBucket.get_last_version()` - Get latest version of file (tested in gridfs_operations.py)
- [x] `GridFSBucket.list()` - List all files (tested in gridfs_operations.py)
- [x] `GridFSBucket.find_one()` - Find single file (tested in gridfs_operations.py)
- [x] `GridFSBucket.get()` - Alias for open_download_stream (tested in gridfs_operations.py)
- [x] Content type support - `content_type` field (tested in gridfs_operations.py)
- [x] Aliases support - `aliases` field for files (tested in gridfs_operations.py)

### 7.5 Binary Data Support ⭐ LOW PRIORITY ✅ COMPLETE
- [x] `Binary()` - Create Binary object (tested in binary_operations.py)
- [x] `Binary.from_uuid()` - Create Binary from UUID (tested in binary_operations.py)
- [x] `Binary.as_uuid()` - Convert Binary to UUID (tested in binary_operations.py)
- [x] Binary subtypes - Different binary subtypes (UUID, FUNCTION, etc.) (tested in binary_operations.py)

### 7.6 ObjectId Operations ⭐ MEDIUM PRIORITY
- [x] `ObjectId()` - Create ObjectId (tested in objectid_ops.py)
- [x] `ObjectId.is_valid()` - Validate ObjectId (tested in objectid_ops.py)
- [x] `ObjectId.generation_time` - Get generation time (tested in objectid_ops.py)
- [x] `ObjectId.__str__()` - Hex string representation (tested in objectid_ops.py)

---

## Implementation Plan

### Phase 1: High Priority (Core Functionality) ✅ COMPLETED
- [x] Aggregation stages: `$count`, `$facet`, `$unwind`
- [x] Update operators: `$push`, `$addToSet`, `$pull`, `$pop`, `$currentDate`
- [x] Array operators: `$filter`, `$map`, `$reduce`, `$slice`, `$indexOfArray`
- [x] Set operators: `$setEquals`, `$setIntersection`, `$setUnion`, `$setDifference`, `$setIsSubset`, `$anyElementTrue`, `$allElementsTrue`
- [x] String operators: `$trim`, `$ltrim`, `$rtrim`, `$regexFind`, `$regexMatch`, `$split`, `$replaceAll`, `$replaceOne`, `$strLenCP`, `$strLenBytes`, `$indexOfBytes`
- [x] Date operators: `$dateAdd`, `$dateSubtract`, `$dateDiff`, `$week`, `$isoDayOfWeek`, `$isoWeek`, `$millisecond`

### Phase 2: Medium Priority (Advanced Features) ✅ COMPLETED
- [x] Object operators: `$mergeObjects`, `$getField`, `$setField`, `$unsetField`, `$objectToArray`
- [x] Type conversion: `$toString`, `$toInt`, `$toDouble`, `$toBool`, `$toUpper`, `$toLower`
- [x] Math operators: `$pow`, `$sqrt`, `$exp`, `$abs`, `$ceil`, `$floor`, `$trunc`, `$ln`, `$log`, `$log10`, `$round`
- [x] Conditional: `$switch`, `$cond`, `$ifNull`
- [x] Query operators: `$nor`, `$text`, `$mod`, `$elemMatch`
- [x] Collection/Database methods
- [x] Expression operators: `$cmp`, `$concat`, `$arrayElemAt`, `$literal`, `$isArray`, `$type`

### Phase 3: Low Priority (Specialized/Extensions) ✅ COMPLETED
- [x] Advanced trigonometric functions ($sin, $cos, $tan, $asin, $acos, $atan, $atan2, $sinh, $cosh, $tanh)
- [x] Inverse hyperbolic functions ($asinh, $acosh, $atanh)
- [x] Angle conversion operators ($degreesToRadians, $radiansToDegrees)
- [x] NeoSQLite extensions (`$log2`, `$sigmoid`)
- [x] Additional aggregation stages ($sample, $lookup, $replaceRoot, $replaceWith, $unset)
- [ ] GridFS enhanced features (partially complete)
- [ ] Binary data UUID methods
- [ ] Custom tokenizers

---

## Notes

- Operators marked with ⭐ are prioritized by usage frequency and importance
- Some operators may have SQL tier limitations that should be documented
- NeoSQLite extensions should be tested but marked as skips when comparing with MongoDB
- Known limitations should be moved to the "Skipped Tests" section rather than failing

### Known Limitations (Skipped Tests)

The following features are skipped during comparison testing due to architectural differences:

| Feature | Reason |
|---------|--------|
| `watch` (Change Streams) | Requires MongoDB replica set; NeoSQLite uses SQLite triggers |
| `$push $each` | Not yet implemented in NeoSQLite |
| `$push $position` | Not yet implemented in NeoSQLite |
| `$push $slice` | Not yet implemented in NeoSQLite |
| `$bit` (update modifier) | Not yet implemented in NeoSQLite |
| `$log2` | NeoSQLite extension (not in MongoDB) |

---

## Progress Tracking

### Phase 0: Refactoring (COMPLETED ✅)
- [x] Extracted 42 comparison functions into separate modules
- [x] Created package structure with reporter, runner, and utils
- [x] Updated main entry point to use the package
- [x] All modules import correctly

### Phase 1: High Priority (Core Functionality) (COMPLETED ✅)
- [x] Aggregation stages: `$count`, `$facet`, `$unwind`
- [x] Update operators: `$push`, `$addToSet`, `$pull`, `$pop`, `$currentDate`
- [x] Array operators: `$filter`, `$map`, `$reduce`, `$slice`, `$indexOfArray`
- [x] Set operators: `$setIntersection`, `$setUnion`, `$setDifference`, `$setIsSubset`, `$anyElementTrue`, `$allElementsTrue`, `$setEquals`
- [x] String operators: `$trim`, `$ltrim`, `$rtrim`, `$regexFind`, `$split`, `$replaceAll`, `$replaceOne`, `$strLenCP`, `$indexOfBytes`, `$regexMatch`, `$strLenBytes`
- [x] Date operators: `$dateAdd`, `$dateSubtract`, `$dateDiff`, `$week`, `$isoDayOfWeek`, `$isoWeek`, `$millisecond`

### Phase 2: Medium Priority (Advanced Features) (COMPLETED ✅)
- [x] Object operators: `$mergeObjects`, `$getField`, `$setField`, `$unsetField`, `$objectToArray`
- [x] Type conversion: `$toString`, `$toInt`, `$toDouble`, `$toBool`, `$toUpper`, `$toLower`, `$toLong`, `$toDecimal`, `$toObjectId`, `$toBinData`, `$toRegex`, `$convert`
- [x] Math operators: `$pow`, `$sqrt`, `$exp`, `$abs`, `$ceil`, `$floor`, `$trunc`, `$ln`, `$log`, `$log10`, `$round`
- [x] Conditional: `$switch`, `$cond`, `$ifNull`
- [x] Query operators: `$nor`, `$text`, `$mod`, `$elemMatch`
- [x] Collection/Database methods
- [x] String operators: `$indexOfCP`, `$regexFindAll`

### Phase 3: Low Priority (Specialized/Extensions) (COMPLETED ✅)
- [x] Advanced trigonometric functions ($sin, $cos, $tan, $asin, $acos, $atan, $atan2, $sinh, $cosh, $tanh)
- [x] Inverse hyperbolic functions ($asinh, $acosh, $atanh)
- [x] Angle conversion operators ($degreesToRadians, $radiansToDegrees)
- [x] NeoSQLite extensions (`$log2`, `$sigmoid`)
- [ ] GridFS enhanced features (partially complete)
- [ ] Binary data UUID methods
- [ ] Custom tokenizers

---

## Current Status (March 2, 2026)

### Test Results Summary

| Metric | Value |
|--------|-------|
| **Total Tests** | 266 |
| **Passed** | 262 |
| **Skipped** | 4 |
| **Failed** | 0 |
| **Compatibility** | **100.0%** |

### Remaining Work

| Category | Missing Items | Priority |
|----------|---------------|----------|
| Collection Methods | `Collection.watch()` | Low |

**Total Remaining**: 1 feature

### Next Steps

1. **Change Streams** (Low Priority): Implement `Collection.watch()` - Requires SQLite triggers for NeoSQLite, MongoDB replica set for PyMongo
