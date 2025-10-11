# NeoSQLite Geospatial API Implementation Plan

## Overview

This document outlines the implementation plan for adding geospatial capabilities to NeoSQLite using SpatiaLite with a trigger-based architecture. The approach creates a separate geospatial table with triggers to maintain synchronization with the main document table, providing optimal performance while keeping the main table clean.

## Architecture Design

### Trigger-Based Approach

The implementation will use a separate geospatial table with triggers to automatically maintain synchronization:

```sql
-- Main document table (unchanged)
CREATE TABLE collection_name (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    _id TEXT,
    data JSONB NOT NULL
);

-- Dedicated geospatial table
CREATE TABLE collection_name__geospatial (
    id INTEGER PRIMARY KEY,
    field_name TEXT NOT NULL,         -- Name of field containing geometry in JSON
    geometry GEOMETRY NOT NULL,       -- Extracted geometry in SpatiaLite format
    FOREIGN KEY (id) REFERENCES collection_name(id) ON DELETE CASCADE
);

-- Spatial index on geospatial table
CREATE VIRTUAL TABLE idx_collection_name_geometry 
USING rtree(id, minx, maxx, miny, maxy);
```

### Trigger Implementation

```sql
-- Trigger for INSERT operations
CREATE TRIGGER collection_name__geospatial_insert 
AFTER INSERT ON collection_name
BEGIN
    INSERT INTO collection_name__geospatial (id, field_name, geometry)
    SELECT 
        NEW.id,
        'location',  -- or dynamically determined from document
        ST_GeomFromText(
            CASE 
                WHEN json_extract(NEW.data, '$.location.type') = 'Point' 
                THEN 'POINT(' || json_extract(NEW.data, '$.location.coordinates[0]') || ' ' || 
                     json_extract(NEW.data, '$.location.coordinates[1]') || ')'
                ELSE NULL
            END
        )
    WHERE json_extract(NEW.data, '$.location') IS NOT NULL
       AND json_extract(NEW.data, '$.location.type') = 'Point';
END;
```

## Implementation Phases

### Phase 1: SpatiaLite Integration and Infrastructure (P0 - Critical)

#### 1.1 SpatiaLite Extension Loading
- **Status**: Not Started
- **Effort**: Medium
- **Impact**: Critical
- **Dependencies**: Connection class modifications

**Tasks:**
- Modify `Connection.__init__()` to load SpatiaLite extension
- Add error handling for systems without SpatiaLite support
- Initialize SpatiaLite metadata if needed
- Add SpatiaLite availability check in initialization

**Implementation Location:** `neosqlite/connection.py`

#### 1.2 Geospatial Table Creation
- **Status**: Not Started
- **Effort**: Medium
- **Impact**: Critical
- **Dependencies**: SpatiaLite extension loading

**Tasks:**
- Modify collection creation to include geospatial companion table
- Add R-Tree spatial index creation
- Create foreign key relationship with main table
- Handle table existence checks and migrations

**Implementation Location:** `neosqlite/collection/__init__.py`

#### 1.3 Utility Functions for Geometry Processing
- **Status**: Not Started
- **Effort**: Medium
- **Impact**: Critical
- **Dependencies**: SpatiaLite extension

**Tasks:**
- Create functions to extract geometry from JSON documents
- Convert GeoJSON to SpatiaLite WKT format
- Handle coordinate system transformations
- Validate geometry data before insertion

**Implementation Location:** `neosqlite/collection/geo_helper.py`

### Phase 2: Trigger Implementation (P1 - High)

#### 2.1 INSERT Trigger
- **Status**: Not Started
- **Effort**: High
- **Impact**: Critical
- **Dependencies**: Phase 1 completion

**Tasks:**
- Create trigger that detects geometry fields in new documents
- Extract and convert geometry to SpatiaLite format
- Handle multiple geometry fields in single document
- Support for Point, LineString, Polygon geometries

**Implementation Location:** SQL executed during collection creation

#### 2.2 UPDATE Trigger
- **Status**: Not Started
- **Effort**: High
- **Impact**: Critical
- **Dependencies**: INSERT trigger implementation

**Tasks:**
- Create trigger for document updates
- Handle geometry field modifications
- Clean up old geometry records when fields are removed
- Maintain spatial index integrity

**Implementation Location:** SQL executed during collection creation

#### 2.3 DELETE Trigger
- **Status**: Not Started
- **Effort**: Low
- **Impact**: High
- **Dependencies**: Foreign key CASCADE (already handled)

**Tasks:**
- Verify CASCADE deletion works properly
- Ensure spatial indexes are maintained after deletes
- Handle cleanup of geometry records

**Implementation Location:** SQL executed during collection creation

### Phase 3: PyMongo Geospatial Operator Support (P1 - High)

#### 3.1 `$near` Operator
- **Status**: Not Started
- **Effort**: High
- **Impact**: High
- **Dependencies**: Trigger-based architecture

**Tasks:**
- Modify SQL translation to handle `$near` queries
- Convert GeoJSON Point to SpatiaLite format for queries
- Implement distance calculations using ST_Distance
- Support `$maxDistance` and `$minDistance`

**Implementation Location:** `neosqlite/collection/sql_translator_unified.py`

#### 3.2 `$geoWithin` Operator
- **Status**: Not Started
- **Effort**: High
- **Impact**: High
- **Dependencies**: Trigger-based architecture

**Tasks:**
- Implement `$geoWithin` queries using ST_Within
- Support for `$box`, `$polygon`, `$center`, `$centerSphere`
- Handle complex polygon geometries
- Optimize queries using spatial indexes

**Implementation Location:** `neosqlite/collection/sql_translator_unified.py`

#### 3.3 `$geoIntersects` Operator
- **Status**: Not Started
- **Effort**: High
- **Impact**: High
- **Dependencies**: Trigger-based architecture

**Tasks:**
- Implement `$geoIntersects` using ST_Intersects
- Handle various geometry type intersections
- Optimize performance with spatial indexing

**Implementation Location:** `neosqlite/collection/sql_translator_unified.py`

#### 3.4 `$nearSphere` Operator
- **Status**: Not Started
- **Effort**: High
- **Impact**: Medium
- **Dependencies**: `$near` operator implementation

**Tasks:**
- Implement spherical distance calculations
- Use appropriate projection for Earth surface calculations
- Handle coordinate system transformations

**Implementation Location:** `neosqlite/collection/sql_translator_unified.py`

### Phase 4: Index Management (P2 - Medium)

#### 4.1 Spatial Index Creation Methods
- **Status**: Not Started
- **Effort**: Medium
- **Impact**: High
- **Dependencies**: Core geospatial infrastructure

**Tasks:**
- `Collection.create_spatial_index()` - Create spatial indexes on geometry fields
- `Collection.drop_spatial_index()` - Drop spatial indexes
- `Collection.list_spatial_indexes()` - List existing spatial indexes

**Implementation Location:** `neosqlite/collection/index_manager.py`

#### 4.2 Compound Index Support
- **Status**: Not Started
- **Effort**: High
- **Impact**: Medium
- **Dependencies**: Basic spatial index implementation

**Tasks:**
- Support for compound indexes mixing regular and spatial fields
- Automatic index selection for mixed queries
- Performance optimization for compound spatial queries

**Implementation Location:** `neosqlite/collection/index_manager.py`

### Phase 5: Advanced Features (P2 - Medium)

#### 5.1 Geometry Validation
- **Status**: Not Started
- **Effort**: Medium
- **Impact**: Medium
- **Dependencies**: Core geospatial infrastructure

**Tasks:**
- Validate geometry data before insertion
- Support for geometry validation options
- Error handling for invalid geometry

**Implementation Location:** `neosqlite/collection/geo_helper.py`

#### 5.2 Coordinate System Support
- **Status**: Not Started
- **Effort**: High
- **Impact**: Medium
- **Dependencies**: Basic geometry processing

**Tasks:**
- Handle coordinate system transformations
- Support for different SRIDs (Spatial Reference ID)
- Proper handling of longitude/latitude order

**Implementation Location:** `neosqlite/collection/geo_helper.py`

### Phase 6: Aggregation Support (P3 - Low)

#### 6.1 Geospatial Aggregation Operators
- **Status**: Not Started
- **Effort**: High
- **Impact**: Low
- **Dependencies**: Core geospatial operators

**Tasks:**
- Support geospatial operators in aggregation pipelines
- Implement `$geoNear` aggregation stage
- Handle geospatial grouping and other advanced operations

**Implementation Location:** `neosqlite/collection/query_engine.py`

## Implementation Timeline

### Phase 1 (Weeks 1-3): Infrastructure Setup
- SpatiaLite extension integration
- Geospatial table creation
- Geometry processing utilities

### Phase 2 (Weeks 4-6): Trigger Implementation
- INSERT, UPDATE, DELETE triggers
- Geometry extraction and validation
- Spatial index creation

### Phase 3 (Weeks 7-10): Core Query Operators
- `$near`, `$geoWithin`, `$geoIntersects` operators
- Query translation and optimization
- Distance calculation support

### Phase 4 (Weeks 11-12): Index Management
- Spatial index creation methods
- Compound index support

### Phase 5 (Weeks 13-14): Advanced Features
- Geometry validation
- Coordinate system transformations

### Phase 6 (Weeks 15+): Aggregation Support
- Geospatial aggregation operators
- Performance optimization

## Success Metrics

1. **API Compatibility**: Full support for PyMongo geospatial operators
2. **Performance**: Spatial queries use R-Tree indexes for optimal performance
3. **Backward Compatibility**: Existing code continues to work unchanged
4. **Data Integrity**: Triggers maintain synchronization between tables
5. **Error Handling**: Graceful degradation when SpatiaLite not available

## Risk Mitigation

1. **Extension Availability**: Check for SpatiaLite availability and provide fallbacks
2. **Performance Impact**: Benchmark trigger overhead and optimize
3. **Complexity**: Implement features incrementally with thorough testing
4. **Dependencies**: Keep external dependencies minimal

## Dependencies and Prerequisites

### External Dependencies
- **SpatiaLite Extension**: Required for full geospatial functionality
- **SQLite Version**: Minimum 3.35.0 for best extension support

### Internal Dependencies
- **Existing Architecture**: Will extend current query translation system
- **JSON Support**: Leverage existing JSON1/JSONB functionality
- **Query Engine**: Integrate with existing query processing pipeline

### Compatibility Requirements
- **PyMongo API Compatibility**: Maintain full PyMongo geospatial API compatibility
- **SQLite Version Compatibility**: Graceful degradation without geospatial features
- **Python Version Compatibility**: Support for current Python versions

## Testing Strategy

### Unit Tests
- Individual operator testing (`$near`, `$geoWithin`, etc.)
- Geometry conversion validation
- Trigger functionality verification

### Integration Tests
- Full PyMongo geospatial API compatibility tests
- Performance benchmarking against expected results
- Edge case testing (invalid geometry, coordinate transformations)

### Performance Tests
- Spatial index utilization verification
- Query performance comparison with raw SpatiaLite
- Memory usage during geospatial operations

## Documentation Requirements

1. **API Documentation**: Update with new geospatial methods and operators
2. **Implementation Guide**: Document trigger-based architecture
3. **Performance Guide**: Best practices for geospatial queries
4. **Migration Guide**: Steps for enabling geospatial features on existing collections

This implementation plan provides a structured approach to adding geospatial capabilities to NeoSQLite while maintaining its core strengths as a SQLite-based NoSQL solution.