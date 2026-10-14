"""Comprehensive unit tests covering GridFS query building, JSON operators, and GridOutCursor."""

from __future__ import annotations

import io
import re
import pytest

import neosqlite
from neosqlite.gridfs import GridFSBucket
from neosqlite.gridfs.grid_file import (
    GridOutCursor,
    _append_json_column_operators,
    _mongo_dot_subpath_to_json_path,
    build_gridfs_files_where,
)
from neosqlite.objectid import ObjectId


def test_build_gridfs_files_where_id_variations():
    """Test where clause construction for different _id and id query formats."""
    oid = ObjectId()
    # 1. ObjectId
    where, params = build_gridfs_files_where({"_id": oid})
    assert "_id = ?" in where
    assert params == [str(oid)]

    # 2. 24-char hex string
    hex_str = str(oid)
    where, params = build_gridfs_files_where({"_id": hex_str})
    assert "_id = ?" in where
    assert params == [hex_str]

    # 3. 24-char non-hex string (ValueError branch)
    invalid_hex = "z" * 24
    where, params = build_gridfs_files_where({"_id": invalid_hex})
    assert "_id = ?" in where
    assert params == [invalid_hex]

    # 4. int ID
    where, params = build_gridfs_files_where({"_id": 12345})
    assert "_id = ?" in where
    assert params == [12345]

    # 5. id field
    where, params = build_gridfs_files_where({"id": 99})
    assert "id = ?" in where
    assert params == [99]

    # 6. _id with $eq dict
    where, params = build_gridfs_files_where({"_id": {"$eq": oid}})
    assert "_id = ?" in where
    assert params == [str(oid)]

    # 7. _id with $in dict
    where, params = build_gridfs_files_where({"_id": {"$in": [oid, 42]}})
    assert "_id IN (?, ?)" in where
    assert params == [str(oid), 42]


def test_build_gridfs_files_where_filename_and_length():
    """Test where clause construction for filename and length variations."""
    # filename scalar
    where, params = build_gridfs_files_where({"filename": "file.txt"})
    assert "filename = ?" in where
    assert params == ["file.txt"]

    # filename $regex, $ne, $eq, unknown
    where, params = build_gridfs_files_where(
        {"filename": {"$regex": "doc", "$ne": "doc_0.txt", "$eq": "doc_1.txt", "$custom": "test"}}
    )
    assert "filename LIKE ?" in where
    assert "filename != ?" in where
    assert "filename = ?" in where
    assert "%doc%" in params
    assert "doc_0.txt" in params
    assert "doc_1.txt" in params

    # length variations
    where, params = build_gridfs_files_where(
        {"length": {"$gt": 10, "$gte": 20, "$lt": 100, "$lte": 50, "$ne": 15, "$eq": 30}}
    )
    assert "length > ?" in where
    assert "length >= ?" in where
    assert "length < ?" in where
    assert "length <= ?" in where
    assert "length != ?" in where
    assert "length = ?" in where


def test_build_gridfs_files_where_logical_operators():
    """Test logical $and, $or, $nor in GridFS queries."""
    # Empty filter
    where, params = build_gridfs_files_where({})
    assert where == ""
    assert params == []

    # Invalid logical operand (not a list)
    where, params = build_gridfs_files_where({"$and": "not-a-list"})
    assert "1=0" in where

    # Subfilter not a dict
    where, params = build_gridfs_files_where({"$or": ["not-a-dict"]})
    assert "1=0" in where

    # Empty subfilter inside $and
    where, params = build_gridfs_files_where({"$and": [{}]})
    assert "(1=1)" in where

    # Empty list in $and / $nor vs $or
    where, params = build_gridfs_files_where({"$and": []})
    assert "1=1" in where

    where, params = build_gridfs_files_where({"$or": []})
    assert "1=0" in where

    # Valid $and, $or, $nor
    where, params = build_gridfs_files_where(
        {"$and": [{"filename": "a.txt"}, {"length": 10}]}
    )
    assert "AND" in where

    where, params = build_gridfs_files_where(
        {"$or": [{"filename": "a.txt"}, {"filename": "b.txt"}]}
    )
    assert "OR" in where

    where, params = build_gridfs_files_where(
        {"$nor": [{"filename": "a.txt"}]}
    )
    assert "IS NOT TRUE" in where

    # Unsupported top-level operator
    where, params = build_gridfs_files_where({"$unsupportedOp": 123})
    assert "1=0" in where

    # Unsupported dotted prefix
    where, params = build_gridfs_files_where({"other_prefix.field": "val"})
    assert "1=0" in where


def test_mongo_dot_subpath_to_json_path_edge_cases():
    """Test converting dotted paths to SQLite JSON path strings."""
    assert _mongo_dot_subpath_to_json_path("category") == "$.category"
    assert _mongo_dot_subpath_to_json_path("0") == "$[0]"
    assert _mongo_dot_subpath_to_json_path("a.b.1") == "$.a.b[1]"
    # Empty or leading dot returns None
    assert _mongo_dot_subpath_to_json_path("") is None
    assert _mongo_dot_subpath_to_json_path(".leading") is None


def test_append_json_column_operators_coverage():
    """Directly test _append_json_column_operators coverage for all cases."""
    col = "metadata"
    path = "'$.field'"

    # 1. $ne variations
    conds = []
    params = []
    _append_json_column_operators(col, path, {"$ne": None}, conds, params)
    assert "IS NOT NULL" in conds[0]

    conds = []
    params = []
    _append_json_column_operators(col, path, {"$ne": {"sub": 1}}, conds, params)
    assert "IS NOT json(?)" in conds[0]

    conds = []
    params = []
    _append_json_column_operators(col, path, {"$ne": re.compile(r"pattern")}, conds, params)
    assert "NOT LIKE ?" in conds[0]

    conds = []
    params = []
    _append_json_column_operators(col, path, {"$ne": "simple_val"}, conds, params)
    assert "!= ?" in conds[0]

    # 2. $gt/$gte/$lt/$lte with dict/list
    conds = []
    params = []
    _append_json_column_operators(col, path, {"$gt": {"invalid": 1}}, conds, params)
    assert conds[0] == "1=0"

    # 3. $in variations
    conds = []
    params = []
    _append_json_column_operators(col, path, {"$in": "not-a-list"}, conds, params)
    assert conds[0] == "1=0"

    conds = []
    params = []
    _append_json_column_operators(col, path, {"$in": []}, conds, params)
    assert conds[0] == "1=0"

    conds = []
    params = []
    _append_json_column_operators(col, path, {"$in": ["a", {"b": 2}, None]}, conds, params)
    assert "IN (" in conds[0]
    assert "= json(?)" in conds[0]
    assert "IS NULL" in conds[0]

    # 4. $nin variations
    conds = []
    params = []
    _append_json_column_operators(col, path, {"$nin": "not-a-list"}, conds, params)
    assert conds[0] == "1=0"

    conds = []
    params = []
    _append_json_column_operators(col, path, {"$nin": []}, conds, params)
    assert conds[0] == "1=1"

    conds = []
    params = []
    _append_json_column_operators(col, path, {"$nin": [None]}, conds, params)
    assert "IS NOT NULL" in conds[0]

    conds = []
    params = []
    _append_json_column_operators(col, path, {"$nin": [{"dict_only": 1}]}, conds, params)
    assert conds[0] == "1=0"

    conds = []
    params = []
    _append_json_column_operators(col, path, {"$nin": ["a", "b", None]}, conds, params)
    assert "NOT IN" in conds[0]

    # 5. $regex with options
    conds = []
    params = []
    _append_json_column_operators(
        col, path, {"$regex": re.compile(r"abc"), "$options": "i"}, conds, params
    )
    assert "NOCASE" in conds[0]

    # 6. $exists variations
    conds = []
    params = []
    _append_json_column_operators(col, path, {"$exists": "not-bool"}, conds, params)
    assert conds[0] == "1=0"

    conds = []
    params = []
    _append_json_column_operators(col, path, {"$exists": False}, conds, params)
    assert "IS NULL" in conds[0]

    # 7. $size variations
    conds = []
    params = []
    _append_json_column_operators(col, path, {"$size": "invalid"}, conds, params)
    assert conds[0] == "1=0"

    conds = []
    params = []
    _append_json_column_operators(col, path, {"$size": 3}, conds, params)
    assert "json_array_length" in conds[0]

    # 8. $all variations
    conds = []
    params = []
    _append_json_column_operators(col, path, {"$all": "not-list"}, conds, params)
    assert conds[0] == "1=0"

    conds = []
    params = []
    _append_json_column_operators(col, path, {"$all": []}, conds, params)
    assert conds[0] == "1=0"

    conds = []
    params = []
    _append_json_column_operators(col, path, {"$all": [{"dict": 1}]}, conds, params)
    assert conds[0] == "1=0"

    conds = []
    params = []
    _append_json_column_operators(col, path, {"$all": ["tag1", "tag2"]}, conds, params)
    assert len(conds) == 2
    assert params == ["tag1", "tag2"]

    # 9. Unsupported operator
    conds = []
    params = []
    _append_json_column_operators(col, path, {"$unknownOp": 1}, conds, params)
    assert conds[0] == "1=0"


def test_grid_out_cursor_methods_and_indexing():
    """Test GridOutCursor sort, skip, limit, clone, rewind, to_list, and errors."""
    conn = neosqlite.Connection(":memory:")
    bucket = GridFSBucket(conn)

    for i in range(5):
        bucket.upload_from_stream(
            f"file_{i}.txt",
            io.BytesIO(f"line 1\nline 2\nfile {i}".encode()),
            metadata={"priority": i % 2, "index": i},
        )

    # 1. Sort formats: string, dict, list
    cursor1 = bucket.find().sort("filename", 1)
    assert cursor1._sort == [("filename", 1)]

    cursor2 = bucket.find().sort({"filename": -1})
    assert cursor2._sort == [("filename", -1)]

    cursor3 = bucket.find().sort([("filename", 1)])
    assert cursor3._sort == [("filename", 1)]

    # 2. skip and limit combinations
    c_skip = bucket.find().sort("filename", 1).skip(2)
    files_skip = c_skip.to_list()
    assert len(files_skip) == 3
    assert files_skip[0].filename == "file_2.txt"

    c_limit = bucket.find().sort("filename", 1).limit(2)
    files_limit = c_limit.to_list()
    assert len(files_limit) == 2
    assert files_limit[0].filename == "file_0.txt"

    c_both = bucket.find().sort("filename", 1).skip(1).limit(2)
    files_both = c_both.to_list()
    assert len(files_both) == 2
    assert files_both[0].filename == "file_1.txt"

    # 3. clone and rewind
    cloned = c_both.clone()
    assert cloned is not c_both
    assert cloned._skip == 1
    assert cloned._limit == 2

    # 4. to_list with length
    c_all = bucket.find().sort("filename", 1)
    assert len(c_all.to_list(3)) == 3

    # 5. Invalid arguments
    with pytest.raises(ValueError, match="skip must be a non-negative integer"):
        bucket.find().skip(-1)

    with pytest.raises(ValueError, match="limit must be a non-negative integer"):
        bucket.find().limit(-1)

    with pytest.raises(ValueError, match="batch_size must be a positive integer"):
        bucket.find().batch_size(0)

    with pytest.raises(ValueError, match="length must be a non-negative integer"):
        bucket.find().to_list(-1)

    # 6. Iterating and cursor state guards
    iter_cursor = bucket.find()
    first_item = next(iter_cursor)
    assert first_item is not None

    with pytest.raises(RuntimeError, match="cannot sort after cursor has started iterating"):
        iter_cursor.sort("filename")

    with pytest.raises(RuntimeError, match="cannot skip after cursor has started iterating"):
        iter_cursor.skip(1)

    with pytest.raises(RuntimeError, match="cannot limit after cursor has started iterating"):
        iter_cursor.limit(1)

    # Rewind allows re-iterating
    iter_cursor.rewind()
    assert iter_cursor._file_ids is None
    all_rewound = list(iter_cursor)
    assert len(all_rewound) == 5

    with pytest.raises(StopIteration):
        next(iter_cursor)

    # 7. GridOut stream reading methods and properties
    file_id = bucket.upload_from_stream(
        "multiline.txt",
        b"row 1\nrow 2\nrow 3\n",
        metadata={"author": "alice"},
    )
    with bucket.open_download_stream(file_id) as grid_out:
        assert grid_out.filename == "multiline.txt"
        assert grid_out.length == len(b"row 1\nrow 2\nrow 3\n")
        assert grid_out.chunk_size > 0
        assert grid_out.metadata == {"author": "alice"}
        assert grid_out.upload_date is not None
        assert grid_out.md5 is not None
        assert grid_out._file_id is not None
        part = grid_out.read(5)
        assert part == b"row 1"
        rest = grid_out.read()
        assert rest == b"\nrow 2\nrow 3\n"
        # Reading at EOF returns empty bytes
        assert grid_out.read() == b""

    # 8. GridIn abort
    upload_stream = bucket.open_upload_stream("aborted.txt")
    upload_stream.write(b"partial content")
    upload_stream.abort()
    assert upload_stream._closed is True
    # Aborting already closed stream does nothing
    upload_stream.abort()
