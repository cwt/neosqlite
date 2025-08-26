# coding: utf-8
import neosqlite


def test_build_update_clause_mul():
    """Test _build_update_clause with $mul operator."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    # Test $mul operator
    result = collection.query_engine.helpers._build_update_clause(
        {"$mul": {"price": 1.1}}
    )
    assert result is not None
    assert "json_extract(data, '$.price') * ?" in result[0]
    assert result[1] == [1.1]


def test_build_update_clause_min():
    """Test _build_update_clause with $min operator."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    # Test $min operator
    result = collection.query_engine.helpers._build_update_clause(
        {"$min": {"score": 50}}
    )
    assert result is not None
    assert "min(json_extract(data, '$.score'), ?)" in result[0]
    assert result[1] == [50]


def test_build_update_clause_max():
    """Test _build_update_clause with $max operator."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    # Test $max operator
    result = collection.query_engine.helpers._build_update_clause(
        {"$max": {"score": 100}}
    )
    assert result is not None
    assert "max(json_extract(data, '$.score'), ?)" in result[0]
    assert result[1] == [100]


def test_build_update_clause_unset():
    """Test _build_update_clause with $unset operator."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    # Test $unset operator
    result = collection.query_engine.helpers._build_update_clause(
        {"$unset": {"temp": ""}}
    )
    assert result is not None
    assert "json_remove(data, '$.temp')" in result[0]
    assert result[1] == []


def test_build_update_clause_multiple():
    """Test _build_update_clause with multiple operators."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    # Test multiple $set operators
    result = collection.query_engine.helpers._build_update_clause(
        {"$set": {"name": "Alice", "age": 30}}
    )
    assert result is not None
    assert "json_set(data, '$.name', ?, '$.age', ?)" in result[0]
    assert result[1] == ["Alice", 30]


def test_build_update_clause_unsupported():
    """Test _build_update_clause with unsupported operator."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    # Test unsupported operator
    result = collection.query_engine.helpers._build_update_clause(
        {"$unsupported": {"field": "value"}}
    )
    assert result is None


def test_build_update_clause_rename():
    """Test _build_update_clause with $rename operator (should fall back)."""
    db = neosqlite.Connection(":memory:")
    collection = db["test"]

    # Test $rename operator (should fall back to Python implementation)
    result = collection.query_engine.helpers._build_update_clause(
        {"$rename": {"old_name": "new_name"}}
    )
    assert result is None
