"""
Tests for the JSON path utilities module.

These tests aim to improve code coverage for the json_path_utils module.
"""

from neosqlite.collection.json_path_utils import (
    parse_json_path,
    build_json_extract_expression,
    build_jsonb_extract_expression,
)


def test_parse_json_path_simple_field():
    """Test parsing of a simple field."""
    result = parse_json_path("name")
    assert result == "$.name"


def test_parse_json_path_nested_field():
    """Test parsing of a nested field."""
    result = parse_json_path("user.profile")
    assert result == "$.user.profile"


def test_parse_json_path_array_index():
    """Test parsing of a field with array index."""
    result = parse_json_path("tags[0]")
    assert result == "$.tags[0]"


def test_parse_json_path_complex_nested_array():
    """Test parsing of complex nested field with arrays."""
    result = parse_json_path("user.friends[2].name")
    assert result == "$.user.friends[2].name"


def test_parse_json_path_multiple_arrays():
    """Test parsing of field with multiple array indices."""
    result = parse_json_path("a[0].b[1].c[2]")
    assert result == "$.a[0].b[1].c[2]"


def test_parse_json_path_complex_path():
    """Test parsing of a complex path."""
    result = parse_json_path("orders.items[2].details[0].price")
    assert result == "$.orders.items[2].details[0].price"


def test_parse_json_path_special_id():
    """Test special handling of _id field."""
    result = parse_json_path("_id")
    assert result == "_id"


def test_parse_json_path_malformed_bracket():
    """Test handling of malformed array index (no closing bracket)."""
    result = parse_json_path("field[no_closing")
    # The function should treat the bracket as a literal character when no closing bracket exists
    assert result == "$.field[no_closing"


def test_parse_json_path_empty_string():
    """Test handling of empty string."""
    result = parse_json_path("")
    assert result == "$."


def test_parse_json_path_only_brackets():
    """Test handling of path with only brackets."""
    result = parse_json_path("[0]")
    assert result == "$.[0]"


def test_parse_json_path_brackets_at_start():
    """Test path starting with brackets."""
    result = parse_json_path("[0].field")
    assert result == "$.[0].field"


def test_build_json_extract_expression():
    """Test building json_extract expression."""
    result = build_json_extract_expression("data", "user.name")
    assert result == "json_extract(data, '$.user.name')"


def test_build_json_extract_expression_with_array():
    """Test building json_extract expression with array access."""
    result = build_json_extract_expression("json_col", "tags[0]")
    assert result == "json_extract(json_col, '$.tags[0]')"


def test_build_json_extract_expression_special_id():
    """Test json_extract with _id (should be treated specially)."""
    result = build_json_extract_expression("data", "_id")
    # _id is special-cased and returns just "_id", not "$._id"
    assert result == "json_extract(data, '_id')"


def test_build_jsonb_extract_expression():
    """Test building jsonb_extract expression."""
    result = build_jsonb_extract_expression("data", "user.name")
    assert result == "jsonb_extract(data, '$.user.name')"


def test_build_jsonb_extract_expression_with_array():
    """Test building jsonb_extract expression with array access."""
    result = build_jsonb_extract_expression("json_col", "tags[0]")
    assert result == "jsonb_extract(json_col, '$.tags[0]')"


def test_build_jsonb_extract_expression_special_id():
    """Test jsonb_extract with _id (should be treated specially)."""
    result = build_jsonb_extract_expression("data", "_id")
    # _id is special-cased and returns just "_id", not "$._id"
    assert result == "jsonb_extract(data, '_id')"


def test_consistency_between_functions():
    """Test that parse_json_path works correctly with both build functions."""
    test_paths = [
        "name",
        "user.profile",
        "tags[0]",
        "user.friends[2].name",
        "a[0].b[1].c[2]",
        "orders.items[2].details[0].price",
        "_id",
    ]

    for path in test_paths:
        parsed = parse_json_path(path)
        json_extract = build_json_extract_expression("data", path)
        jsonb_extract = build_jsonb_extract_expression("data", path)

        # Check that the parsed path is embedded correctly
        assert parsed in json_extract
        assert parsed in jsonb_extract

        # Check the structure of the results
        assert json_extract.startswith("json_extract(data, '")
        assert json_extract.endswith("')")
        assert jsonb_extract.startswith("jsonb_extract(data, '")
        assert jsonb_extract.endswith("')")


def test_edge_case_bracket_handling():
    """Test edge cases related to bracket handling."""
    # Test multiple consecutive brackets (edge case)
    result = parse_json_path("field[[0]]")  # nested brackets
    assert result == "$.field[[0]]"

    # Test bracket followed by dot
    result = parse_json_path("field[0].next")
    assert result == "$.field[0].next"

    # Test field with bracket at end
    result = parse_json_path("field[0]")
    assert result == "$.field[0]"

    # Test malformed: multiple opening brackets without closing
    result = parse_json_path("field[[no_close")
    assert result == "$.field[[no_close"
