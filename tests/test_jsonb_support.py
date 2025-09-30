"""
Tests for JSONB support utilities to improve coverage.
"""

from unittest.mock import Mock

from neosqlite.collection.jsonb_support import (
    supports_jsonb,
    _get_json_function_prefix,
    should_use_json_functions,
    _contains_text_operator,
    sqlite3,  # Import the same sqlite3 that the module uses
)


def test_get_json_function_prefix():
    """Test the _get_json_function_prefix function."""
    assert _get_json_function_prefix(True) == "jsonb"
    assert _get_json_function_prefix(False) == "json"


def test_should_use_json_functions_jsonb_not_supported():
    """Test should_use_json_functions when JSONB is not supported."""
    # When JSONB is not supported, it should always return True (use json functions)
    assert should_use_json_functions(query=None, jsonb_supported=False) is True
    assert (
        should_use_json_functions(query={"name": "test"}, jsonb_supported=False)
        is True
    )


def test_should_use_json_functions_jsonb_supported_no_query():
    """Test should_use_json_functions when JSONB is supported but no query provided."""
    # When JSONB is supported and no query is provided, it should return False (use jsonb functions)
    assert should_use_json_functions(query=None, jsonb_supported=True) is False


def test_should_use_json_functions_jsonb_supported_with_text_search():
    """Test should_use_json_functions when JSONB is supported but query has text search."""
    # Query with $text should return True (use json functions for FTS compatibility)
    query_with_text = {"$text": {"$search": "test"}}
    assert (
        should_use_json_functions(query=query_with_text, jsonb_supported=True)
        is True
    )


def test_should_use_json_functions_jsonb_supported_without_text_search():
    """Test should_use_json_functions when JSONB is supported and no text search."""
    # Query without $text should return False (use jsonb functions)
    query_without_text = {"name": "test"}
    assert (
        should_use_json_functions(
            query=query_without_text, jsonb_supported=True
        )
        is False
    )


def test_contains_text_operator_simple():
    """Test _contains_text_operator with simple $text query."""
    query = {"$text": {"$search": "test"}}
    assert _contains_text_operator(query) is True


def test_contains_text_operator_not_text():
    """Test _contains_text_operator with non-text query."""
    query = {"name": "test"}
    assert _contains_text_operator(query) is False


def test_contains_text_operator_nested_in_and():
    """Test _contains_text_operator with $text nested in $and."""
    query = {"$and": [{"name": "test"}, {"$text": {"$search": "search"}}]}
    assert _contains_text_operator(query) is True


def test_contains_text_operator_nested_in_or():
    """Test _contains_text_operator with $text nested in $or."""
    query = {"$or": [{"name": "test"}, {"$text": {"$search": "search"}}]}
    assert _contains_text_operator(query) is True


def test_contains_text_operator_nested_in_nor():
    """Test _contains_text_operator with $text nested in $nor."""
    query = {"$nor": [{"name": "test"}, {"$text": {"$search": "search"}}]}
    assert _contains_text_operator(query) is True


def test_contains_text_operator_nested_in_not():
    """Test _contains_text_operator with $text nested in $not."""
    query = {"$not": {"$text": {"$search": "search"}}}
    assert _contains_text_operator(query) is True


def test_contains_text_operator_deeply_nested():
    """Test _contains_text_operator with deeply nested $text."""
    query = {
        "$and": [
            {"$or": [{"field": "value"}, {"$text": {"$search": "search"}}]}
        ]
    }
    assert _contains_text_operator(query) is True


def test_contains_text_operator_non_dict_input():
    """Test _contains_text_operator with non-dict input."""
    assert _contains_text_operator("not_a_dict") is False
    assert _contains_text_operator(["not", "a", "dict"]) is False
    assert _contains_text_operator(123) is False
    assert _contains_text_operator(None) is False


def test_contains_text_operator_empty_dict():
    """Test _contains_text_operator with empty dict."""
    assert _contains_text_operator({}) is False


def test_contains_text_operator_complex_nested_without_text():
    """Test _contains_text_operator with complex nesting without $text."""
    query = {
        "$and": [
            {"$or": [{"field1": "value1"}, {"field2": "value2"}]},
            {"$nor": [{"field3": "value3"}]},
        ]
    }
    assert _contains_text_operator(query) is False


def test_supports_jsonb_with_mock_connection():
    """Test supports_jsonb with a mock connection that simulates JSONB support."""
    # Test successful execution (JSONB supported)
    mock_connection_success = Mock()
    mock_connection_success.execute.return_value = Mock()
    assert supports_jsonb(mock_connection_success) is True

    # Test failed execution (JSONB not supported)
    mock_connection_fail = Mock()
    mock_connection_fail.execute.side_effect = sqlite3.OperationalError(
        "no such function: jsonb"
    )
    # Create a new mock object to test the failure path
    mock_connection_fail_for_test = Mock()
    mock_connection_fail_for_test.execute.side_effect = (
        sqlite3.OperationalError("no such function: jsonb")
    )
    assert supports_jsonb(mock_connection_fail_for_test) is False
