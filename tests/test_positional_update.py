"""Unit tests for positional array update operations."""

from neosqlite.collection.query_helper.positional_update import (
    _apply_positional_recursive,
    _apply_positional_update,
    _matches_filter,
    _matches_query_operators,
    _set_nested_field,
)


def test_apply_positional_update_empty_field_path():
    """Test _apply_positional_update with empty field path."""
    doc = {"a": 1}
    assert _apply_positional_update(doc, "", 2) is False


def test_apply_positional_update_no_positional():
    """Test _apply_positional_update with no positional operator."""
    doc = {"a": {"b": 1}}
    assert _apply_positional_update(doc, "a.b", 2) is True
    assert doc["a"]["b"] == 2


def test_apply_positional_recursive_index_out_of_bounds():
    """Test _apply_positional_recursive with index out of bounds."""
    doc = {"a": 1}
    assert _apply_positional_recursive(doc, ["a"], 1, 2) is False


def test_apply_positional_recursive_all_non_list():
    """Test $[] operator on a non-list field."""
    doc = {"a": 1}
    # Current part is $[] but doc is not a list
    assert _apply_positional_recursive(doc, ["$[]"], 0, 2) is False


def test_apply_positional_recursive_filtered_no_filter_spec():
    """Test $[identifier] when no filter is found for identifier."""
    doc = {"a": [1, 2, 3]}
    # identifier is "x", but array_filters is empty
    assert _apply_positional_recursive(doc["a"], ["$[x]"], 0, 10, []) is False


def test_apply_positional_recursive_filtered_non_list():
    """Test $[identifier] operator on a non-list field."""
    doc = {"a": 1}
    # Current part is $[x] but doc is not a list
    assert _apply_positional_recursive(doc, ["$[x]"], 0, 2, [{"x": 1}]) is False


def test_apply_positional_recursive_dollar_non_list():
    """Test $ operator on a non-list field."""
    doc = {"a": 1}
    # Current part is $ but doc is not a list
    assert _apply_positional_recursive(doc, ["$"], 0, 2) is False


def test_apply_positional_recursive_dollar_with_filter_doc():
    """Test $ operator with filter_doc matching."""
    doc = {"scores": [80, 90, 100]}
    parts = ["scores", "$"]
    # scores field matches 90 in filter_doc
    assert (
        _apply_positional_recursive(
            doc,
            parts,
            1,
            95,
            filter_doc={"scores": 90},
            parent_array=doc["scores"],
        )
        is True
    )
    assert doc["scores"] == [80, 95, 100]


def test_apply_positional_recursive_dollar_with_complex_filter_doc():
    """Test $ operator with complex filter_doc matching."""
    doc = {"scores": [{"val": 80}, {"val": 90}, {"val": 100}]}
    parts = ["scores", "$", "val"]
    # scores field matches {"val": 90} in filter_doc
    assert (
        _apply_positional_recursive(
            doc,
            parts,
            1,
            95,
            filter_doc={"scores": {"val": 90}},
            parent_array=doc["scores"],
        )
        is True
    )
    assert doc["scores"][1]["val"] == 95


def test_apply_positional_recursive_dollar_no_filter_doc():
    """Test $ operator without filter_doc (updates first element)."""
    doc = {"scores": [80, 90, 100]}
    parts = ["scores", "$"]
    assert (
        _apply_positional_recursive(
            doc, parts, 1, 95, parent_array=doc["scores"]
        )
        is True
    )
    assert doc["scores"] == [95, 90, 100]


def test_apply_positional_recursive_dollar_no_field_in_filter_doc():
    """Test $ operator when field is missing from filter_doc (updates first element)."""
    doc = {"scores": [80, 90, 100]}
    parts = ["scores", "$"]
    # filter_doc doesn't contain "scores"
    assert (
        _apply_positional_recursive(
            doc, parts, 1, 95, filter_doc={"_id": 1}, parent_array=doc["scores"]
        )
        is True
    )
    assert doc["scores"] == [95, 90, 100]


def test_apply_positional_recursive_regular_field_non_dict():
    """Test regular field access on non-dict."""
    assert _apply_positional_recursive(1, ["a"], 0, 2) is False


def test_apply_positional_recursive_create_nested_structure():
    """Test creating nested structure if it doesn't exist (only last part)."""
    doc = {}
    # Should work for last part
    assert _apply_positional_recursive(doc, ["a"], 0, 2) is True
    assert doc["a"] == 2

    # Should return False for middle parts
    doc = {}
    assert _apply_positional_recursive(doc, ["a", "b"], 0, 2) is False
    assert "a" not in doc


def test_apply_positional_recursive_next_is_positional():
    """Test next_is_positional logic in _apply_positional_recursive."""
    doc = {"a": [1, 2, 3]}
    # field path: "a.$[]", value: 0
    # when processing "a", it should see "$[]" is next
    assert _apply_positional_recursive(doc, ["a", "$[]"], 0, 0) is True
    assert doc["a"] == [0, 0, 0]


def test_matches_filter_scalar():
    """Test _matches_filter with scalar filter_spec."""
    assert _matches_filter(1, 1) is True
    assert _matches_filter(1, 2) is False


def test_matches_filter_dict_element_dict_filter():
    """Test _matches_filter with dict element and dict filter."""
    elem = {"a": 1, "b": 2}
    assert _matches_filter(elem, {"a": 1}) is True
    assert _matches_filter(elem, {"a": 1, "b": 2}) is True
    assert _matches_filter(elem, {"a": 2}) is False
    assert _matches_filter(elem, {"c": 1}) is False


def test_matches_filter_with_query_operators():
    """Test _matches_filter with query operators."""
    elem = {"a": 10}
    assert _matches_filter(elem, {"a": {"$gt": 5}}) is True
    assert _matches_filter(elem, {"a": {"$lt": 5}}) is False


def test_matches_query_operators_all():
    """Test all query operators in _matches_query_operators."""
    assert _matches_query_operators(10, {"$eq": 10}) is True
    assert _matches_query_operators(10, {"$eq": 11}) is False
    assert _matches_query_operators(10, {"$gt": 5}) is True
    assert _matches_query_operators(10, {"$gt": 15}) is False
    assert _matches_query_operators(10, {"$gte": 10}) is True
    assert _matches_query_operators(10, {"$gte": 11}) is False
    assert _matches_query_operators(10, {"$lt": 15}) is True
    assert _matches_query_operators(10, {"$lt": 5}) is False
    assert _matches_query_operators(10, {"$lte": 10}) is True
    assert _matches_query_operators(10, {"$lte": 9}) is False
    assert _matches_query_operators(10, {"$ne": 11}) is True
    assert _matches_query_operators(10, {"$ne": 10}) is False
    assert _matches_query_operators(10, {"$in": [10, 11]}) is True
    assert _matches_query_operators(10, {"$in": [1, 2]}) is False
    assert _matches_query_operators(10, {"$nin": [1, 2]}) is True
    assert _matches_query_operators(10, {"$nin": [10, 11]}) is False


def test_apply_positional_recursive_all_nested():
    """Test $[] operator with nested field update."""
    doc = [{"a": 1}, {"a": 2}]
    # path: "$[].a", value: 3
    assert _apply_positional_recursive(doc, ["$[]", "a"], 0, 3) is True
    assert doc == [{"a": 3}, {"a": 3}]


def test_apply_positional_recursive_filtered_nested():
    """Test $[identifier] operator with nested field update."""
    doc = [{"a": 1}, {"a": 2}]
    # path: "$[x].a", value: 3, filter: a=1
    assert (
        _apply_positional_recursive(doc, ["$[x]", "a"], 0, 3, [{"x": {"a": 1}}])
        is True
    )
    assert doc == [{"a": 3}, {"a": 2}]


def test_apply_positional_recursive_dollar_nested_with_filter_doc():
    """Test $ operator with nested field update and filter_doc."""
    doc = {"scores": [{"val": 80}, {"val": 90}]}
    parts = ["scores", "$", "val"]
    # scores field matches {"val": 90} in filter_doc
    assert (
        _apply_positional_recursive(
            doc,
            parts,
            1,
            95,
            filter_doc={"scores": {"val": 90}},
            parent_array=doc["scores"],
        )
        is True
    )
    assert doc["scores"][1]["val"] == 95


def test_apply_positional_recursive_dollar_nested_no_filter_doc():
    """Test $ operator with nested field update and no filter_doc."""
    doc = {"scores": [{"val": 80}, {"val": 90}]}
    parts = ["scores", "$", "val"]
    assert (
        _apply_positional_recursive(
            doc, parts, 1, 95, parent_array=doc["scores"]
        )
        is True
    )
    assert doc["scores"][0]["val"] == 95


def test_apply_positional_recursive_regular_field_nested():
    """Test regular field access with nested recursive call."""
    doc = {"a": {"b": 1}}
    assert _apply_positional_recursive(doc, ["a", "b"], 0, 2) is True
    assert doc["a"]["b"] == 2


def test_apply_positional_recursive_array_filters_no_identifier():
    """Test $[identifier] when array_filters exists but doesn't have the identifier."""
    doc = [{"a": 1}]
    assert _apply_positional_recursive(doc, ["$[x]"], 0, 2, [{"y": 1}]) is False


def test_apply_positional_recursive_array_filters_none():
    """Test $[identifier] when array_filters is None."""
    doc = [{"a": 1}]
    assert _apply_positional_recursive(doc, ["$[x]"], 0, 2, None) is False


def test_matches_filter_missing_key():
    """Test _matches_filter when key is missing in element."""
    assert _matches_filter({"a": 1}, {"b": 1}) is False


def test_matches_filter_nested_operators():
    """Test _matches_filter with nested query operators."""
    assert _matches_filter({"a": 10}, {"a": {"$gt": 5, "$lt": 15}}) is True
    assert _matches_filter({"a": 10}, {"a": {"$gt": 15}}) is False


def test_apply_positional_update_with_positional():
    """Test _apply_positional_update with a positional operator."""
    doc = {"scores": [80, 90, 100]}
    # This should call _apply_positional_recursive
    assert _apply_positional_update(doc, "scores.$[]", 0) is True
    assert doc["scores"] == [0, 0, 0]


def test_apply_positional_recursive_filtered_matching_filter():
    """Test $[identifier] when filter is found and matched."""
    doc = [1, 2, 3]
    # This should hit line 133
    assert _apply_positional_recursive(doc, ["$[x]"], 0, 10, [{"x": 2}]) is True
    assert doc == [1, 10, 3]


def test_apply_positional_recursive_dollar_no_index():
    """Test $ operator with index=0 (no field name to look back to)."""
    doc = [80, 90, 100]
    # index is 0, so field_name cannot be looked up
    assert (
        _apply_positional_recursive(
            doc, ["$"], 0, 95, filter_doc={"scores": 90}
        )
        is True
    )
    assert doc == [95, 90, 100]


def test_apply_positional_recursive_regular_field_nested_no_next_positional():
    """Test regular field access with non-positional next part."""
    doc = {"a": {"b": {"c": 1}}}
    assert _apply_positional_recursive(doc, ["a", "b", "c"], 0, 2) is True
    assert doc["a"]["b"]["c"] == 2


def test_apply_positional_recursive_dollar_no_field_in_filter_doc_nested():
    """Test $ operator when field is missing from filter_doc and it's a nested update."""
    doc = {"scores": [{"val": 80}, {"val": 90}]}
    parts = ["scores", "$", "val"]
    # filter_doc doesn't contain "scores"
    assert (
        _apply_positional_recursive(
            doc, parts, 1, 95, filter_doc={"_id": 1}, parent_array=doc["scores"]
        )
        is True
    )
    assert doc["scores"][0]["val"] == 95


def test_apply_positional_recursive_dollar_no_index_nested():
    """Test $ operator with index=0 and nested update."""
    doc = [{"val": 80}, {"val": 90}]
    # index is 0, filter_doc present, nested update
    assert (
        _apply_positional_recursive(
            doc, ["$", "val"], 0, 95, filter_doc={"scores": 90}
        )
        is True
    )
    assert doc[0]["val"] == 95


def test_matches_filter_scalar_with_dict_filter():
    """Test _matches_filter with scalar element and dict filter."""
    assert _matches_filter(10, {"$gt": 5}) is True
    assert _matches_filter(10, {"$lt": 5}) is False


def test_set_nested_field_creation():
    """Test _set_nested_field with multiple levels of creation."""
    doc = {}
    _set_nested_field(doc, "a.b.c", 42)
    assert doc["a"]["b"]["c"] == 42
