"""
Tests for the SQL translator unified module.
"""

from neosqlite.collection.sql_translator_unified import (
    _empty_result,
    _text_search_fallback,
    SQLFieldAccessor,
    SQLOperatorTranslator,
    SQLClauseBuilder,
    SQLTranslator,
)
from neosqlite.collection.jsonb_support import supports_jsonb
import sqlite3


def _get_expected_function_name():
    """Get the expected function name based on JSONB support."""
    # Create a temporary in-memory database to test JSONB support
    db = sqlite3.connect(":memory:")
    try:
        jsonb_supported = supports_jsonb(db)
    except sqlite3.OperationalError:
        # If JSONB is not supported, fall back to standard json
        jsonb_supported = False
    finally:
        db.close()

    # Determine expected function name based on JSONB support
    return "jsonb_extract" if jsonb_supported else "json_extract"


def test_empty_result():
    """Test the _empty_result function."""
    result = _empty_result()
    assert result == ("", [])


def test_text_search_fallback():
    """Test the _text_search_fallback function."""
    result = _text_search_fallback()
    assert result == (None, [])


class TestSQLFieldAccessor:
    """Tests for the SQLFieldAccessor class."""

    def test_init_default(self):
        """Test SQLFieldAccessor initialization with default values."""
        accessor = SQLFieldAccessor()
        assert accessor.data_column == "data"
        assert accessor.id_column == "id"

    def test_init_custom(self):
        """Test SQLFieldAccessor initialization with custom values."""
        accessor = SQLFieldAccessor(data_column="json_data", id_column="doc_id")
        assert accessor.data_column == "json_data"
        assert accessor.id_column == "doc_id"

    def test_get_field_access_id_field(self):
        """Test field access for _id field."""
        accessor = SQLFieldAccessor()
        result = accessor.get_field_access("_id")
        assert result == "id"

    def test_get_field_access_id_field_custom(self):
        """Test field access for _id field with custom id column."""
        accessor = SQLFieldAccessor(id_column="doc_id")
        result = accessor.get_field_access("_id")
        assert result == "doc_id"

    def test_get_field_access_simple_field(self):
        """Test field access for simple field."""
        expected_function = _get_expected_function_name()
        # Create a temporary in-memory database to test JSONB support
        db = sqlite3.connect(":memory:")
        try:
            jsonb_supported = supports_jsonb(db)
        except sqlite3.OperationalError:
            jsonb_supported = False
        finally:
            db.close()
        accessor = SQLFieldAccessor(jsonb_supported=jsonb_supported)
        result = accessor.get_field_access("name")
        assert result == f"{expected_function}(data, '$.name')"

    def test_get_field_access_regular_field_custom(self):
        """Test field access for regular fields with custom data column."""
        expected_function = _get_expected_function_name()
        # Create a temporary in-memory database to test JSONB support
        db = sqlite3.connect(":memory:")
        try:
            jsonb_supported = supports_jsonb(db)
        except sqlite3.OperationalError:
            jsonb_supported = False
        finally:
            db.close()
        accessor = SQLFieldAccessor(
            data_column="json_data", jsonb_supported=jsonb_supported
        )
        result = accessor.get_field_access("name")
        assert result == f"{expected_function}(json_data, '$.name')"

    def test_get_field_access_context(self):
        """Test field access with context parameter."""
        expected_function = _get_expected_function_name()
        # Create a temporary in-memory database to test JSONB support
        db = sqlite3.connect(":memory:")
        try:
            jsonb_supported = supports_jsonb(db)
        except sqlite3.OperationalError:
            jsonb_supported = False
        finally:
            db.close()
        accessor = SQLFieldAccessor(jsonb_supported=jsonb_supported)
        result = accessor.get_field_access("name", context="temp")
        assert result == f"{expected_function}(data, '$.name')"

    def test_parse_json_path_simple_field(self):
        """Test parsing simple field path."""
        accessor = SQLFieldAccessor()
        result = accessor._parse_json_path("name")
        assert result == "$.name"

    def test_parse_json_path_nested_field(self):
        """Test parsing nested field path."""
        accessor = SQLFieldAccessor()
        result = accessor._parse_json_path("address.street")
        assert result == "$.address.street"

    def test_parse_json_path_array_indexing(self):
        """Test parsing field with array indexing."""
        accessor = SQLFieldAccessor()
        result = accessor._parse_json_path("tags[0]")
        assert result == "$.tags[0]"

    def test_parse_json_path_nested_array_access(self):
        """Test parsing nested array access."""
        accessor = SQLFieldAccessor()
        result = accessor._parse_json_path("orders.items[2].name")
        assert result == "$.orders.items[2].name"

    def test_parse_json_path_complex_path(self):
        """Test parsing complex path with multiple array indices."""
        accessor = SQLFieldAccessor()
        result = accessor._parse_json_path("a.b[0].c[1].d")
        assert result == "$.a.b[0].c[1].d"

    def test_parse_json_path_id_field(self):
        """Test parsing _id field (special case)."""
        accessor = SQLFieldAccessor()
        result = accessor._parse_json_path("_id")
        assert result == "_id"

    def test_parse_json_path_array_index(self):
        """Test parsing field path with array index."""
        expected_function = _get_expected_function_name()
        # Create a temporary in-memory database to test JSONB support
        db = sqlite3.connect(":memory:")
        try:
            jsonb_supported = supports_jsonb(db)
        except sqlite3.OperationalError:
            jsonb_supported = False
        finally:
            db.close()
        accessor = SQLFieldAccessor(jsonb_supported=jsonb_supported)
        result = accessor.get_field_access("tags[0]")
        assert result == f"{expected_function}(data, '$.tags[0]')"

    def test_get_field_access_nested_array_access(self):
        """Test parsing nested field path with array access."""
        expected_function = _get_expected_function_name()
        # Create a temporary in-memory database to test JSONB support
        db = sqlite3.connect(":memory:")
        try:
            jsonb_supported = supports_jsonb(db)
        except sqlite3.OperationalError:
            jsonb_supported = False
        finally:
            db.close()
        accessor = SQLFieldAccessor(jsonb_supported=jsonb_supported)
        result = accessor.get_field_access("orders.items[2].name")
        assert result == f"{expected_function}(data, '$.orders.items[2].name')"


class TestSQLOperatorTranslator:
    """Tests for the SQLOperatorTranslator class."""

    def test_init_default(self):
        """Test SQLOperatorTranslator initialization with default values."""
        translator = SQLOperatorTranslator()
        assert isinstance(translator.field_accessor, SQLFieldAccessor)

    def test_init_custom_field_accessor(self):
        """Test SQLOperatorTranslator initialization with custom field accessor."""
        custom_accessor = SQLFieldAccessor(data_column="json_data")
        translator = SQLOperatorTranslator(field_accessor=custom_accessor)
        assert translator.field_accessor == custom_accessor

    def test_translate_operator_eq(self):
        """Test translation of $eq operator."""
        translator = SQLOperatorTranslator()
        result = translator.translate_operator("field", "$eq", "value")
        assert result == ("field = ?", ["value"])

    def test_translate_operator_gt(self):
        """Test translation of $gt operator."""
        translator = SQLOperatorTranslator()
        result = translator.translate_operator("field", "$gt", 10)
        assert result == ("field > ?", [10])

    def test_translate_operator_lt(self):
        """Test translation of $lt operator."""
        translator = SQLOperatorTranslator()
        result = translator.translate_operator("field", "$lt", 10)
        assert result == ("field < ?", [10])

    def test_translate_operator_gte(self):
        """Test translation of $gte operator."""
        translator = SQLOperatorTranslator()
        result = translator.translate_operator("field", "$gte", 10)
        assert result == ("field >= ?", [10])

    def test_translate_operator_lte(self):
        """Test translation of $lte operator."""
        translator = SQLOperatorTranslator()
        result = translator.translate_operator("field", "$lte", 10)
        assert result == ("field <= ?", [10])

    def test_translate_operator_ne(self):
        """Test translation of $ne operator."""
        translator = SQLOperatorTranslator()
        result = translator.translate_operator("field", "$ne", "value")
        assert result == ("field != ?", ["value"])

    def test_translate_operator_in(self):
        """Test translation of $in operator."""
        translator = SQLOperatorTranslator()
        result = translator.translate_operator("field", "$in", [1, 2, 3])
        assert result == ("field IN (?, ?, ?)", [1, 2, 3])

    def test_translate_operator_in_empty_list(self):
        """Test translation of $in operator with empty list."""
        translator = SQLOperatorTranslator()
        result = translator.translate_operator("field", "$in", [])
        assert result == ("field IN ()", [])

    def test_translate_operator_in_tuple(self):
        """Test translation of $in operator with tuple."""
        translator = SQLOperatorTranslator()
        result = translator.translate_operator("field", "$in", (1, 2, 3))
        assert result == ("field IN (?, ?, ?)", [1, 2, 3])

    def test_translate_operator_nin(self):
        """Test translation of $nin operator."""
        translator = SQLOperatorTranslator()
        result = translator.translate_operator("field", "$nin", [1, 2, 3])
        assert result == ("field NOT IN (?, ?, ?)", [1, 2, 3])

    def test_translate_operator_nin_empty_list(self):
        """Test translation of $nin operator with empty list."""
        translator = SQLOperatorTranslator()
        result = translator.translate_operator("field", "$nin", [])
        assert result == ("field NOT IN ()", [])

    def test_translate_operator_nin_tuple(self):
        """Test translation of $nin operator with tuple."""
        translator = SQLOperatorTranslator()
        result = translator.translate_operator("field", "$nin", (1, 2, 3))
        assert result == ("field NOT IN (?, ?, ?)", [1, 2, 3])

    def test_translate_operator_exists_true(self):
        """Test translation of $exists operator with true value."""
        translator = SQLOperatorTranslator()
        result = translator.translate_operator("field", "$exists", True)
        assert result == ("field IS NOT NULL", [])

    def test_translate_operator_exists_false(self):
        """Test translation of $exists operator with false value."""
        translator = SQLOperatorTranslator()
        result = translator.translate_operator("field", "$exists", False)
        assert result == ("field IS NULL", [])

    def test_translate_operator_mod(self):
        """Test translation of $mod operator."""
        translator = SQLOperatorTranslator()
        result = translator.translate_operator("field", "$mod", [3, 1])
        assert result == ("field % ? = ?", [3, 1])

    def test_translate_operator_mod_invalid_format(self):
        """Test translation of $mod operator with invalid format."""
        translator = SQLOperatorTranslator()
        result = translator.translate_operator("field", "$mod", [3])
        assert result == (None, [])

    def test_translate_operator_mod_not_list(self):
        """Test translation of $mod operator with non-list value."""
        translator = SQLOperatorTranslator()
        result = translator.translate_operator("field", "$mod", "invalid")
        assert result == (None, [])

    def test_translate_operator_size(self):
        """Test translation of $size operator."""
        translator = SQLOperatorTranslator()
        result = translator.translate_operator("field", "$size", 5)
        assert result == ("json_array_length(field) = ?", [5])

    def test_translate_operator_size_not_int(self):
        """Test translation of $size operator with non-int value."""
        translator = SQLOperatorTranslator()
        result = translator.translate_operator("field", "$size", "invalid")
        assert result == (None, [])

    def test_translate_operator_contains(self):
        """Test translation of $contains operator."""
        translator = SQLOperatorTranslator()
        result = translator.translate_operator("field", "$contains", "test")
        assert result == ("lower(field) LIKE ?", ["%test%"])

    def test_translate_operator_contains_case_insensitive(self):
        """Test translation of $contains operator with case conversion."""
        translator = SQLOperatorTranslator()
        result = translator.translate_operator("field", "$contains", "TEST")
        assert result == ("lower(field) LIKE ?", ["%test%"])

    def test_translate_operator_contains_non_string(self):
        """Test translation of $contains operator with non-string value."""
        translator = SQLOperatorTranslator()
        result = translator.translate_operator("field", "$contains", 123)
        assert result == ("lower(field) LIKE ?", ["%123%"])

    def test_translate_operator_unsupported(self):
        """Test translation of unsupported operator."""
        translator = SQLOperatorTranslator()
        result = translator.translate_operator("field", "$unsupported", "value")
        assert result == (None, [])


class TestSQLClauseBuilder:
    """Tests for the SQLClauseBuilder class."""

    def test_init_default(self):
        """Test SQLClauseBuilder initialization with default values."""
        builder = SQLClauseBuilder()
        assert isinstance(builder.field_accessor, SQLFieldAccessor)
        assert isinstance(builder.operator_translator, SQLOperatorTranslator)

    def test_init_custom_components(self):
        """Test SQLClauseBuilder initialization with custom components."""
        custom_accessor = SQLFieldAccessor()
        custom_translator = SQLOperatorTranslator()
        builder = SQLClauseBuilder(
            field_accessor=custom_accessor,
            operator_translator=custom_translator,
        )
        assert builder.field_accessor == custom_accessor
        assert builder.operator_translator == custom_translator

    def test_build_logical_condition_and(self):
        """Test building $and logical condition."""
        builder = SQLClauseBuilder()
        conditions = [{"name": "Alice"}, {"age": {"$gte": 18}}]
        result = builder._build_logical_condition("$and", conditions)
        expected_sql = "(json_extract(data, '$.name') = ?) AND (json_extract(data, '$.age') >= ?)"
        assert result[0] == expected_sql
        assert result[1] == ["Alice", 18]

    def test_build_logical_condition_or(self):
        """Test building $or logical condition."""
        builder = SQLClauseBuilder()
        conditions = [{"name": "Alice"}, {"age": {"$gte": 18}}]
        result = builder._build_logical_condition("$or", conditions)
        expected_sql = "(json_extract(data, '$.name') = ?) OR (json_extract(data, '$.age') >= ?)"
        assert result[0] == expected_sql
        assert result[1] == ["Alice", 18]

    def test_build_logical_condition_nor(self):
        """Test building $nor logical condition."""
        builder = SQLClauseBuilder()
        conditions = [{"name": "Alice"}, {"age": {"$gte": 18}}]
        result = builder._build_logical_condition("$nor", conditions)
        expected_sql = "NOT ((json_extract(data, '$.name') = ?) OR (json_extract(data, '$.age') >= ?))"
        assert result[0] == expected_sql
        assert result[1] == ["Alice", 18]

    def test_build_logical_condition_unsupported_operator(self):
        """Test building unsupported logical condition."""
        builder = SQLClauseBuilder()
        conditions = [{"name": "Alice"}]
        result = builder._build_logical_condition("$unsupported", conditions)
        assert result == (None, ["Alice"])

    def test_build_logical_condition_non_list(self):
        """Test building logical condition with non-list conditions."""
        builder = SQLClauseBuilder()
        result = builder._build_logical_condition("$and", "invalid")
        assert result == (None, [])

    def test_build_logical_condition_empty_list(self):
        """Test building logical condition with empty list."""
        builder = SQLClauseBuilder()
        result = builder._build_logical_condition("$and", [])
        assert result == ("", [])

    def test_build_logical_condition_invalid_condition(self):
        """Test building logical condition with invalid condition type."""
        builder = SQLClauseBuilder()
        conditions = ["invalid"]
        result = builder._build_logical_condition("$and", conditions)
        assert result == ("", [])

    def test_build_where_clause_simple_equality(self):
        """Test building WHERE clause with simple equality."""
        builder = SQLClauseBuilder()
        query = {"name": "Alice"}
        result = builder.build_where_clause(query)
        assert result[0] == "WHERE json_extract(data, '$.name') = ?"
        assert result[1] == ["Alice"]

    def test_build_where_clause_operator(self):
        """Test building WHERE clause with operator."""
        builder = SQLClauseBuilder()
        query = {"age": {"$gte": 18}}
        result = builder.build_where_clause(query)
        assert result[0] == "WHERE json_extract(data, '$.age') >= ?"
        assert result[1] == [18]

    def test_build_where_clause_and(self):
        """Test building WHERE clause with $and operator."""
        builder = SQLClauseBuilder()
        query = {"$and": [{"name": "Alice"}, {"age": {"$gte": 18}}]}
        result = builder.build_where_clause(query)
        expected_sql = "WHERE (json_extract(data, '$.name') = ?) AND (json_extract(data, '$.age') >= ?)"
        assert result[0] == expected_sql
        assert result[1] == ["Alice", 18]

    def test_build_where_clause_or(self):
        """Test building WHERE clause with $or operator."""
        builder = SQLClauseBuilder()
        query = {"$or": [{"name": "Alice"}, {"age": {"$gte": 18}}]}
        result = builder.build_where_clause(query)
        expected_sql = "WHERE (json_extract(data, '$.name') = ?) OR (json_extract(data, '$.age') >= ?)"
        assert result[0] == expected_sql
        assert result[1] == ["Alice", 18]

    def test_build_where_clause_nor(self):
        """Test building WHERE clause with $nor operator."""
        builder = SQLClauseBuilder()
        query = {"$nor": [{"name": "Alice"}, {"age": {"$gte": 18}}]}
        result = builder.build_where_clause(query)
        expected_sql = "WHERE NOT ((json_extract(data, '$.name') = ?) OR (json_extract(data, '$.age') >= ?))"
        assert result[0] == expected_sql
        assert result[1] == ["Alice", 18]

    def test_build_where_clause_not(self):
        """Test building WHERE clause with $not operator."""
        builder = SQLClauseBuilder()
        query = {"$not": {"name": "Alice"}}
        result = builder.build_where_clause(query)
        assert result[0] == "WHERE NOT (json_extract(data, '$.name') = ?)"
        assert result[1] == ["Alice"]

    def test_build_where_clause_not_invalid_format(self):
        """Test building WHERE clause with $not operator with invalid format."""
        builder = SQLClauseBuilder()
        query = {"$not": "invalid"}
        result = builder.build_where_clause(query)
        assert result == ("", [])  # Text search should fallback to empty result

    def test_build_where_clause_nested_text_search(self):
        """Test building WHERE clause with nested text search (should fallback)."""
        builder = SQLClauseBuilder()
        query = {"$and": [{"name": "Alice"}, {"$text": {"$search": "test"}}]}
        result = builder.build_where_clause(query)
        # This should not fallback because the top-level $and doesn't contain $text
        # The $text is in a nested condition which is handled by _build_logical_condition
        assert result[0] == "WHERE (json_extract(data, '$.name') = ?)"
        assert result[1] == ["Alice"]

    def test_build_where_clause_empty_query(self):
        """Test building WHERE clause with empty query."""
        builder = SQLClauseBuilder()
        query = {}
        result = builder.build_where_clause(query)
        assert result == ("", [])

    def test_build_order_by_clause(self):
        """Test building ORDER BY clause."""
        builder = SQLClauseBuilder()
        sort_spec = {"name": 1, "age": -1}
        result = builder.build_order_by_clause(sort_spec)
        assert (
            result
            == "ORDER BY json_extract(data, '$.name') ASC, json_extract(data, '$.age') DESC"
        )

    def test_build_order_by_clause_empty(self):
        """Test building ORDER BY clause with empty specification."""
        builder = SQLClauseBuilder()
        sort_spec = {}
        result = builder.build_order_by_clause(sort_spec)
        assert result == ""

    def test_build_limit_offset_clause_with_limit(self):
        """Test building LIMIT clause."""
        builder = SQLClauseBuilder()
        result = builder.build_limit_offset_clause(limit_value=10)
        assert result == "LIMIT 10"

    def test_build_limit_offset_clause_with_limit_and_skip(self):
        """Test building LIMIT and OFFSET clauses."""
        builder = SQLClauseBuilder()
        result = builder.build_limit_offset_clause(limit_value=10, skip_value=5)
        assert result == "LIMIT 10 OFFSET 5"

    def test_build_limit_offset_clause_with_skip_only(self):
        """Test building OFFSET clause only."""
        builder = SQLClauseBuilder()
        result = builder.build_limit_offset_clause(skip_value=5)
        assert result == "LIMIT -1 OFFSET 5"

    def test_build_limit_offset_clause_no_limit_no_skip(self):
        """Test building clauses with no limit and no skip."""
        builder = SQLClauseBuilder()
        result = builder.build_limit_offset_clause()
        assert result == ""


class TestSQLTranslator:
    """Tests for the SQLTranslator class."""

    def test_init_default(self):
        """Test SQLTranslator initialization with default values."""
        translator = SQLTranslator()
        assert translator.table_name == "collection"
        assert translator.data_column == "data"
        assert translator.id_column == "id"
        assert isinstance(translator.field_accessor, SQLFieldAccessor)
        assert isinstance(translator.operator_translator, SQLOperatorTranslator)
        assert isinstance(translator.clause_builder, SQLClauseBuilder)

    def test_init_custom(self):
        """Test SQLTranslator initialization with custom values."""
        translator = SQLTranslator(
            table_name="users", data_column="json_data", id_column="user_id"
        )
        assert translator.table_name == "users"
        assert translator.data_column == "json_data"
        assert translator.id_column == "user_id"

    def test_translate_match_simple(self):
        """Test translating simple $match stage."""
        translator = SQLTranslator()
        match_spec = {"name": "Alice"}
        result = translator.translate_match(match_spec)
        assert result[0] == "WHERE json_extract(data, '$.name') = ?"
        assert result[1] == ["Alice"]

    def test_translate_match_with_operator(self):
        """Test translating $match stage with operator."""
        translator = SQLTranslator()
        match_spec = {"age": {"$gte": 18}}
        result = translator.translate_match(match_spec)
        assert result[0] == "WHERE json_extract(data, '$.age') >= ?"
        assert result[1] == [18]

    def test_translate_match_text_search(self):
        """Test translating $match stage with text search (should fallback)."""
        translator = SQLTranslator()
        match_spec = {"$text": {"$search": "test"}}
        result = translator.translate_match(match_spec)
        assert result == (None, [])

    def test_translate_match_nested_text_search(self):
        """Test translating $match stage with nested text search (should fallback)."""
        translator = SQLTranslator()
        match_spec = {
            "$and": [{"name": "Alice"}, {"$text": {"$search": "test"}}]
        }
        result = translator.translate_match(match_spec)
        assert result == (None, [])

    def test_contains_text_operator_top_level(self):
        """Test _contains_text_operator with top-level $text."""
        translator = SQLTranslator()
        query = {"$text": {"$search": "test"}}
        result = translator._contains_text_operator(query)
        assert result is True

    def test_contains_text_operator_nested_in_and(self):
        """Test _contains_text_operator with $text nested in $and."""
        translator = SQLTranslator()
        query = {"$and": [{"name": "Alice"}, {"$text": {"$search": "test"}}]}
        result = translator._contains_text_operator(query)
        assert result is True

    def test_contains_text_operator_nested_in_or(self):
        """Test _contains_text_operator with $text nested in $or."""
        translator = SQLTranslator()
        query = {"$or": [{"name": "Alice"}, {"$text": {"$search": "test"}}]}
        result = translator._contains_text_operator(query)
        assert result is True

    def test_contains_text_operator_nested_in_nor(self):
        """Test _contains_text_operator with $text nested in $nor."""
        translator = SQLTranslator()
        query = {"$nor": [{"name": "Alice"}, {"$text": {"$search": "test"}}]}
        result = translator._contains_text_operator(query)
        assert result is True

    def test_contains_text_operator_nested_in_not(self):
        """Test _contains_text_operator with $text nested in $not."""
        translator = SQLTranslator()
        query = {"$not": {"$text": {"$search": "test"}}}
        result = translator._contains_text_operator(query)
        assert result is True

    def test_contains_text_operator_no_text(self):
        """Test _contains_text_operator with no $text operators."""
        translator = SQLTranslator()
        query = {"$and": [{"name": "Alice"}, {"age": {"$gte": 18}}]}
        result = translator._contains_text_operator(query)
        assert result is False

    def test_translate_sort(self):
        """Test translating $sort stage."""
        translator = SQLTranslator()
        sort_spec = {"name": 1, "age": -1}
        result = translator.translate_sort(sort_spec)
        assert (
            result
            == "ORDER BY json_extract(data, '$.name') ASC, json_extract(data, '$.age') DESC"
        )

    def test_translate_skip_limit_with_limit(self):
        """Test translating $limit stage."""
        translator = SQLTranslator()
        result = translator.translate_skip_limit(limit_value=10)
        assert result == "LIMIT 10"

    def test_translate_skip_limit_with_limit_and_skip(self):
        """Test translating $limit and $skip stages."""
        translator = SQLTranslator()
        result = translator.translate_skip_limit(limit_value=10, skip_value=5)
        assert result == "LIMIT 10 OFFSET 5"

    def test_translate_skip_limit_with_skip_only(self):
        """Test translating $skip stage only."""
        translator = SQLTranslator()
        result = translator.translate_skip_limit(skip_value=5)
        assert result == "LIMIT -1 OFFSET 5"

    def test_translate_field_access_id(self):
        """Test translating field access for _id field."""
        translator = SQLTranslator()
        result = translator.translate_field_access("_id")
        assert result == "id"

    def test_translate_field_access_regular_field(self):
        """Test translating field access for regular field."""
        translator = SQLTranslator()
        result = translator.translate_field_access("name")
        assert result == "json_extract(data, '$.name')"

    def test_translate_sort_skip_limit_all_params(self):
        """Test translating sort/skip/limit stages together."""
        translator = SQLTranslator()
        result = translator.translate_sort_skip_limit(
            sort_spec={"name": 1}, skip_value=5, limit_value=10
        )
        assert result == (
            "ORDER BY json_extract(data, '$.name') ASC",
            "LIMIT 10 OFFSET 5",
            "",
        )

    def test_translate_sort_skip_limit_sort_only(self):
        """Test translating sort stage only."""
        translator = SQLTranslator()
        result = translator.translate_sort_skip_limit(sort_spec={"name": 1})
        assert result == ("ORDER BY json_extract(data, '$.name') ASC", "", "")

    def test_translate_sort_skip_limit_none(self):
        """Test translating with no parameters."""
        translator = SQLTranslator()
        result = translator.translate_sort_skip_limit(sort_spec=None)
        assert result == ("", "", "")

    def test_operator_translator_binary_serialization(self):
        """Test SQLOperatorTranslator with binary object serialization."""
        translator = SQLOperatorTranslator()

        # Create a mock binary object that is a bytes instance and has encode_for_storage method
        class MockBinary(bytes):
            def __new__(cls):
                return super().__new__(cls, b"test data")

            def encode_for_storage(self):
                return self

        # Test with a mock binary value that has the encode_for_storage method
        mock_binary = MockBinary()
        # Mock the neosqlite_json_dumps_for_sql function to return a string
        from unittest.mock import patch

        with patch(
            "neosqlite.collection.json_helpers.neosqlite_json_dumps_for_sql"
        ) as mock_dumps:
            mock_dumps.return_value = '"base64:test_data"'
            result = translator.translate_operator("field", "$eq", mock_binary)
            # Should serialize the binary data
            assert result[0] == "field = ?"
            # The parameter should be the mocked string
            assert result[1][0] == '"base64:test_data"'

    def test_build_logical_condition_break_on_none_clause(self):
        """Test _build_logical_condition breaking when clause is None."""
        builder = SQLClauseBuilder()

        # Mock build_where_clause to return None for specific input
        original_build_where_clause = builder.build_where_clause

        def mock_build_where_clause(query, context="direct", is_nested=True):
            if query == {"invalid": "condition"}:
                return None, []
            return original_build_where_clause(query, context, is_nested)

        builder.build_where_clause = mock_build_where_clause

        conditions = [{"valid": "condition"}, {"invalid": "condition"}]
        result = builder._build_logical_condition("$and", conditions)
        # Should break and return the clauses built so far
        assert result[0] == "(json_extract(data, '$.valid') = ?)"
        assert result[1] == ["condition"]

    def test_build_logical_condition_break_on_non_dict(self):
        """Test _build_logical_condition breaking when condition is not dict."""
        builder = SQLClauseBuilder()
        conditions = [{"valid": "condition"}, "invalid_condition"]
        result = builder._build_logical_condition("$and", conditions)
        # Should break and return the clauses built so far
        assert result[0] == "(json_extract(data, '$.valid') = ?)"
        assert result[1] == ["condition"]

    def test_build_where_clause_unsupported_operator_returns_none(self):
        """Test build_where_clause when translate_operator returns None."""
        builder = SQLClauseBuilder()

        # Test with an unsupported operator that would make translate_operator return None
        query = {"field": {"$unsupported": "value"}}

        # Mock the operator translator to return None for this case
        class MockOperatorTranslator:
            def translate_operator(self, field_access, operator, value):
                if operator == "$unsupported":
                    return None, []
                return f"{field_access} = ?", [value]

        builder.operator_translator = MockOperatorTranslator()

        result = builder.build_where_clause(query)
        assert result == (None, [])

    def test_build_where_clause_simple_equality_nested(self):
        """Test build_where_clause simple equality in nested context."""
        builder = SQLClauseBuilder()
        query = {"name": "Alice"}
        result = builder.build_where_clause(query, is_nested=True)
        # Should not have WHERE prefix when nested
        assert result[0] == "json_extract(data, '$.name') = ?"
        assert result[1] == ["Alice"]

    def test_build_limit_offset_clause_zero_skip(self):
        """Test build_limit_offset_clause with zero skip value."""
        builder = SQLClauseBuilder()
        result = builder.build_limit_offset_clause(skip_value=0)
        assert result == ""

    def test_contains_text_operator_empty_query(self):
        """Test _contains_text_operator with empty query."""
        translator = SQLTranslator()
        query = {}
        result = translator._contains_text_operator(query)
        assert result is False

    def test_contains_text_operator_non_dict_value(self):
        """Test _contains_text_operator with non-dict value in logical operators."""
        translator = SQLTranslator()
        query = {"$and": [{"name": "Alice"}, "invalid_condition"]}
        result = translator._contains_text_operator(query)
        assert result is False

    def test_contains_text_operator_non_list_value(self):
        """Test _contains_text_operator with non-list value for logical operators."""
        translator = SQLTranslator()
        query = {"$and": "invalid"}
        result = translator._contains_text_operator(query)
        assert result is False

    def test_field_accessor_id_field_special_case(self):
        """Test SQLFieldAccessor special handling for _id field."""
        accessor = SQLFieldAccessor(id_column="custom_id")
        result = accessor.get_field_access("_id", context="temp")
        assert result == "custom_id"

    def test_operator_translator_init_with_none(self):
        """Test SQLOperatorTranslator initialization with None field accessor."""
        translator = SQLOperatorTranslator(field_accessor=None)
        assert isinstance(translator.field_accessor, SQLFieldAccessor)

    def test_clause_builder_init_with_none_components(self):
        """Test SQLClauseBuilder initialization with None components."""
        builder = SQLClauseBuilder(
            field_accessor=None, operator_translator=None
        )
        assert isinstance(builder.field_accessor, SQLFieldAccessor)
        assert isinstance(builder.operator_translator, SQLOperatorTranslator)
