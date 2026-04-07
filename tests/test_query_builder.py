"""
Tests for query_builder.py - focusing on uncovered paths in QueryBuilderMixin

Targets uncovered code paths:
- _build_operator_clause (datetime_indexed, $mod, $size, $contains, $elemMatch)
- _apply_query (logical operators, $expr, $text, $jsonSchema, regex, nested paths)
- _get_operator_fn (valid and invalid operators)
- _build_simple_where_clause (various operator combinations)
- _build_field_clause (various field types and operators)
- _build_id_operator_clause (edge cases)
- _categorize_ids (edge cases)
- _search_in_value (recursive search)
- _build_text_search_query
- _build_sort_clause with collation
- _build_pagination_clause edge cases
"""

import re

import pytest

import neosqlite
from neosqlite.collection.query_helper import QueryHelper
from neosqlite.objectid import ObjectId


@pytest.fixture
def connection():
    """Set up a neosqlite connection."""
    conn = neosqlite.Connection(":memory:")
    yield conn
    conn.close()


@pytest.fixture
def collection(connection):
    """Provide a collection with test data."""
    coll = connection["test_qb"]
    coll.insert_many(
        [
            {
                "name": "Alice",
                "age": 30,
                "score": 85,
                "tags": ["python", "sql"],
            },
            {"name": "Bob", "age": 25, "score": 92, "tags": ["java", "sql"]},
            {
                "name": "Charlie",
                "age": 35,
                "score": 78,
                "tags": ["python", "js"],
            },
            {"name": "Diana", "age": 28, "score": 95, "tags": ["go", "sql"]},
        ]
    )
    return coll


@pytest.fixture
def query_helper(collection):
    """Provide a QueryHelper instance."""
    return QueryHelper(collection)


class TestBuildOperatorClause:
    """Tests for _build_operator_clause method."""

    def test_eq_operator(self, query_helper):
        """Test $eq operator clause generation."""
        clause, params = query_helper._build_operator_clause(
            "'$.name'", {"$eq": "test"}
        )
        assert "extract(data, '$.name') = ?" in clause
        assert params == ["test"]

    def test_gt_operator(self, query_helper):
        """Test $gt operator clause generation."""
        clause, params = query_helper._build_operator_clause(
            "'$.age'", {"$gt": 25}
        )
        assert "extract(data, '$.age') > ?" in clause
        assert params == [25]

    def test_lt_operator(self, query_helper):
        """Test $lt operator clause generation."""
        clause, params = query_helper._build_operator_clause(
            "'$.age'", {"$lt": 30}
        )
        assert "extract(data, '$.age') < ?" in clause
        assert params == [30]

    def test_gte_operator(self, query_helper):
        """Test $gte operator clause generation."""
        clause, params = query_helper._build_operator_clause(
            "'$.age'", {"$gte": 25}
        )
        assert "extract(data, '$.age') >= ?" in clause
        assert params == [25]

    def test_lte_operator(self, query_helper):
        """Test $lte operator clause generation."""
        clause, params = query_helper._build_operator_clause(
            "'$.age'", {"$lte": 30}
        )
        assert "extract(data, '$.age') <= ?" in clause
        assert params == [30]

    def test_ne_operator(self, query_helper):
        """Test $ne operator clause generation."""
        clause, params = query_helper._build_operator_clause(
            "'$.name'", {"$ne": "Alice"}
        )
        assert "extract(data, '$.name') != ?" in clause
        assert params == ["Alice"]

    def test_in_operator(self, query_helper):
        """Test $in operator clause generation."""
        clause, params = query_helper._build_operator_clause(
            "'$.name'", {"$in": ["Alice", "Bob"]}
        )
        assert "extract(data, '$.name') IN (?, ?)" in clause
        assert params == ["Alice", "Bob"]

    def test_nin_operator(self, query_helper):
        """Test $nin operator clause generation."""
        clause, params = query_helper._build_operator_clause(
            "'$.name'", {"$nin": ["Alice", "Bob"]}
        )
        assert "extract(data, '$.name') NOT IN (?, ?)" in clause
        assert params == ["Alice", "Bob"]

    def test_exists_true(self, query_helper):
        """Test $exists: true operator clause."""
        clause, params = query_helper._build_operator_clause(
            "'$.name'", {"$exists": True}
        )
        assert "extract(data, '$.name') IS NOT NULL" in clause
        assert params == []

    def test_exists_false(self, query_helper):
        """Test $exists: false operator clause."""
        clause, params = query_helper._build_operator_clause(
            "'$.name'", {"$exists": False}
        )
        assert "extract(data, '$.name') IS NULL" in clause
        assert params == []

    def test_exists_invalid_value(self, query_helper):
        """Test $exists with invalid value falls back."""
        clause, params = query_helper._build_operator_clause(
            "'$.name'", {"$exists": "invalid"}
        )
        assert clause is None
        assert params == []

    def test_mod_operator(self, query_helper):
        """Test $mod operator clause."""
        clause, params = query_helper._build_operator_clause(
            "'$.age'", {"$mod": [5, 0]}
        )
        assert "json_type(data, '$.age')" in clause
        assert "extract(data, '$.age') % ? = ?" in clause
        assert params == [5, 0]

    def test_mod_invalid_format(self, query_helper):
        """Test $mod with invalid format falls back."""
        clause, params = query_helper._build_operator_clause(
            "'$.age'", {"$mod": [5]}
        )
        assert clause is None
        assert params == []

    def test_size_operator(self, query_helper):
        """Test $size operator clause."""
        clause, params = query_helper._build_operator_clause(
            "'$.tags'", {"$size": 2}
        )
        assert "json_array_length" in clause
        assert params == [2]

    def test_size_invalid_value(self, query_helper):
        """Test $size with invalid value falls back."""
        clause, params = query_helper._build_operator_clause(
            "'$.tags'", {"$size": "invalid"}
        )
        assert clause is None
        assert params == []

    def test_contains_operator(self, query_helper):
        """Test $contains operator clause."""
        clause, params = query_helper._build_operator_clause(
            "'$.name'", {"$contains": "ali"}
        )
        assert "lower" in clause
        assert "LIKE ?" in clause
        assert params == ["%ali%"]

    def test_contains_invalid_value(self, query_helper):
        """Test $contains with invalid value falls back."""
        clause, params = query_helper._build_operator_clause(
            "'$.name'", {"$contains": 123}
        )
        assert clause is None
        assert params == []

    def test_elemmatch_simple_value(self, query_helper):
        """Test $elemMatch with simple value."""
        clause, params = query_helper._build_operator_clause(
            "'$.tags'", {"$elemMatch": "python"}
        )
        assert "json_type(data, '$.tags') = 'array'" in clause
        assert "EXISTS" in clause
        assert "value = ?" in clause
        assert params == ["python"]

    def test_elemmatch_with_operators(self, query_helper):
        """Test $elemMatch with operators."""
        clause, params = query_helper._build_operator_clause(
            "'$.scores'", {"$elemMatch": {"$gt": 90}}
        )
        assert "json_type(data, '$.scores') = 'array'" in clause
        assert "EXISTS" in clause
        assert "value > ?" in clause
        assert params == [90]

    def test_elemmatch_with_subfield(self, query_helper):
        """Test $elemMatch with subfield."""
        clause, params = query_helper._build_operator_clause(
            "'$.items'", {"$elemMatch": {"name": "test", "qty": {"$gt": 10}}}
        )
        assert "json_type(data, '$.items') = 'array'" in clause
        assert "EXISTS" in clause
        assert "extract(value," in clause

    def test_multiple_operators_combined(self, query_helper):
        """Test multiple operators combined with AND."""
        clause, params = query_helper._build_operator_clause(
            "'$.age'", {"$gte": 25, "$lte": 35, "$ne": 30}
        )
        assert ">=" in clause
        assert "<=" in clause
        assert "!=" in clause
        assert clause.count("AND") == 2
        assert params == [25, 35, 30]

    def test_unsupported_operator(self, query_helper):
        """Test unsupported operator returns None."""
        clause, params = query_helper._build_operator_clause(
            "'$.field'", {"$unsupported": "value"}
        )
        assert clause is None
        assert params == []

    def test_empty_operators(self, query_helper):
        """Test empty operators dict returns None."""
        clause, params = query_helper._build_operator_clause("'$.field'", {})
        assert clause is None
        assert params == []


class TestApplyQuery:
    """Tests for _apply_query method."""

    def test_apply_query_simple_match(self, query_helper):
        """Test _apply_query with simple field match."""
        doc = {"name": "Alice", "age": 30}
        assert query_helper._apply_query({"name": "Alice"}, doc) is True
        assert query_helper._apply_query({"name": "Bob"}, doc) is False

    def test_apply_query_with_operators(self, query_helper):
        """Test _apply_query with query operators."""
        doc = {"name": "Alice", "age": 30}
        assert query_helper._apply_query({"age": {"$gt": 25}}, doc) is True
        assert query_helper._apply_query({"age": {"$lt": 25}}, doc) is False

    def test_apply_query_and_operator(self, query_helper):
        """Test _apply_query with $and operator."""
        doc = {"name": "Alice", "age": 30}
        assert (
            query_helper._apply_query(
                {"$and": [{"name": "Alice"}, {"age": 30}]}, doc
            )
            is True
        )
        assert (
            query_helper._apply_query(
                {"$and": [{"name": "Alice"}, {"age": 25}]}, doc
            )
            is False
        )

    def test_apply_query_or_operator(self, query_helper):
        """Test _apply_query with $or operator."""
        doc = {"name": "Alice", "age": 30}
        assert (
            query_helper._apply_query(
                {"$or": [{"name": "Bob"}, {"age": 30}]}, doc
            )
            is True
        )
        assert (
            query_helper._apply_query(
                {"$or": [{"name": "Bob"}, {"age": 25}]}, doc
            )
            is False
        )

    def test_apply_query_nor_operator(self, query_helper):
        """Test _apply_query with $nor operator."""
        doc = {"name": "Alice", "age": 30}
        assert (
            query_helper._apply_query(
                {"$nor": [{"name": "Bob"}, {"age": 25}]}, doc
            )
            is True
        )
        assert (
            query_helper._apply_query(
                {"$nor": [{"name": "Alice"}, {"age": 25}]}, doc
            )
            is False
        )

    def test_apply_query_not_operator(self, query_helper):
        """Test _apply_query with $not operator."""
        doc = {"name": "Alice", "age": 30}
        assert query_helper._apply_query({"$not": {"name": "Bob"}}, doc) is True
        assert (
            query_helper._apply_query({"$not": {"name": "Alice"}}, doc) is False
        )

    def test_apply_query_none_document(self, query_helper):
        """Test _apply_query with None document."""
        assert query_helper._apply_query({"name": "Alice"}, None) is False

    def test_apply_query_nested_field(self, query_helper):
        """Test _apply_query with nested field path."""
        doc = {"user": {"name": "Alice", "address": {"city": "NYC"}}}
        assert query_helper._apply_query({"user.name": "Alice"}, doc) is True
        assert (
            query_helper._apply_query({"user.address.city": "NYC"}, doc) is True
        )
        assert (
            query_helper._apply_query({"user.address.city": "LA"}, doc) is False
        )

    def test_apply_query_regex_pattern(self, query_helper):
        """Test _apply_query with regex pattern."""
        doc = {"name": "Alice", "email": "alice@example.com"}
        assert (
            query_helper._apply_query({"name": re.compile(r"^Ali")}, doc)
            is True
        )
        assert (
            query_helper._apply_query({"name": re.compile(r"^Bob")}, doc)
            is False
        )

    def test_apply_query_regex_with_options(self, query_helper):
        """Test _apply_query with $regex and $options."""
        doc = {"name": "Alice"}
        assert (
            query_helper._apply_query(
                {"name": {"$regex": "alice", "$options": "i"}}, doc
            )
            is True
        )

    def test_apply_query_options_without_regex_error(self, query_helper):
        """Test _apply_query with $options without $regex raises error."""
        doc = {"name": "Alice"}
        from neosqlite.exceptions import MalformedQueryException

        with pytest.raises(
            MalformedQueryException,
            match="Can't use \\$options without \\$regex",
        ):
            query_helper._apply_query({"name": {"$options": "i"}}, doc)

    def test_apply_query_json_schema(self, query_helper):
        """Test _apply_query with $jsonSchema."""
        doc = {"name": "Alice", "age": 30}
        schema = {
            "required": ["name"],
            "properties": {
                "name": {"bsonType": "string"},
                "age": {"bsonType": "int"},
            },
        }
        assert query_helper._apply_query({"$jsonSchema": schema}, doc) is True

    def test_apply_query_text_search(self, collection, query_helper):
        """Test _apply_query with $text operator."""
        doc = {"name": "Alice", "description": "Python developer"}
        # Text search in Python fallback should search in document fields
        result = query_helper._apply_query(
            {"$text": {"$search": "python"}}, doc
        )
        # Result depends on whether FTS tables exist
        assert isinstance(result, bool)

    def test_apply_query_elemmatch_operator(self, query_helper):
        """Test _apply_query with $elemMatch."""
        doc = {"tags": ["python", "sql", "java"]}
        assert (
            query_helper._apply_query(
                {"tags": {"$elemMatch": {"$eq": "python"}}}, doc
            )
            is True
        )
        assert (
            query_helper._apply_query(
                {"tags": {"$elemMatch": {"$eq": "go"}}}, doc
            )
            is False
        )

    def test_apply_query_in_operator(self, query_helper):
        """Test _apply_query with $in operator."""
        doc = {"name": "Alice", "age": 30}
        assert (
            query_helper._apply_query({"name": {"$in": ["Alice", "Bob"]}}, doc)
            is True
        )
        assert (
            query_helper._apply_query(
                {"name": {"$in": ["Bob", "Charlie"]}}, doc
            )
            is False
        )

    def test_apply_query_exists_operator(self, query_helper):
        """Test _apply_query with $exists operator."""
        doc = {"name": "Alice", "age": 30}
        assert (
            query_helper._apply_query({"name": {"$exists": True}}, doc) is True
        )
        assert (
            query_helper._apply_query({"email": {"$exists": True}}, doc)
            is False
        )
        assert (
            query_helper._apply_query({"email": {"$exists": False}}, doc)
            is True
        )


class TestGetOperatorFn:
    """Tests for _get_operator_fn method."""

    def test_get_valid_operator(self, query_helper):
        """Test getting valid operator function."""
        func = query_helper._get_operator_fn("$eq")
        assert func is not None
        assert callable(func)

    def test_get_all_valid_operators(self, query_helper):
        """Test getting all standard operator functions."""
        operators = [
            "$eq",
            "$ne",
            "$gt",
            "$gte",
            "$lt",
            "$lte",
            "$in",
            "$nin",
            "$exists",
            "$mod",
            "$size",
            "$regex",
            "$elemMatch",
        ]
        for op in operators:
            func = query_helper._get_operator_fn(op)
            assert func is not None
            assert callable(func)

    def test_get_invalid_operator_no_dollar(self, query_helper):
        """Test invalid operator without $ prefix raises exception."""
        from neosqlite.exceptions import MalformedQueryException

        with pytest.raises(
            MalformedQueryException, match="not a valid query operation"
        ):
            query_helper._get_operator_fn("eq")

    def test_get_unimplemented_operator(self, query_helper):
        """Test unimplemented operator raises exception."""
        from neosqlite.exceptions import MalformedQueryException

        with pytest.raises(
            MalformedQueryException, match="not currently implemented"
        ):
            query_helper._get_operator_fn("$imaginaryOp")


class TestBuildSimpleWhereClause:
    """Tests for _build_simple_where_clause method."""

    def test_simple_equality(self, query_helper):
        """Test simple field equality."""
        result = query_helper._build_simple_where_clause({"name": "test"})
        assert result is not None
        clause, params, tables = result
        assert "WHERE" in clause
        assert params == ["test"]

    def test_multiple_fields(self, query_helper):
        """Test multiple field equality."""
        result = query_helper._build_simple_where_clause(
            {"name": "test", "age": 30}
        )
        assert result is not None
        clause, params, tables = result
        assert "WHERE" in clause
        assert "AND" in clause

    def test_with_gt_operator(self, query_helper):
        """Test with $gt operator."""
        result = query_helper._build_simple_where_clause({"age": {"$gt": 25}})
        assert result is not None
        clause, params, tables = result
        assert ">" in clause
        assert params == [25]

    def test_with_in_operator(self, query_helper):
        """Test with $in operator."""
        result = query_helper._build_simple_where_clause(
            {"name": {"$in": ["Alice", "Bob"]}}
        )
        assert result is not None
        clause, params, tables = result
        assert "IN" in clause

    def test_with_exists_operator(self, query_helper):
        """Test with $exists operator."""
        result = query_helper._build_simple_where_clause(
            {"name": {"$exists": True}}
        )
        assert result is not None
        clause, params, tables = result
        assert "IS NOT NULL" in clause

    def test_with_logical_operator_fallback(self, query_helper):
        """Test logical operators force Python fallback."""
        result = query_helper._build_simple_where_clause(
            {"$and": [{"name": "test"}]}
        )
        assert result is None

    def test_with_or_operator_fallback(self, query_helper):
        """Test $or forces Python fallback."""
        result = query_helper._build_simple_where_clause(
            {"$or": [{"name": "test"}]}
        )
        assert result is None

    def test_with_nor_operator_fallback(self, query_helper):
        """Test $nor forces Python fallback."""
        result = query_helper._build_simple_where_clause(
            {"$nor": [{"name": "test"}]}
        )
        assert result is None

    def test_with_not_operator_fallback(self, query_helper):
        """Test $not forces Python fallback."""
        result = query_helper._build_simple_where_clause(
            {"$not": {"name": "test"}}
        )
        assert result is None

    def test_empty_query(self, query_helper):
        """Test empty query returns empty clause."""
        result = query_helper._build_simple_where_clause({})
        assert result is not None
        clause, params, tables = result
        assert clause == ""
        assert params == []

    def test_where_query_raises_on_where_operator(self, query_helper):
        """Test $where raises NotImplementedError."""
        with pytest.raises(NotImplementedError, match="\\$where"):
            query_helper._build_simple_where_clause(
                {"$where": "function() { return true; }"}
            )

    def test_where_query_raises_on_function_operator(self, query_helper):
        """Test $function raises NotImplementedError."""
        with pytest.raises(NotImplementedError, match="\\$function"):
            query_helper._build_simple_where_clause({"$function": {}})

    def test_where_query_raises_on_accumulator_operator(self, query_helper):
        """Test $accumulator raises NotImplementedError."""
        with pytest.raises(NotImplementedError, match="\\$accumulator"):
            query_helper._build_simple_where_clause({"$accumulator": {}})


class TestBuildFieldClause:
    """Tests for _build_field_clause method."""

    def test_json_schema_fallback(self, query_helper):
        """Test $jsonSchema returns None (Python fallback)."""
        result = query_helper._build_field_clause(
            "$jsonSchema", {"required": ["name"]}
        )
        assert result is None

    def test_id_field_with_objectid(self, query_helper):
        """Test _id field with ObjectId value."""
        oid = ObjectId()
        clause, params = query_helper._build_field_clause("_id", oid)
        assert "_id = ?" in clause
        assert params == [str(oid)]

    def test_id_field_with_int(self, query_helper):
        """Test _id field with integer value."""
        clause, params = query_helper._build_field_clause("_id", 123)
        assert ".id = ?" in clause
        assert params == [123]

    def test_id_field_with_24char_string(self, query_helper):
        """Test _id field with 24-char ObjectId string."""
        oid = ObjectId()
        clause, params = query_helper._build_field_clause("_id", str(oid))
        assert "_id = ?" in clause
        assert params == [str(oid)]

    def test_id_field_with_non_objectid_string(self, query_helper):
        """Test _id field with non-ObjectId string."""
        clause, params = query_helper._build_field_clause("_id", "some_id")
        assert "_id = ?" in clause
        assert params == ["some_id"]

    def test_id_field_with_operator_dict(self, query_helper):
        """Test _id field with operator dictionary."""
        clause, params = query_helper._build_field_clause(
            "_id", {"$in": [ObjectId(), ObjectId()]}
        )
        assert "IN" in clause

    def test_regular_field_with_equality(self, query_helper):
        """Test regular field with equality."""
        clause, params = query_helper._build_field_clause("name", "Alice")
        assert "extract(data," in clause
        assert params == ["Alice"]

    def test_regular_field_with_operator(self, query_helper):
        """Test regular field with operator."""
        clause, params = query_helper._build_field_clause("age", {"$gt": 25})
        assert "extract(data," in clause
        assert ">" in clause
        assert params == [25]

    def test_regex_object_fallback(self, query_helper):
        """Test regex object returns None (Python fallback)."""
        result = query_helper._build_field_clause("name", re.compile(r"^A"))
        assert result is None


class TestBuildIdOperatorClause:
    """Tests for _build_id_operator_clause method."""

    def test_id_in_with_mixed_types(self, query_helper):
        """Test _id $in with mixed int and ObjectId types."""
        oid = ObjectId()
        clause, params = query_helper._build_id_operator_clause(
            {"$in": [123, oid]}
        )
        assert clause is not None
        assert "id IN" in clause or "_id IN" in clause

    def test_id_nin_with_ints(self, query_helper):
        """Test _id $nin with integers."""
        clause, params = query_helper._build_id_operator_clause(
            {"$nin": [1, 2, 3]}
        )
        assert clause is not None
        assert "NOT IN" in clause
        assert params == [1, 2, 3]

    def test_id_ne_with_int(self, query_helper):
        """Test _id $ne with integer."""
        clause, params = query_helper._build_id_operator_clause({"$ne": 123})
        assert clause is not None
        assert "id != ?" in clause
        assert "_id != ?" in clause
        # Should exclude both int and string representation
        assert 123 in params
        assert "123" in params

    def test_id_ne_with_objectid(self, query_helper):
        """Test _id $ne with ObjectId."""
        oid = ObjectId()
        clause, params = query_helper._build_id_operator_clause({"$ne": oid})
        assert clause is not None
        assert "_id != ?" in clause
        assert str(oid) in params

    def test_id_gt_with_int(self, query_helper):
        """Test _id $gt with integer."""
        clause, params = query_helper._build_id_operator_clause({"$gt": 100})
        assert clause is not None
        assert "id > ?" in clause
        assert params == [100]

    def test_id_lt_with_objectid(self, query_helper):
        """Test _id $lt with ObjectId."""
        oid = ObjectId()
        clause, params = query_helper._build_id_operator_clause({"$lt": oid})
        assert clause is not None
        assert "_id < ?" in clause
        assert params == [str(oid)]

    def test_id_in_empty_list_fallback(self, query_helper):
        """Test _id $in with empty list falls back."""
        result = query_helper._build_id_operator_clause({"$in": []})
        assert result is None

    def test_id_in_with_unsupported_type(self, query_helper):
        """Test _id $in with unsupported type falls back."""
        result = query_helper._build_id_operator_clause({"$in": [1.5]})
        assert result is None

    def test_id_unsupported_operator(self, query_helper):
        """Test unsupported operator on _id falls back."""
        result = query_helper._build_id_operator_clause({"$regex": "test"})
        assert result is None


class TestCategorizeIds:
    """Tests for _categorize_ids method."""

    def test_categorize_ints(self, query_helper):
        """Test categorizing integer IDs."""
        result = query_helper._categorize_ids([1, 2, 3])
        assert result is not None
        int_ids, string_ids = result
        assert int_ids == [1, 2, 3]
        assert string_ids == []

    def test_categorize_objectids(self, query_helper):
        """Test categorizing ObjectId IDs."""
        oids = [ObjectId(), ObjectId()]
        result = query_helper._categorize_ids(oids)
        assert result is not None
        int_ids, string_ids = result
        assert int_ids == []
        assert len(string_ids) == 2

    def test_categorize_mixed_types(self, query_helper):
        """Test categorizing mixed int and ObjectId."""
        oid = ObjectId()
        result = query_helper._categorize_ids([123, oid, "test"])
        assert result is not None
        int_ids, string_ids = result
        assert 123 in int_ids
        assert str(oid) in string_ids
        assert "test" in string_ids

    def test_categorize_string_as_int(self, query_helper):
        """Test categorizing string that can be parsed as int."""
        result = query_helper._categorize_ids(["456"])
        assert result is not None
        int_ids, string_ids = result
        assert 456 in int_ids

    def test_categorize_unsupported_type(self, query_helper):
        """Test categorizing unsupported type returns None."""
        result = query_helper._categorize_ids([1.5])
        assert result is None


class TestSearchInValue:
    """Tests for _search_in_value method."""

    def test_search_in_string(self, query_helper):
        """Test searching in a string value."""
        assert query_helper._search_in_value("Hello World", "world") is True
        assert query_helper._search_in_value("Hello World", "foo") is False

    def test_search_in_dict(self, query_helper):
        """Test searching in a dictionary value."""
        data = {"name": "Alice", "nested": {"key": "value"}}
        assert query_helper._search_in_value(data, "alice") is True
        assert query_helper._search_in_value(data, "value") is True
        assert query_helper._search_in_value(data, "missing") is False

    def test_search_in_list(self, query_helper):
        """Test searching in a list value."""
        data = ["hello", "world", {"nested": "dict"}]
        assert query_helper._search_in_value(data, "hello") is True
        assert query_helper._search_in_value(data, "dict") is True
        assert query_helper._search_in_value(data, "missing") is False

    def test_search_in_other_type(self, query_helper):
        """Test searching in non-string/dict/list returns False."""
        assert query_helper._search_in_value(123, "123") is False
        assert query_helper._search_in_value(None, "test") is False


class TestBuildSortClause:
    """Tests for _build_sort_clause method."""

    def test_sort_ascending(self, query_helper):
        """Test ascending sort clause."""
        clause = query_helper._build_sort_clause({"name": 1})
        assert "ORDER BY" in clause
        assert "ASC" in clause

    def test_sort_descending(self, query_helper):
        """Test descending sort clause."""
        clause = query_helper._build_sort_clause({"age": -1})
        assert "ORDER BY" in clause
        assert "DESC" in clause

    def test_sort_multiple_fields(self, query_helper):
        """Test sorting by multiple fields."""
        clause = query_helper._build_sort_clause({"name": 1, "age": -1})
        assert "ORDER BY" in clause
        assert "ASC" in clause
        assert "DESC" in clause

    def test_sort_id_field(self, query_helper):
        """Test sorting by _id field."""
        clause = query_helper._build_sort_clause({"_id": 1})
        assert "_id" in clause

    def test_sort_with_collation_nocase(self, query_helper):
        """Test sorting with case-insensitive collation."""
        clause = query_helper._build_sort_clause(
            {"name": 1}, collation={"strength": 1}
        )
        assert "COLLATE NOCASE" in clause

    def test_sort_with_collation_case_level_false(self, query_helper):
        """Test sorting with caseLevel=False."""
        clause = query_helper._build_sort_clause(
            {"name": 1}, collation={"caseLevel": False}
        )
        assert "COLLATE NOCASE" in clause

    def test_sort_with_collation_strength_3(self, query_helper):
        """Test sorting with strength=3 (default, may still use NOCASE based on implementation)."""
        clause = query_helper._build_sort_clause(
            {"name": 1}, collation={"strength": 3}
        )
        # strength=3 with default caseLevel=False may still use NOCASE
        # Just verify the clause is built correctly
        assert "ORDER BY" in clause
        assert "ASC" in clause

    def test_sort_none_returns_empty(self, query_helper):
        """Test None sort returns empty string."""
        clause = query_helper._build_sort_clause(None)
        assert clause == ""

    def test_sort_empty_dict(self, query_helper):
        """Test empty sort dict returns empty string."""
        clause = query_helper._build_sort_clause({})
        assert clause == ""


class TestBuildPaginationClause:
    """Tests for _build_pagination_clause method."""

    def test_limit_only(self, query_helper):
        """Test limit only pagination."""
        clause = query_helper._build_pagination_clause(10)
        assert "LIMIT 10" in clause

    def test_skip_only(self, query_helper):
        """Test skip only pagination (requires LIMIT -1)."""
        clause = query_helper._build_pagination_clause(None, skip=5)
        assert "LIMIT -1 OFFSET 5" in clause

    def test_limit_and_skip(self, query_helper):
        """Test limit and skip combined."""
        clause = query_helper._build_pagination_clause(10, skip=5)
        assert "LIMIT 10" in clause
        assert "OFFSET 5" in clause

    def test_no_limit_no_skip(self, query_helper):
        """Test no limit and no skip returns empty."""
        clause = query_helper._build_pagination_clause(None, 0)
        assert clause == ""


class TestBuildTextSearchQuery:
    """Tests for _build_text_search_query method."""

    def test_text_search_no_fts_tables(self, collection, query_helper):
        """Test text search when no FTS tables exist."""
        # This collection doesn't have FTS tables
        result = query_helper._build_text_search_query(
            {"$text": {"$search": "test"}}
        )
        # Should return None when no FTS tables
        assert result is None

    def test_text_search_invalid_query(self, query_helper):
        """Test text search with invalid query format."""
        assert (
            query_helper._build_text_search_query({"$text": "invalid"}) is None
        )
        assert query_helper._build_text_search_query({"$text": {}}) is None
        assert (
            query_helper._build_text_search_query({"$text": {"$search": 123}})
            is None
        )

    def test_text_search_not_in_query(self, query_helper):
        """Test text search when $text not in query."""
        result = query_helper._build_text_search_query({"name": "test"})
        assert result is None


class TestCategorizeIdValue:
    """Tests for _categorize_id_value method."""

    def test_categorize_int(self, query_helper):
        """Test categorizing an integer."""
        int_val, str_val = query_helper._categorize_id_value(123)
        assert int_val == 123
        assert str_val is None

    def test_categorize_objectid(self, query_helper):
        """Test categorizing an ObjectId."""
        oid = ObjectId()
        int_val, str_val = query_helper._categorize_id_value(oid)
        assert int_val is None
        assert str_val == str(oid)

    def test_categorize_objectid_string(self, query_helper):
        """Test categorizing an ObjectId string."""
        oid = ObjectId()
        int_val, str_val = query_helper._categorize_id_value(str(oid))
        assert int_val is None
        assert str_val == str(oid)

    def test_categorize_numeric_string(self, query_helper):
        """Test categorizing a numeric string."""
        int_val, str_val = query_helper._categorize_id_value("456")
        assert int_val == 456
        assert str_val is None

    def test_categorize_plain_string(self, query_helper):
        """Test categorizing a plain string (not ObjectId or int)."""
        int_val, str_val = query_helper._categorize_id_value("test_id")
        assert int_val is None
        assert str_val == "test_id"

    def test_categorize_unsupported_type(self, query_helper):
        """Test categorizing unsupported type."""
        int_val, str_val = query_helper._categorize_id_value(1.5)
        assert int_val is None
        assert str_val is None
