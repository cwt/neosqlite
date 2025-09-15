"""
Integration tests for $inc and $mul operations to ensure MongoDB-compatible behavior.
"""

import pytest
from neosqlite import Connection
from neosqlite.exceptions import MalformedQueryException


class TestIncMulOperations:
    """Integration tests for $inc and $mul operations."""

    def setup_method(self):
        """Set up test database."""
        self.db = Connection(":memory:")
        self.collection = self.db["test_collection"]

    def test_inc_with_string_value_raises_error(self):
        """Test that $inc with string value raises MalformedQueryException."""
        # Insert document with string field
        result = self.collection.insert_one(
            {"name": "test", "value": "not_a_number"}
        )
        doc_id = result.inserted_id

        # $inc should raise an error
        with pytest.raises(MalformedQueryException) as exc_info:
            self.collection.update_one({"_id": doc_id}, {"$inc": {"value": 5}})
        assert "Cannot apply $inc to a value of non-numeric type" in str(
            exc_info.value
        )
        assert "str" in str(exc_info.value)
        assert "'not_a_number'" in str(exc_info.value)

    def test_mul_with_string_value_raises_error(self):
        """Test that $mul with string value raises MalformedQueryException."""
        # Insert document with string field
        result = self.collection.insert_one(
            {"name": "test", "value": "not_a_number"}
        )
        doc_id = result.inserted_id

        # $mul should raise an error
        with pytest.raises(MalformedQueryException) as exc_info:
            self.collection.update_one({"_id": doc_id}, {"$mul": {"value": 3}})
        assert "Cannot apply $mul to a value of non-numeric type" in str(
            exc_info.value
        )
        assert "str" in str(exc_info.value)
        assert "'not_a_number'" in str(exc_info.value)

    def test_inc_with_numeric_string_raises_error(self):
        """Test that $inc with numeric string value raises MalformedQueryException."""
        # Insert document with numeric string field
        result = self.collection.insert_one({"name": "test", "value": "123"})
        doc_id = result.inserted_id

        # $inc should raise an error (strings are not considered numeric)
        with pytest.raises(MalformedQueryException) as exc_info:
            self.collection.update_one({"_id": doc_id}, {"$inc": {"value": 5}})
        assert "Cannot apply $inc to a value of non-numeric type" in str(
            exc_info.value
        )
        assert "str" in str(exc_info.value)
        assert "'123'" in str(exc_info.value)

    def test_mul_with_numeric_string_raises_error(self):
        """Test that $mul with numeric string value raises MalformedQueryException."""
        # Insert document with numeric string field
        result = self.collection.insert_one({"name": "test", "value": "123"})
        doc_id = result.inserted_id

        # $mul should raise an error (strings are not considered numeric)
        with pytest.raises(MalformedQueryException) as exc_info:
            self.collection.update_one({"_id": doc_id}, {"$mul": {"value": 3}})
        assert "Cannot apply $mul to a value of non-numeric type" in str(
            exc_info.value
        )
        assert "str" in str(exc_info.value)
        assert "'123'" in str(exc_info.value)

    def test_inc_with_integer_works(self):
        """Test that $inc works correctly with integer values."""
        # Insert document with integer field
        result = self.collection.insert_one({"name": "test", "value": 10})
        doc_id = result.inserted_id

        # $inc should work
        self.collection.update_one({"_id": doc_id}, {"$inc": {"value": 5}})
        updated_doc = self.collection.find_one({"_id": doc_id})
        assert updated_doc["value"] == 15

    def test_mul_with_integer_works(self):
        """Test that $mul works correctly with integer values."""
        # Insert document with integer field
        result = self.collection.insert_one({"name": "test", "value": 10})
        doc_id = result.inserted_id

        # $mul should work
        self.collection.update_one({"_id": doc_id}, {"$mul": {"value": 3}})
        updated_doc = self.collection.find_one({"_id": doc_id})
        assert updated_doc["value"] == 30

    def test_inc_with_float_works(self):
        """Test that $inc works correctly with float values."""
        # Insert document with float field
        result = self.collection.insert_one({"name": "test", "value": 10.5})
        doc_id = result.inserted_id

        # $inc should work
        self.collection.update_one({"_id": doc_id}, {"$inc": {"value": 5.5}})
        updated_doc = self.collection.find_one({"_id": doc_id})
        assert updated_doc["value"] == 16.0

    def test_mul_with_float_works(self):
        """Test that $mul works correctly with float values."""
        # Insert document with float field
        result = self.collection.insert_one({"name": "test", "value": 10.5})
        doc_id = result.inserted_id

        # $mul should work
        self.collection.update_one({"_id": doc_id}, {"$mul": {"value": 2}})
        updated_doc = self.collection.find_one({"_id": doc_id})
        assert updated_doc["value"] == 21.0

    def test_inc_with_missing_field_works(self):
        """Test that $inc works with missing fields (treated as 0)."""
        # Insert document without the field
        result = self.collection.insert_one({"name": "test"})
        doc_id = result.inserted_id

        # $inc should work (creates field with increment value)
        self.collection.update_one({"_id": doc_id}, {"$inc": {"new_field": 5}})
        updated_doc = self.collection.find_one({"_id": doc_id})
        assert updated_doc["new_field"] == 5

    def test_mul_with_missing_field_works(self):
        """Test that $mul works with missing fields (treated as 0)."""
        # Insert document without the field
        result = self.collection.insert_one({"name": "test"})
        doc_id = result.inserted_id

        # $mul should work (creates field, 0 * value = 0)
        self.collection.update_one({"_id": doc_id}, {"$mul": {"new_field": 5}})
        updated_doc = self.collection.find_one({"_id": doc_id})
        assert updated_doc["new_field"] == 0

    def test_inc_with_none_field_works(self):
        """Test that $inc works with None fields (treated as 0)."""
        # Insert document with None field
        result = self.collection.insert_one({"name": "test", "value": None})
        doc_id = result.inserted_id

        # $inc should work (None treated as 0)
        self.collection.update_one({"_id": doc_id}, {"$inc": {"value": 5}})
        updated_doc = self.collection.find_one({"_id": doc_id})
        # Note: In MongoDB, incrementing a null field sets it to the increment value
        assert updated_doc["value"] == 5

    def test_operations_with_complex_queries_force_python_implementation(self):
        """Test that complex queries force Python implementation with validation."""
        # Insert document with string field
        result = self.collection.insert_one(
            {"name": "test", "value": "not_a_number"}
        )
        doc_id = result.inserted_id

        # Use complex query that forces Python implementation
        # $inc should still raise an error due to validation
        with pytest.raises(MalformedQueryException) as exc_info:
            self.collection.update_one(
                {"$or": [{"_id": doc_id}, {"_id": {"$ne": -1}}]},
                {"$inc": {"value": 5}},
            )
        assert "Cannot apply $inc to a value of non-numeric type" in str(
            exc_info.value
        )

        # $mul should still raise an error due to validation
        with pytest.raises(MalformedQueryException) as exc_info:
            self.collection.update_one(
                {"$or": [{"_id": doc_id}, {"_id": {"$ne": -1}}]},
                {"$mul": {"value": 3}},
            )
        assert "Cannot apply $mul to a value of non-numeric type" in str(
            exc_info.value
        )

    def test_operations_with_binary_fields_force_python_implementation(self):
        """Test that operations with Binary fields force Python implementation."""
        from neosqlite.binary import Binary

        # Insert document with Binary field (forces Python implementation)
        result = self.collection.insert_one(
            {
                "name": "test",
                "binary_data": Binary(b"some data"),
                "string_value": "not_a_number",
            }
        )
        doc_id = result.inserted_id

        # Operations on string field should still raise errors
        with pytest.raises(MalformedQueryException) as exc_info:
            self.collection.update_one(
                {"_id": doc_id}, {"$inc": {"string_value": 5}}
            )
        assert "Cannot apply $inc to a value of non-numeric type" in str(
            exc_info.value
        )

        with pytest.raises(MalformedQueryException) as exc_info:
            self.collection.update_one(
                {"_id": doc_id}, {"$mul": {"string_value": 3}}
            )
        assert "Cannot apply $mul to a value of non-numeric type" in str(
            exc_info.value
        )
