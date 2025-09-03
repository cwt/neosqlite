"""
Test cases for the unified SQL translation framework.
"""

import unittest
import tempfile
import os
from neosqlite import Connection
from neosqlite.collection.query_helper import QueryHelper
from neosqlite.collection.temporary_table_aggregation import (
    TemporaryTableAggregationProcessor,
)
from neosqlite.collection.sql_translator_unified import SQLTranslator


class TestSQLTranslationFramework(unittest.TestCase):
    """Test cases for the unified SQL translation framework."""

    def setUp(self):
        """Set up test fixtures."""
        # Create temporary database for testing
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        self.temp_db.close()
        self.conn = Connection(self.temp_db.name)
        self.collection = self.conn["test_collection"]
        self.query_helper = QueryHelper(self.collection)
        self.translator = SQLTranslator(self.collection.name, "data", "id")

    def tearDown(self):
        """Tear down test fixtures."""
        self.conn.close()
        os.unlink(self.temp_db.name)

    def test_simple_equality_query(self):
        """Test simple equality query translation."""
        query = {"name": "John"}
        where_clause, params = self.translator.translate_match(query)
        self.assertEqual(where_clause, "WHERE json_extract(data, '$.name') = ?")
        self.assertEqual(params, ["John"])

    def test_comparison_operators(self):
        """Test comparison operators translation."""
        query = {"age": {"$gt": 25}}
        where_clause, params = self.translator.translate_match(query)
        self.assertEqual(where_clause, "WHERE json_extract(data, '$.age') > ?")
        self.assertEqual(params, [25])

        query = {"age": {"$lt": 65}}
        where_clause, params = self.translator.translate_match(query)
        self.assertEqual(where_clause, "WHERE json_extract(data, '$.age') < ?")
        self.assertEqual(params, [65])

        query = {"age": {"$gte": 18}}
        where_clause, params = self.translator.translate_match(query)
        self.assertEqual(where_clause, "WHERE json_extract(data, '$.age') >= ?")
        self.assertEqual(params, [18])

        query = {"age": {"$lte": 100}}
        where_clause, params = self.translator.translate_match(query)
        self.assertEqual(where_clause, "WHERE json_extract(data, '$.age') <= ?")
        self.assertEqual(params, [100])

        query = {"age": {"$ne": 30}}
        where_clause, params = self.translator.translate_match(query)
        self.assertEqual(where_clause, "WHERE json_extract(data, '$.age') != ?")
        self.assertEqual(params, [30])

    def test_membership_operators(self):
        """Test membership operators translation."""
        query = {"status": {"$in": ["active", "pending"]}}
        where_clause, params = self.translator.translate_match(query)
        self.assertEqual(
            where_clause, "WHERE json_extract(data, '$.status') IN (?, ?)"
        )
        self.assertEqual(params, ["active", "pending"])

        query = {"status": {"$nin": ["inactive", "deleted"]}}
        where_clause, params = self.translator.translate_match(query)
        self.assertEqual(
            where_clause, "WHERE json_extract(data, '$.status') NOT IN (?, ?)"
        )
        self.assertEqual(params, ["inactive", "deleted"])

    def test_exists_operator(self):
        """Test $exists operator translation."""
        query = {"email": {"$exists": True}}
        where_clause, params = self.translator.translate_match(query)
        self.assertEqual(
            where_clause, "WHERE json_extract(data, '$.email') IS NOT NULL"
        )
        self.assertEqual(params, [])

        query = {"email": {"$exists": False}}
        where_clause, params = self.translator.translate_match(query)
        self.assertEqual(
            where_clause, "WHERE json_extract(data, '$.email') IS NULL"
        )
        self.assertEqual(params, [])

    def test_mod_operator(self):
        """Test $mod operator translation."""
        query = {"count": {"$mod": [3, 1]}}
        where_clause, params = self.translator.translate_match(query)
        self.assertEqual(
            where_clause, "WHERE json_extract(data, '$.count') % ? = ?"
        )
        self.assertEqual(params, [3, 1])

    def test_size_operator(self):
        """Test $size operator translation."""
        query = {"tags": {"$size": 3}}
        where_clause, params = self.translator.translate_match(query)
        self.assertEqual(
            where_clause,
            "WHERE json_array_length(json_extract(data, '$.tags')) = ?",
        )
        self.assertEqual(params, [3])

    def test_contains_operator(self):
        """Test $contains operator translation."""
        query = {"description": {"$contains": "test"}}
        where_clause, params = self.translator.translate_match(query)
        self.assertEqual(
            where_clause,
            "WHERE lower(json_extract(data, '$.description')) LIKE ?",
        )
        self.assertEqual(params, ["%test%"])

    def test_id_field_handling(self):
        """Test _id field handling."""
        query = {"_id": 123}
        where_clause, params = self.translator.translate_match(query)
        self.assertEqual(where_clause, "WHERE id = ?")
        self.assertEqual(params, [123])

    def test_sort_translation(self):
        """Test sort translation."""
        sort_spec = {"name": 1, "age": -1}
        order_by = self.translator.translate_sort(sort_spec)
        self.assertEqual(
            order_by,
            "ORDER BY json_extract(data, '$.name') ASC, json_extract(data, '$.age') DESC",
        )

    def test_skip_limit_translation(self):
        """Test skip/limit translation."""
        limit_offset = self.translator.translate_skip_limit(10, 5)
        self.assertEqual(limit_offset, "LIMIT 10 OFFSET 5")

        limit_offset = self.translator.translate_skip_limit(None, 5)
        self.assertEqual(limit_offset, "LIMIT -1 OFFSET 5")

        limit_offset = self.translator.translate_skip_limit(10, 0)
        self.assertEqual(limit_offset, "LIMIT 10")

    def test_combined_sort_skip_limit_translation(self):
        """Test combined sort/skip/limit translation."""
        sort_spec = {"name": 1}
        order_by, limit_offset, _ = self.translator.translate_sort_skip_limit(
            sort_spec, 5, 10
        )
        self.assertEqual(order_by, "ORDER BY json_extract(data, '$.name') ASC")
        self.assertEqual(limit_offset, "LIMIT 10 OFFSET 5")

    def test_query_helper_integration(self):
        """Test QueryHelper integration with unified SQL translator."""
        query = {"name": "John", "age": {"$gt": 25}}
        where_clause, params = self.query_helper._build_simple_where_clause(
            query
        )
        self.assertIsNotNone(where_clause)
        self.assertIn("json_extract(data, '$.name') = ?", where_clause)
        self.assertIn("json_extract(data, '$.age') > ?", where_clause)
        self.assertEqual(len(params), 2)
        self.assertIn("John", params)
        self.assertIn(25, params)

    def test_temporary_table_processor_integration(self):
        """Test TemporaryTableAggregationProcessor integration with unified SQL translator."""
        processor = TemporaryTableAggregationProcessor(
            self.collection, self.query_helper
        )

        # Test that the processor can be instantiated
        self.assertIsNotNone(processor)

        # Test sort/skip/limit translation
        sort_spec = {"name": 1}
        # This would normally be called internally by the processor
        translator = SQLTranslator(self.collection.name, "data", "id")
        order_by = translator.translate_sort(sort_spec)
        self.assertEqual(order_by, "ORDER BY json_extract(data, '$.name') ASC")


if __name__ == "__main__":
    unittest.main()
