"""
Test cases for enhanced text search functionality in NeoSQLite.
"""

import pytest
import re
import unicodedata
from neosqlite import Connection


class TestEnhancedTextSearch:
    """Test cases for enhanced text search with international character support."""

    def setup_method(self):
        """Set up test database and collection."""
        self.conn = Connection(":memory:")
        self.collection = self.conn.test_collection

    def teardown_method(self):
        """Clean up test database."""
        self.conn.close()

    def test_basic_case_insensitive_search(self):
        """Test basic case-insensitive text search."""
        # Insert test documents
        self.collection.insert_many(
            [
                {
                    "name": "Python Developer",
                    "description": "Works with Python programming language",
                },
                {
                    "name": "Java Developer",
                    "description": "Works with Java programming language",
                },
                {
                    "name": "JavaScript Developer",
                    "description": "Works with JavaScript programming language",
                },
            ]
        )

        # Test case-insensitive search
        result = list(self.collection.find({"$text": {"$search": "python"}}))
        assert len(result) == 1
        assert result[0]["name"] == "Python Developer"

        # Test with different case
        result = list(self.collection.find({"$text": {"$search": "PYTHON"}}))
        assert len(result) == 1
        assert result[0]["name"] == "Python Developer"

    def test_international_character_search(self):
        """Test text search with international characters."""
        # Insert test documents with international characters
        self.collection.insert_many(
            [
                {
                    "name": "José María",
                    "description": "Software engineer from España",
                },
                {
                    "name": "François Dubois",
                    "description": "Développeur from France",
                },
                {
                    "name": "Björk Guðmundsdóttir",
                    "description": "Artist from Ísland",
                },
                {
                    "name": "Müller Schmidt",
                    "description": "Engineer from Deutschland",
                },
            ]
        )

        # Test search with accented characters
        result = list(self.collection.find({"$text": {"$search": "José"}}))
        assert len(result) == 1
        assert result[0]["name"] == "José María"

        # Test search with base characters (should match accented ones)
        result = list(self.collection.find({"$text": {"$search": "Jose"}}))
        assert len(result) == 1
        assert result[0]["name"] == "José María"

        # Test search with special characters
        result = list(self.collection.find({"$text": {"$search": "Björk"}}))
        assert len(result) == 1
        assert result[0]["name"] == "Björk Guðmundsdóttir"

    def test_diacritic_insensitive_search(self):
        """Test that searches are insensitive to diacritics."""
        # Insert test documents
        self.collection.insert_many(
            [
                {"title": "Café", "content": "A place to drink coffee"},
                {"title": " naive", "content": "Simple approach"},
                {"title": "Resume", "content": "Professional summary"},
                {"title": "Résumé", "content": "Professional document"},
            ]
        )

        # Search for base characters should match accented ones
        result = list(self.collection.find({"$text": {"$search": "cafe"}}))
        assert len(result) == 1
        assert result[0]["title"] == "Café"

        # Search for accented characters
        result = list(self.collection.find({"$text": {"$search": "résumé"}}))
        assert len(result) == 2  # Should match both Resume and Résumé
        titles = [doc["title"] for doc in result]
        assert "Resume" in titles
        assert "Résumé" in titles

    def test_unicode_normalization(self):
        """Test proper Unicode normalization in search."""
        # Insert test documents with various Unicode forms
        self.collection.insert_many(
            [
                {"text": "ℕ ⊆ ℝ ⊂ ℂ", "description": "Mathematical sets"},
                {"text": "ℕ ⊆ ℝ ⊂ ℂ", "description": "Same mathematical sets"},
                {"text": "Normal text", "description": "Regular ASCII text"},
            ]
        )

        # Search for Unicode characters
        result = list(self.collection.find({"$text": {"$search": "ℕ"}}))
        assert len(result) == 2
        # Both documents should have "mathematical sets" in their description (case insensitive)
        assert all(
            "mathematical sets" in doc["description"].lower() for doc in result
        )

    def test_nested_field_search(self):
        """Test text search in nested document fields."""
        # Insert test documents with nested fields
        self.collection.insert_many(
            [
                {
                    "name": "Project Alpha",
                    "details": {"description": "Python-based project"},
                },
                {
                    "name": "Project Beta",
                    "details": {"description": "Java-based project"},
                },
                {
                    "name": "Project Gamma",
                    "details": {"description": "JavaScript-based project"},
                },
            ]
        )

        # Search in nested fields
        result = list(self.collection.find({"$text": {"$search": "python"}}))
        assert len(result) == 1
        assert result[0]["name"] == "Project Alpha"

    def test_array_field_search(self):
        """Test text search in array fields."""
        # Insert test documents with array fields
        self.collection.insert_many(
            [
                {
                    "name": "Developer Skills",
                    "skills": ["Python", "Django", "PostgreSQL"],
                },
                {
                    "name": "Frontend Skills",
                    "skills": ["JavaScript", "React", "CSS"],
                },
                {
                    "name": "DevOps Skills",
                    "skills": ["Docker", "Kubernetes", "Python"],
                },
            ]
        )

        # Search in array fields
        result = list(self.collection.find({"$text": {"$search": "python"}}))
        assert len(result) == 2
        names = [doc["name"] for doc in result]
        assert "Developer Skills" in names
        assert "DevOps Skills" in names

    def test_special_characters_in_search_term(self):
        """Test handling of special regex characters in search terms."""
        # Insert test documents
        self.collection.insert_many(
            [
                {"text": "Price: $100", "description": "Item costs $100"},
                {
                    "text": "Email: user@example.com",
                    "description": "Contact email",
                },
                {"text": "Pattern: [a-z]+", "description": "Regex pattern"},
            ]
        )

        # Search for terms with special characters
        result = list(self.collection.find({"$text": {"$search": "$100"}}))
        assert len(result) == 1
        assert result[0]["text"] == "Price: $100"

        result = list(
            self.collection.find({"$text": {"$search": "user@example.com"}})
        )
        assert len(result) == 1
        assert result[0]["text"] == "Email: user@example.com"

        result = list(self.collection.find({"$text": {"$search": "[a-z]+"}}))
        assert len(result) == 1
        assert result[0]["text"] == "Pattern: [a-z]+"

    def test_empty_search_term(self):
        """Test behavior with empty search terms."""
        # Insert test documents
        self.collection.insert_many(
            [
                {"text": "Hello world", "description": "Greeting"},
                {"text": "Goodbye world", "description": "Farewell"},
            ]
        )

        # Empty search should ideally match nothing or all (implementation dependent)
        result = list(self.collection.find({"$text": {"$search": ""}}))
        # We expect this to be handled gracefully
        assert isinstance(result, list)

    def test_aggregation_pipeline_text_search(self):
        """Test text search in aggregation pipelines."""
        # Insert test documents
        self.collection.insert_many(
            [
                {
                    "name": "Active Python Project",
                    "status": "active",
                    "description": "Python development",
                },
                {
                    "name": "Inactive Python Project",
                    "status": "inactive",
                    "description": "Python maintenance",
                },
                {
                    "name": "Active Java Project",
                    "status": "active",
                    "description": "Java development",
                },
            ]
        )

        # Test pipeline with text search
        pipeline = [
            {"$match": {"status": "active"}},
            {"$match": {"$text": {"$search": "python"}}},
        ]

        result = list(self.collection.aggregate(pipeline))
        assert len(result) == 1
        assert result[0]["name"] == "Active Python Project"

    def test_performance_comparison(self):
        """Test that enhanced search doesn't significantly degrade performance."""
        # Insert a larger dataset
        docs = []
        for i in range(100):
            docs.append(
                {
                    "name": f"Document {i}",
                    "content": f"This is content with {'Python' if i % 3 == 0 else 'Java'} reference",
                    "status": "active" if i % 2 == 0 else "inactive",
                }
            )

        self.collection.insert_many(docs)

        # Time the search operation
        import time

        start_time = time.time()
        result = list(self.collection.find({"$text": {"$search": "python"}}))
        end_time = time.time()

        # Should complete in reasonable time
        assert end_time - start_time < 1.0  # Less than 1 second
        assert len(result) > 0  # Should find some results


def test_text_search_function_unit():
    """Unit tests for the text search function directly."""
    from neosqlite.collection.text_search import unified_text_search

    # Test basic functionality
    doc = {"name": "Python Developer", "description": "Works with Python"}
    assert unified_text_search(doc, "python") == True
    assert unified_text_search(doc, "Python") == True
    assert unified_text_search(doc, "JAVA") == False

    # Test international characters
    doc = {"name": "José María", "location": "España"}
    assert unified_text_search(doc, "jose") == True
    assert unified_text_search(doc, "espana") == True

    # Test nested documents
    doc = {"user": {"name": "Alice", "skills": ["Python", "Django"]}}
    assert unified_text_search(doc, "python") == True
    assert unified_text_search(doc, "django") == True

    # Test arrays
    doc = {"tags": ["Python", "Web", "Development"]}
    assert unified_text_search(doc, "python") == True
    assert unified_text_search(doc, "web") == True


if __name__ == "__main__":
    pytest.main([__file__])
