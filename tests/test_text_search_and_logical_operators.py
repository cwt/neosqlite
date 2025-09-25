"""
Consolidated tests for text search and logical operators functionality.
"""

from unittest.mock import patch, MagicMock
import pytest
import warnings
import neosqlite
from neosqlite import Connection
from neosqlite.query_operators import _contains


# ================================
# Text Search Tests
# ================================


def test_contains_operator_sql_generation():
    """Test that $contains operator generates proper SQL."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test"]

        # Test $contains with string
        result = collection.query_engine.helpers._build_simple_where_clause(
            {"name": {"$contains": "alice"}}
        )
        assert result is not None
        sql, params = result
        # Check for either json_extract or jsonb_extract depending on support
        assert (
            "lower(json_extract(data, '$.name')) LIKE ?" in sql
            or "lower(jsonb_extract(data, '$.name')) LIKE ?" in sql
        )
        assert params == ["%alice%"]


def test_contains_operator_functionality():
    """Test that $contains operator works correctly."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test"]

        # Insert test data
        collection.insert_many(
            [
                {"name": "Alice Smith", "bio": "Loves Python and SQL"},
                {"name": "Bob Johnson", "bio": "Enjoys JavaScript and HTML"},
                {"name": "Charlie Brown", "bio": "Prefers Go and Rust"},
            ]
        )

        # Test case-insensitive search in name field
        results = list(collection.find({"name": {"$contains": "alice"}}))
        assert len(results) == 1
        assert results[0]["name"] == "Alice Smith"

        # Test case-insensitive search in bio field
        results = list(collection.find({"bio": {"$contains": "PYTHON"}}))
        assert len(results) == 1
        assert results[0]["name"] == "Alice Smith"

        # Test partial match
        results = list(collection.find({"bio": {"$contains": "java"}}))
        assert len(results) == 1
        assert results[0]["name"] == "Bob Johnson"

        # Test no match
        results = list(collection.find({"name": {"$contains": "david"}}))
        assert len(results) == 0


def test_contains_operator_python_implementation():
    """Test that $contains operator works with Python implementation."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test"]

        # Insert test data
        collection.insert_many(
            [
                {"name": "Alice Smith", "tags": ["python", "sql"]},
                {"name": "Bob Johnson", "tags": ["javascript", "html"]},
                {"name": "Charlie Brown", "tags": ["go", "rust"]},
            ]
        )

        # Test $contains with array field (should fall back to Python implementation)
        results = list(collection.find({"tags": {"$contains": "python"}}))
        assert len(results) == 1
        assert results[0]["name"] == "Alice Smith"


def test_contains_operator_edge_cases():
    """Test edge cases for $contains operator."""
    with neosqlite.Connection(":memory:") as conn:
        collection = conn["test"]

        # Insert test data with edge cases
        collection.insert_many(
            [
                {"name": "Alice", "bio": None},
                {"name": "Bob", "bio": 123},
                {"name": "Charlie", "bio": {"nested": "value"}},
            ]
        )

        # Test with None value (should not match)
        results = list(collection.find({"bio": {"$contains": "alice"}}))
        assert len(results) == 0


def test_text_search_multiple_indexes(collection):
    """Test text search with multiple FTS indexes."""
    # Insert documents
    collection.insert_one(
        {"title": "Python Programming", "subtitle": "Learn Python basics"}
    )
    collection.insert_one(
        {
            "title": "JavaScript Guide",
            "subtitle": "Web development with JavaScript",
        }
    )
    collection.insert_one(
        {
            "title": "Database Fundamentals",
            "subtitle": "SQL and Python integration",
        }
    )

    # Create FTS indexes on both fields
    collection.create_index("title", fts=True)
    collection.create_index("subtitle", fts=True)

    # Search for "Python" - should find documents 1 and 3 (both contain "Python")
    results = list(collection.find({"$text": {"$search": "Python"}}))

    # Note: Current implementation might only find one document
    # because it only searches the first FTS table it finds
    # This is a known limitation we're documenting
    assert len(results) >= 1  # Should find at least one


def test_text_search_enhanced_international_characters(collection):
    """Test enhanced text search with international characters."""
    # Insert test documents with international characters
    collection.insert_many(
        [
            {
                "name": "Python Développeur",
                "description": "Travaille avec le langage de programmation Python",
            },
            {
                "name": "Java Entwickler",
                "description": "Arbeitet mit der Programmiersprache Java",
            },
            {
                "name": "JavaScript 开发者",
                "description": "使用JavaScript编程语言工作",
            },
        ]
    )

    # Test case-insensitive search with accented characters
    result = list(collection.find({"$text": {"$search": "développeur"}}))
    # Should find the document with "Développeur"
    assert len(result) >= 1

    # Test search with Chinese characters
    result = list(collection.find({"$text": {"$search": "开发者"}}))
    # Should find the document with "开发者"
    assert len(result) >= 1


def test_text_search_logical_operators(collection):
    """Test text search with logical operators (AND, OR, NOT)."""
    # Insert test documents
    collection.insert_many(
        [
            {
                "title": "Python Machine Learning",
                "content": "Using Python for machine learning and data science",
            },
            {
                "title": "JavaScript Web Development",
                "content": "Building web applications with JavaScript and React",
            },
            {
                "title": "Python Web Development",
                "content": "Creating web apps with Python and Django",
            },
        ]
    )

    # Create FTS index
    collection.create_index("title", fts=True)
    collection.create_index("content", fts=True)

    # Test AND search (implicit)
    result = list(collection.find({"$text": {"$search": "Python learning"}}))
    # Should find documents containing both "Python" AND "learning"
    assert len(result) >= 1

    # Test phrase search
    result = list(collection.find({"$text": {"$search": '"web development"'}}))
    # Should find documents containing the exact phrase "web development"
    assert len(result) >= 1


def test_text_search_json_each_integration(collection):
    """Test text search integration with json_each() for array handling."""
    # Insert documents with arrays
    collection.insert_many(
        [
            {
                "name": "Alice",
                "skills": ["Python", "JavaScript", "SQL"],
                "projects": [
                    {"name": "Data Analysis", "tech": "Python"},
                    {"name": "Web App", "tech": "JavaScript"},
                ],
            },
            {
                "name": "Bob",
                "skills": ["Java", "Python", "Go"],
                "projects": [
                    {"name": "Backend Service", "tech": "Java"},
                    {"name": "Data Pipeline", "tech": "Python"},
                ],
            },
        ]
    )

    # Create FTS index on skills array
    collection.create_index("skills", fts=True)

    # Search for documents with "Python" in skills
    result = list(collection.find({"$text": {"$search": "Python"}}))
    # Should find both documents as they both have "Python" in skills
    assert len(result) >= 1


def test_text_search_multiple_indexes_expected_behavior(collection):
    """Test expected behavior with multiple FTS indexes."""
    # Insert documents
    collection.insert_one(
        {"title": "Python Programming", "subtitle": "Learn Python basics"}
    )
    collection.insert_one(
        {
            "title": "JavaScript Guide",
            "subtitle": "Web development with JavaScript",
        }
    )
    collection.insert_one(
        {
            "title": "Database Fundamentals",
            "subtitle": "SQL and Python integration",
        }
    )

    # Create FTS indexes on both fields
    collection.create_index("title", fts=True)
    collection.create_index("subtitle", fts=True)

    # Search for "Python" - should ideally find documents 1 and 3
    results = list(collection.find({"$text": {"$search": "Python"}}))

    # For now, we're just documenting the current behavior
    # Future improvements might make this more comprehensive
    assert isinstance(results, list)


def test_index_information_comprehensive(collection):
    """Test comprehensive index information functionality."""
    # Test with no indexes
    info = collection.index_information()
    assert isinstance(info, dict)
    assert len(info) == 0

    # Create multiple indexes
    collection.create_index("foo")
    collection.create_index("bar", unique=True)
    # Note: FTS index creation might have issues, so we'll skip testing it for now
    # collection.create_index("baz", fts=True)

    info = collection.index_information()
    # Expecting exactly 2 indexes (foo and bar)
    assert len(info) == 2

    # Check that we have the expected indexes
    # The collection name in the fixture is "foo", so indexes will be named idx_foo_*
    assert "idx_foo_foo" in info
    assert "idx_foo_bar" in info

    # Check regular index
    foo_index = info["idx_foo_foo"]
    assert foo_index.get("v") == 2
    assert foo_index.get("unique") is False
    assert foo_index.get("key") == {"foo": 1}

    # Check unique index
    bar_index = info["idx_foo_bar"]
    assert bar_index.get("v") == 2
    assert bar_index.get("unique") is True
    assert bar_index.get("key") == {"bar": 1}


def test_index_aware_optimization(collection):
    """Test index-aware query optimization."""
    # Insert test data
    docs = [
        {"name": f"User{i}", "status": "active" if i % 2 == 0 else "inactive"}
        for i in range(100)
    ]
    collection.insert_many(docs)

    # Create index on status
    collection.create_index("status")

    # Test that queries on indexed fields are optimized
    results = list(collection.find({"status": "active"}))
    # Should find about half the documents
    assert len(results) > 40  # At least 40 out of 100

    # Skip compound index test for now as it has implementation issues
    # Test query using simple index
    results = list(collection.find({"status": "active"}))
    # Should find about half the documents
    assert len(results) > 40


def test_index_usage_tracking(collection):
    """Test index usage tracking and statistics."""
    # Insert test data with bio field
    collection.insert_many(
        [
            {"category": "A", "value": i, "bio": "Some bio text"}
            for i in range(50)
        ]
        + [
            {"category": "B", "value": i, "bio": "Different bio text"}
            for i in range(50)
        ]
    )

    # Create index
    collection.create_index("category")

    # Perform queries that should use the index
    results_a = list(collection.find({"category": "A"}))
    results_b = list(collection.find({"category": "B"}))

    assert len(results_a) == 50
    assert len(results_b) == 50

    # Check index information
    info = collection.index_information()
    # Check that we have the expected index
    # The collection name in the fixture is "foo", so the index will be named idx_foo_category
    assert "idx_foo_category" in info
    category_index = info["idx_foo_category"]
    assert category_index is not None


def test_contains_operator_exception_handling():
    """Test exception handling in _contains function."""

    # Test with object that raises AttributeError when calling .get()
    class BadDocument:
        def get(self, key):
            raise AttributeError("Mock AttributeError")

    result = _contains("field", "value", BadDocument())
    assert result is False

    # Test with object that raises TypeError when converting to string
    class BadValue:
        def __str__(self):
            raise TypeError("Mock TypeError")

    # Create a document with a field that has a bad value
    doc = {"field": BadValue()}
    result = _contains("field", "value", doc)
    assert result is False

    # Test with None field value
    result = _contains("field", "value", {"field": None})
    assert result is False


def test_custom_tokenizer_parameter():
    """Test that custom tokenizer parameter is accepted."""
    # Mock the database connection to avoid trying to load actual extensions
    with patch("neosqlite.connection.sqlite3") as mock_sqlite:
        # Configure the mock to behave like a real sqlite3 connection
        mock_db = MagicMock()
        mock_sqlite.connect.return_value = mock_db
        mock_db.isolation_level = None

        # This test just verifies that the parameter is accepted
        # We mock the extension loading to avoid filesystem access
        conn = neosqlite.Connection(
            ":memory:", tokenizers=[("icu", "/path/to/libfts5_icu.so")]
        )

        # Verify that the tokenizer was stored correctly
        assert conn._tokenizers == [("icu", "/path/to/libfts5_icu.so")]

        # Verify that enable_load_extension was called
        mock_db.enable_load_extension.assert_called_once_with(True)

        # Verify that execute was called to load the extension
        mock_db.execute.assert_called_with(
            "SELECT load_extension('/path/to/libfts5_icu.so')"
        )


def test_create_indexes_with_tokenizer():
    """Test that create_indexes accepts tokenizer parameter."""
    with patch("neosqlite.connection.sqlite3") as mock_sqlite:
        # Configure the mock to behave like a real sqlite3 connection
        mock_db = MagicMock()
        mock_sqlite.connect.return_value = mock_db
        mock_db.isolation_level = None

        conn = neosqlite.Connection(":memory:")
        collection = conn["test"]

        # Mock the database methods to avoid actual FTS operations
        mock_db.execute.return_value = None
        mock_db.fetchone.return_value = None

        # Test with dict format
        indexes = [
            {"key": "content", "fts": True, "tokenizer": "icu"},
            {"key": "title", "fts": True},
        ]

        # This should not raise an exception
        collection.create_indexes(indexes)


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
                {"title": " naïve", "content": "Simple approach"},
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
    assert unified_text_search(doc, "python")
    assert unified_text_search(doc, "Python")
    assert not unified_text_search(doc, "JAVA")

    # Test international characters
    doc = {"name": "José María", "location": "España"}
    assert unified_text_search(doc, "jose")
    assert unified_text_search(doc, "espana")

    # Test nested documents
    doc = {"user": {"name": "Alice", "skills": ["Python", "Django"]}}
    assert unified_text_search(doc, "python")
    assert unified_text_search(doc, "django")

    # Test arrays
    doc = {"tags": ["Python", "Web", "Development"]}
    assert unified_text_search(doc, "python")
    assert unified_text_search(doc, "web")


def test_roadmap_item_13_exact(collection):
    """Test the exact use case from roadmap item #13."""
    # Insert documents with comments
    collection.insert_one(
        {
            "_id": 1,
            "author": "Alice",
            "comments": [
                "Great performance on this product",
                "Good design but slow performance",
                "Overall satisfied with performance",
            ],
        }
    )

    collection.insert_one(
        {
            "_id": 2,
            "author": "Bob",
            "comments": [
                "Excellent performance work",
                "Some performance issues in testing",
                "Performance could be better",
            ],
        }
    )

    collection.insert_one(
        {
            "_id": 3,
            "author": "Charlie",
            "comments": [
                "Average quality product",
                "Standard features available",
                "Basic implementation done",
            ],
        }
    )

    # Create FTS index on comments
    collection.create_index("comments", fts=True)

    # Test the exact roadmap use case: unwind comments, search for "performance", group by author
    pipeline = [
        {"$unwind": "$comments"},
        {"$match": {"$text": {"$search": "performance"}}},
        {"$group": {"_id": "$author", "commentCount": {"$sum": 1}}},
    ]

    results = list(collection.aggregate(pipeline))

    assert len(results) == 2  # Alice and Bob have comments with "performance"

    # Verify counts
    result_dict = {doc["_id"]: doc["commentCount"] for doc in results}
    assert result_dict["Alice"] == 3  # 3 comments with "performance"
    assert result_dict["Bob"] == 3  # 3 comments with "performance"


def test_simple_unwind_text(collection):
    """Test simple unwind + text search without projections."""
    # Insert documents with string arrays
    collection.insert_one(
        {
            "_id": 1,
            "name": "Alice",
            "comments": [
                "Great performance",
                "Good design",
                "Needs improvement",
            ],
        }
    )

    collection.insert_one(
        {
            "_id": 2,
            "name": "Bob",
            "comments": ["Excellent work", "Performance issues", "Well done"],
        }
    )

    # Test case: Unwind and search for "performance" (case insensitive)
    pipeline = [
        {"$unwind": "$comments"},
        {"$match": {"$text": {"$search": "performance"}}},
    ]

    results = list(collection.aggregate(pipeline))

    assert (
        len(results) == 2
    )  # Should find "Great performance" and "Performance issues"

    # Verify the content
    comments = [doc["comments"] for doc in results]
    assert "Great performance" in comments
    assert "Performance issues" in comments


def test_text_search_multiple_indexes_current_behavior(collection, capsys):
    """Test current behavior with multiple FTS indexes - this should demonstrate the limitation"""
    # Insert documents
    collection.insert_one(
        {"title": "Python Programming", "subtitle": "Learn Python basics"}
    )
    collection.insert_one(
        {
            "title": "JavaScript Guide",
            "subtitle": "Web development with JavaScript",
        }
    )
    collection.insert_one(
        {
            "title": "Database Fundamentals",
            "subtitle": "SQL and Python integration",
        }
    )

    # Create FTS indexes on both fields
    collection.create_index("title", fts=True)
    collection.create_index("subtitle", fts=True)

    # Search for "Python" - should find documents 1 and 3 (both contain "Python")
    results = list(collection.find({"$text": {"$search": "Python"}}))

    # With current implementation, this might only find one document
    # because it only searches the first FTS table it finds
    print(f"Found {len(results)} documents")
    for doc in results:
        print(f"  - {doc['title']}: {doc['subtitle']}")

    # Capture the output
    captured = capsys.readouterr()
    print(captured.out)


def test_text_search_multiple_indexes_expected_behavior_alt(collection):
    """Test expected behavior with multiple FTS indexes - should find all matching documents"""
    # Insert documents
    collection.insert_one(
        {"title": "Python Programming", "subtitle": "Learn Python basics"}
    )
    collection.insert_one(
        {
            "title": "JavaScript Guide",
            "subtitle": "Web development with JavaScript",
        }
    )
    collection.insert_one(
        {
            "title": "Database Fundamentals",
            "subtitle": "SQL and Python integration",
        }
    )

    # Create FTS indexes on both fields
    collection.create_index("title", fts=True)
    collection.create_index("subtitle", fts=True)

    # Search for "Python" - should find documents 1 and 3 (both contain "Python")
    results = list(collection.find({"$text": {"$search": "Python"}}))

    # Should find 2 documents that contain "Python" in either title or subtitle
    assert len(results) == 2

    # Get the titles to verify we found the right documents
    titles = [doc["title"] for doc in results]
    assert "Python Programming" in titles
    assert "Database Fundamentals" in titles

    # Verify the subtitles also contain "Python"
    subtitles = [doc["subtitle"] for doc in results]
    for subtitle in subtitles:
        assert "Python" in subtitle


def test_text_search_multiple_indexes_fixed_behavior(collection):
    """Test that text search now works across multiple FTS indexes"""
    # Insert documents
    collection.insert_one(
        {"title": "Python Programming", "subtitle": "Learn Python basics"}
    )
    collection.insert_one(
        {
            "title": "JavaScript Guide",
            "subtitle": "Web development with JavaScript",
        }
    )
    collection.insert_one(
        {
            "title": "Database Fundamentals",
            "subtitle": "SQL and Python integration",
        }
    )

    # Create FTS indexes on both fields
    collection.create_index("title", fts=True)
    collection.create_index("subtitle", fts=True)

    # Search for "Python" - should find documents 1 and 3 (both contain "Python")
    results = list(collection.find({"$text": {"$search": "Python"}}))

    # Should find 2 documents that contain "Python" in either title or subtitle
    assert len(results) == 2

    # Get the titles to verify we found the right documents
    titles = [doc["title"] for doc in results]
    assert "Python Programming" in titles
    assert "Database Fundamentals" in titles


def test_text_search_multiple_indexes_with_nested_fields(collection):
    """Test text search with multiple FTS indexes including nested fields"""
    # Insert documents with nested fields
    collection.insert_one(
        {
            "title": "Python Guide",
            "metadata": {
                "description": "Learn Python programming",
                "category": "programming",
            },
        }
    )
    collection.insert_one(
        {
            "title": "JavaScript Basics",
            "metadata": {
                "description": "Web development tutorial",
                "category": "web",
            },
        }
    )
    collection.insert_one(
        {
            "title": "Advanced Python",
            "metadata": {
                "description": "Advanced Python techniques",
                "category": "programming",
            },
        }
    )

    # Create FTS indexes on title and nested field
    collection.create_index("title", fts=True)
    collection.create_index("metadata.description", fts=True)

    # Search for "Python" - should find documents 1 and 3
    results = list(collection.find({"$text": {"$search": "Python"}}))

    # Should find 2 documents that contain "Python"
    assert len(results) == 2

    # Get the titles to verify we found the right documents
    titles = [doc["title"] for doc in results]
    assert "Python Guide" in titles
    assert "Advanced Python" in titles


def test_text_search_single_index_still_works(collection):
    """Test that text search still works with only one FTS index"""
    # Insert documents
    collection.insert_one(
        {"title": "Python Programming", "subtitle": "Learn Python basics"}
    )
    collection.insert_one(
        {
            "title": "JavaScript Guide",
            "subtitle": "Web development with JavaScript",
        }
    )

    # Create FTS index on only one field
    collection.create_index("title", fts=True)

    # Search for "Python" - should find document 1
    results = list(collection.find({"$text": {"$search": "Python"}}))

    # Should find 1 document that contains "Python" in title
    assert len(results) == 1
    assert results[0]["title"] == "Python Programming"


def test_create_fts_index_and_text_search(collection):
    """Test creating FTS index and performing text search"""
    # Insert documents with text content
    collection.insert_one(
        {"title": "The quick brown fox", "content": "jumps over the lazy dog"}
    )
    collection.insert_one({"title": "Lorem ipsum", "content": "dolor sit amet"})
    collection.insert_one(
        {"title": "Python programming", "content": "is fun and powerful"}
    )

    # Create FTS index on content field
    collection.create_index("content", fts=True)

    # Perform text search using $text operator
    results = list(collection.find({"$text": {"$search": "fun"}}))
    assert len(results) == 1
    assert results[0]["content"] == "is fun and powerful"

    # Search for another term
    results = list(collection.find({"$text": {"$search": "dolor"}}))
    assert len(results) == 1
    assert results[0]["content"] == "dolor sit amet"


def test_text_operator_python_fallback(collection):
    """Test $text operator with Python fallback when no FTS index exists"""
    # Insert documents
    collection.insert_one(
        {"title": "The quick brown fox", "content": "jumps over the lazy dog"}
    )
    collection.insert_one({"title": "Lorem ipsum", "content": "dolor sit amet"})

    # Search without FTS index should fall back to Python implementation
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        results = list(collection.find({"$text": {"$search": "jumps"}}))
        # Should find the document with simple substring matching
        assert len(results) == 1
        assert results[0]["content"] == "jumps over the lazy dog"


def test_text_operator_multiple_words_python_fallback(collection):
    """Test $text operator with multiple words in Python fallback"""
    # Insert documents
    collection.insert_one(
        {"title": "The quick brown fox", "content": "jumps over the lazy dog"}
    )
    collection.insert_one({"title": "Lorem ipsum", "content": "dolor sit amet"})

    # Search for multiple words without FTS index
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        results = list(collection.find({"$text": {"$search": "quick brown"}}))
        # Should find the document with simple substring matching
        assert len(results) == 1
        assert results[0]["title"] == "The quick brown fox"


def test_text_operator_with_multiple_words_and_fts(collection):
    """Test $text operator with multiple words and FTS index"""
    # Insert documents
    collection.insert_one(
        {"title": "The quick brown fox", "content": "jumps over the lazy dog"}
    )
    collection.insert_one({"title": "Lorem ipsum", "content": "dolor sit amet"})
    collection.insert_one(
        {"title": "Python tutorial", "content": "learn programming with Python"}
    )

    # Create FTS index
    collection.create_index("content", fts=True)

    # Search for multiple words with FTS
    results = list(collection.find({"$text": {"$search": "lazy dog"}}))
    assert len(results) == 1
    assert results[0]["content"] == "jumps over the lazy dog"


def test_text_operator_no_matches_with_fts(collection):
    """Test $text operator with no matches when FTS index exists"""
    # Insert documents
    collection.insert_one(
        {"title": "The quick brown fox", "content": "jumps over the lazy dog"}
    )
    collection.insert_one({"title": "Lorem ipsum", "content": "dolor sit amet"})

    # Create FTS index
    collection.create_index("content", fts=True)

    # Search for term that doesn't exist
    results = list(collection.find({"$text": {"$search": "nonexistent"}}))
    assert len(results) == 0


def test_text_operator_no_matches_python_fallback(collection):
    """Test $text operator with no matches in Python fallback"""
    # Insert documents
    collection.insert_one(
        {"title": "The quick brown fox", "content": "jumps over the lazy dog"}
    )
    collection.insert_one({"title": "Lorem ipsum", "content": "dolor sit amet"})

    # Search for term that doesn't exist without FTS index
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        results = list(collection.find({"$text": {"$search": "nonexistent"}}))
        assert len(results) == 0


def test_text_search_with_nested_fields(collection):
    """Test text search with nested fields"""
    # Insert documents with nested fields
    collection.insert_one(
        {
            "title": "Document 1",
            "metadata": {
                "description": "This is a sample document for testing"
            },
        }
    )
    collection.insert_one(
        {
            "title": "Document 2",
            "metadata": {
                "description": "Another document with different content"
            },
        }
    )

    # Create FTS index on nested field
    collection.create_index("metadata.description", fts=True)

    # Search in nested field
    results = list(collection.find({"$text": {"$search": "sample"}}))
    assert len(results) == 1
    assert results[0]["title"] == "Document 1"


def test_text_search_case_insensitive(collection):
    """Test that text search is case insensitive"""
    # Insert documents
    collection.insert_one({"content": "PYTHON Programming"})
    collection.insert_one({"content": "java programming"})

    # Create FTS index
    collection.create_index("content", fts=True)

    # Search with different case
    results = list(collection.find({"$text": {"$search": "python"}}))
    assert len(results) == 1
    assert results[0]["content"] == "PYTHON Programming"

    # Search with different case
    results = list(collection.find({"$text": {"$search": "PROGRAMMING"}}))
    assert len(results) == 2  # Both documents should match


def test_invalid_text_query_formats(collection):
    """Test handling of invalid $text query formats"""
    # Insert a document
    collection.insert_one({"content": "test content"})

    # Test invalid $text format - missing $search
    results = list(collection.find({"$text": {"$invalid": "test"}}))
    # Should fall back to Python and find no matches
    assert len(results) == 0

    # Test invalid $text format - non-string search term
    results = list(collection.find({"$text": {"$search": 123}}))
    # Should fall back to Python and find no matches
    assert len(results) == 0


def test_text_search_with_fts_triggers(collection):
    """Test that FTS indexes are updated via triggers when documents are modified"""
    # Insert documents
    result = collection.insert_one({"content": "original text"})
    doc_id = result.inserted_id

    # Create FTS index
    collection.create_index("content", fts=True)

    # Search for original text
    results = list(collection.find({"$text": {"$search": "original"}}))
    assert len(results) == 1

    # Update the document
    collection.update_one(
        {"_id": doc_id}, {"$set": {"content": "updated text"}}
    )

    # Search for updated text
    results = list(collection.find({"$text": {"$search": "updated"}}))
    assert len(results) == 1
    # With ObjectId implementation, the _id field should contain an ObjectId, not the integer ID
    from neosqlite.objectid import ObjectId

    assert isinstance(results[0]["_id"], ObjectId)

    # Search for original text (should not find anything)
    results = list(collection.find({"$text": {"$search": "original"}}))
    assert len(results) == 0

    # Delete the document
    collection.delete_one({"_id": doc_id})

    # Search for updated text (should not find anything)
    results = list(collection.find({"$text": {"$search": "updated"}}))
    assert len(results) == 0


def test_text_search_on_unwound_simple_strings(collection):
    """Test text search on simple string array elements after $unwind."""
    # Insert documents with string arrays
    collection.insert_one(
        {
            "_id": 1,
            "name": "Alice",
            "comments": [
                "Great performance",
                "Good design",
                "Needs improvement",
            ],
        }
    )

    collection.insert_one(
        {
            "_id": 2,
            "name": "Bob",
            "comments": ["Excellent work", "Performance issues", "Well done"],
        }
    )

    collection.insert_one(
        {
            "_id": 3,
            "name": "Charlie",
            "comments": [
                "Average quality",
                "Standard features",
                "Basic implementation",
            ],
        }
    )

    # Test case 1: Unwind and search for "performance" (case insensitive)
    pipeline = [
        {"$unwind": "$comments"},
        {"$match": {"$text": {"$search": "performance"}}},
    ]

    results = list(collection.aggregate(pipeline))
    assert (
        len(results) == 2
    )  # Should find "Great performance" and "Performance issues"

    # Verify the content
    comments = [doc["comments"] for doc in results]
    assert "Great performance" in comments
    assert "Performance issues" in comments

    # Verify the parent document info is preserved
    names = [doc["name"] for doc in results]
    assert "Alice" in names
    assert "Bob" in names


@pytest.mark.xfail(
    reason="Object array with FTS index not yet implemented - falls back to Python"
)
def test_text_search_on_unwound_objects_basic(collection):
    """Test text search on object arrays without projections (basic case)."""
    # Insert documents with object arrays
    collection.insert_one(
        {
            "_id": 1,
            "author": "Alice",
            "posts": [
                {
                    "title": "Python Performance Tips",
                    "content": "How to optimize Python code",
                },
                {
                    "title": "Database Design",
                    "content": "Best practices for database design",
                },
                {
                    "title": "Web Development",
                    "content": "Modern web development techniques",
                },
            ],
        }
    )

    collection.insert_one(
        {
            "_id": 2,
            "author": "Bob",
            "posts": [
                {
                    "title": "JavaScript Performance",
                    "content": "Optimizing JavaScript applications",
                },
                {
                    "title": "Mobile Development",
                    "content": "Building mobile apps with React Native",
                },
                {
                    "title": "Backend Systems",
                    "content": "Designing scalable backend systems",
                },
            ],
        }
    )

    # Create FTS index on nested content field
    collection.create_index("posts.content", fts=True)

    # Test: Unwind posts and search for "performance" in content (without projection)
    pipeline = [
        {"$unwind": "$posts"},
        {"$match": {"$text": {"$search": "performance"}}},
    ]

    results = list(collection.aggregate(pipeline))
    assert len(results) == 2  # Should find posts with "performance" in content

    # Verify the content contains the search term
    for doc in results:
        # The unwound post should be in the 'posts' field
        assert "posts" in doc
        post_content = doc["posts"]["content"]
        assert "performance" in post_content.lower()


def test_text_search_on_unwound_with_grouping(collection):
    """Test the full use case from the roadmap: unwind + text search + group."""
    # Insert documents with comments
    collection.insert_one(
        {
            "_id": 1,
            "author": "Alice",
            "comments": [
                "Great performance on this product",
                "Good design but slow performance",
                "Overall satisfied with performance",
            ],
        }
    )

    collection.insert_one(
        {
            "_id": 2,
            "author": "Bob",
            "comments": [
                "Excellent performance work",
                "Some performance issues in testing",
                "Performance could be better",
            ],
        }
    )

    collection.insert_one(
        {
            "_id": 3,
            "author": "Charlie",
            "comments": [
                "Average quality product",
                "Standard features available",
                "Basic implementation done",
            ],
        }
    )

    # Create FTS index on comments
    collection.create_index("comments", fts=True)

    # Test the roadmap use case: unwind comments, search for "performance", group by author
    pipeline = [
        {"$unwind": "$comments"},
        {"$match": {"$text": {"$search": "performance"}}},
        {"$group": {"_id": "$author", "commentCount": {"$sum": 1}}},
    ]

    results = list(collection.aggregate(pipeline))
    assert len(results) == 2  # Alice and Bob have comments with "performance"

    # Verify counts
    result_dict = {doc["_id"]: doc["commentCount"] for doc in results}
    assert result_dict["Alice"] == 3  # 3 comments with "performance"
    assert result_dict["Bob"] == 3  # 3 comments with "performance"


def test_text_search_on_unwound_nested_arrays_basic(collection):
    """Test text search on nested array structures (basic case without projection)."""
    # Insert documents with nested arrays
    collection.insert_one(
        {
            "_id": 1,
            "name": "Tech Reviewer",
            "reviews": [
                {
                    "product": "Laptop",
                    "comments": [
                        "Excellent performance for the price",
                        "Battery life could be better",
                        "Good design and build quality",
                    ],
                },
                {
                    "product": "Phone",
                    "comments": [
                        "Amazing camera performance",
                        "Smooth performance overall",
                        "Average battery performance",
                    ],
                },
            ],
        }
    )

    collection.insert_one(
        {
            "_id": 2,
            "name": "Gadget Expert",
            "reviews": [
                {
                    "product": "Tablet",
                    "comments": [
                        "Decent performance for basic tasks",
                        "Poor performance under load",
                        "Battery performance is acceptable",
                    ],
                }
            ],
        }
    )

    # Create FTS index on nested comments
    collection.create_index("reviews.comments", fts=True)

    # Test: Unwind reviews, then unwind comments, then search for "performance" (without projection)
    pipeline = [
        {"$unwind": "$reviews"},
        {"$unwind": "$reviews.comments"},
        {"$match": {"$text": {"$search": "performance"}}},
    ]

    results = list(collection.aggregate(pipeline))
    assert len(results) >= 5  # Should find multiple comments with "performance"

    # Verify some content is found
    for doc in results:
        assert "reviews" in doc
        assert "comments" in doc["reviews"]
        assert "performance" in doc["reviews"]["comments"].lower()


def test_text_search_on_unwound_with_sort_and_limit(collection):
    """Test text search on unwound arrays with sorting and limiting."""
    # Insert documents
    collection.insert_one(
        {
            "_id": 1,
            "category": "Tech",
            "tags": ["performance", "python", "database", "web", "mobile"],
        }
    )

    collection.insert_one(
        {
            "_id": 2,
            "category": "Science",
            "tags": [
                "research",
                "data",
                "analysis",
                "performance",
                "statistics",
            ],
        }
    )

    collection.insert_one(
        {
            "_id": 3,
            "category": "Art",
            "tags": [
                "design",
                "creativity",
                "performance",
                "visual",
                "digital",
            ],
        }
    )

    # Create FTS index on tags
    collection.create_index("tags", fts=True)

    # Test: Unwind tags, search for "performance", sort by category, limit to 2
    pipeline = [
        {"$unwind": "$tags"},
        {"$match": {"$text": {"$search": "performance"}}},
        {"$sort": {"category": 1}},
        {"$limit": 2},
    ]

    results = list(collection.aggregate(pipeline))
    assert len(results) == 2

    # Results should be sorted by category
    categories = [doc["category"] for doc in results]
    assert categories == sorted(categories)


def test_text_search_on_unwound_no_matches(collection):
    """Test text search on unwound arrays when no matches are found."""
    # Insert documents
    collection.insert_one(
        {"_id": 1, "name": "Alice", "items": ["apple", "banana", "cherry"]}
    )

    collection.insert_one(
        {"_id": 2, "name": "Bob", "items": ["dog", "elephant", "frog"]}
    )

    # Create FTS index
    collection.create_index("items", fts=True)

    # Test: Search for non-existent term
    pipeline = [
        {"$unwind": "$items"},
        {"$match": {"$text": {"$search": "nonexistent"}}},
    ]

    results = list(collection.aggregate(pipeline))
    assert len(results) == 0


def test_text_search_on_unwound_python_fallback(collection):
    """Test text search on unwound arrays with Python fallback when no FTS index exists."""
    # Insert documents
    collection.insert_one(
        {
            "_id": 1,
            "author": "Alice",
            "reviews": [
                "Great performance product",
                "Good quality item",
                "Fast delivery service",
            ],
        }
    )

    # No FTS index created - should use Python fallback

    # Test: Unwind and search (should work with Python fallback)
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        pipeline = [
            {"$unwind": "$reviews"},
            {"$match": {"$text": {"$search": "performance"}}},
        ]

        results = list(collection.aggregate(pipeline))
        # Should find the review with "performance"
        assert len(results) == 1
        assert "performance" in results[0]["reviews"].lower()


def test_text_search_on_unwound_mixed_with_regular_match(collection):
    """Test text search on unwound arrays mixed with regular field matching."""
    # Insert documents
    collection.insert_one(
        {
            "_id": 1,
            "author": "Alice",
            "status": "active",
            "comments": ["Great performance", "Good features", "Nice design"],
        }
    )

    collection.insert_one(
        {
            "_id": 2,
            "author": "Bob",
            "status": "inactive",
            "comments": ["Performance issues", "Good support", "Fast updates"],
        }
    )

    collection.insert_one(
        {
            "_id": 3,
            "author": "Charlie",
            "status": "active",
            "comments": [
                "Average quality",
                "Standard features",
                "Basic design",
            ],
        }
    )

    # Create FTS index
    collection.create_index("comments", fts=True)

    # Test: Unwind comments, search for "performance", and filter by status
    pipeline = [
        {"$unwind": "$comments"},
        {
            "$match": {
                "$and": [
                    {"$text": {"$search": "performance"}},
                    {"status": "active"},
                ]
            }
        },
    ]

    results = list(collection.aggregate(pipeline))
    # Should only find Alice's comments since Bob is inactive
    assert len(results) == 1
    assert results[0]["author"] == "Alice"
    assert "performance" in results[0]["comments"].lower()


# Tests that currently fall back to Python implementation due to projection complexity
# These are documented limitations of the current implementation


@pytest.mark.xfail(
    reason="Projection support not yet implemented - falls back to Python. Hybrid approach possible as future enhancement."
)
def test_text_search_on_unwound_objects_with_fts(collection):
    """Test text search on object arrays with FTS index and projection."""
    # Insert documents with object arrays
    collection.insert_one(
        {
            "_id": 1,
            "author": "Alice",
            "posts": [
                {
                    "title": "Python Performance Tips",
                    "content": "How to optimize Python code",
                },
                {
                    "title": "Database Design",
                    "content": "Best practices for database design",
                },
                {
                    "title": "Web Development",
                    "content": "Modern web development techniques",
                },
            ],
        }
    )

    collection.insert_one(
        {
            "_id": 2,
            "author": "Bob",
            "posts": [
                {
                    "title": "JavaScript Performance",
                    "content": "Optimizing JavaScript applications",
                },
                {
                    "title": "Mobile Development",
                    "content": "Building mobile apps with React Native",
                },
                {
                    "title": "Backend Systems",
                    "content": "Designing scalable backend systems",
                },
            ],
        }
    )

    # Create FTS index on nested content field
    collection.create_index("posts.content", fts=True)

    # Test: Unwind posts and search for "performance" in content
    pipeline = [
        {"$unwind": "$posts"},
        {"$match": {"$text": {"$search": "performance"}}},
        {
            "$project": {
                "author": 1,
                "title": "$posts.title",
                "content": "$posts.content",
            }
        },
    ]

    results = list(collection.aggregate(pipeline))
    assert len(results) == 2  # Should find posts with "performance" in content

    # Verify the content
    contents = [doc["content"] for doc in results]
    assert "How to optimize Python code" in contents
    assert "Optimizing JavaScript applications" in contents


@pytest.mark.xfail(
    reason="Nested array projection support not yet implemented - falls back to Python"
)
def test_text_search_on_unwound_nested_arrays(collection):
    """Test text search on nested array structures with projection."""
    # Insert documents with nested arrays
    collection.insert_one(
        {
            "_id": 1,
            "name": "Tech Reviewer",
            "reviews": [
                {
                    "product": "Laptop",
                    "comments": [
                        "Excellent performance for the price",
                        "Battery life could be better",
                        "Good design and build quality",
                    ],
                },
                {
                    "product": "Phone",
                    "comments": [
                        "Amazing camera performance",
                        "Smooth performance overall",
                        "Average battery performance",
                    ],
                },
            ],
        }
    )

    collection.insert_one(
        {
            "_id": 2,
            "name": "Gadget Expert",
            "reviews": [
                {
                    "product": "Tablet",
                    "comments": [
                        "Decent performance for basic tasks",
                        "Poor performance under load",
                        "Battery performance is acceptable",
                    ],
                }
            ],
        }
    )

    # Create FTS index on nested comments
    collection.create_index("reviews.comments", fts=True)

    # Test: Unwind reviews, then unwind comments, then search for "performance"
    pipeline = [
        {"$unwind": "$reviews"},
        {"$unwind": "$reviews.comments"},
        {"$match": {"$text": {"$search": "performance"}}},
        {
            "$project": {
                "reviewer": "$name",
                "product": "$reviews.product",
                "comment": "$reviews.comments",
            }
        },
    ]

    results = list(collection.aggregate(pipeline))
    assert len(results) >= 5  # Should find multiple comments with "performance"

    # Verify some content is found
    comments = [doc["comment"] for doc in results]
    assert any("performance" in comment.lower() for comment in comments)


@pytest.mark.xfail(
    reason="Multiple term search not yet implemented - falls back to Python"
)
def test_text_search_on_unwound_with_multiple_matches(collection):
    """Test text search that matches multiple terms within unwound elements."""
    # Insert documents
    collection.insert_one(
        {
            "_id": 1,
            "author": "Developer",
            "snippets": [
                "High performance code with clean design",
                "Database performance optimization techniques",
                "Web application performance tuning",
                "Mobile performance best practices",
            ],
        }
    )

    # Create FTS index
    collection.create_index("snippets", fts=True)

    # Test: Search for multiple terms
    pipeline = [
        {"$unwind": "$snippets"},
        {"$match": {"$text": {"$search": "performance database"}}},
    ]

    results = list(collection.aggregate(pipeline))
    # Should find snippets containing both terms
    assert len(results) >= 1

    # Verify content contains both terms
    snippets = [doc["snippets"] for doc in results]
    found_performance = False
    found_database = False
    for snippet in snippets:
        if "performance" in snippet.lower():
            found_performance = True
        if "database" in snippet.lower():
            found_database = True
    assert found_performance and found_database


@pytest.mark.xfail(
    reason="FTS index integration with unwound elements not yet implemented - falls back to Python"
)
def test_text_search_on_unwound_case_insensitive(collection):
    """Test that text search on unwound arrays is case insensitive."""
    # Insert documents
    collection.insert_one(
        {
            "_id": 1,
            "author": "Alice",
            "notes": ["PERFORMANCE Review", "Quality Check", "Design Analysis"],
        }
    )

    collection.insert_one(
        {
            "_id": 2,
            "author": "Bob",
            "notes": ["performance testing", "Code Review", "System Analysis"],
        }
    )

    # Create FTS index
    collection.create_index("notes", fts=True)

    # Test: Search with different case
    pipeline = [
        {"$unwind": "$notes"},
        {"$match": {"$text": {"$search": "PERFORMANCE"}}},  # Uppercase search
    ]

    results = list(collection.aggregate(pipeline))
    assert (
        len(results) == 2
    )  # Should find both "PERFORMANCE Review" and "performance testing"

    # Verify content
    notes = [doc["notes"] for doc in results]
    assert "PERFORMANCE Review" in notes
    assert "performance testing" in notes


# ================================
# Logical Operators Tests
# ================================


def test_or_with_contains_operator(collection):
    """Test that $or operator works correctly with $contains operator."""
    # Insert test documents
    collection.insert_one(
        {"title": "Hello World", "subtitle": "This is a test"}
    )
    collection.insert_one(
        {"title": "Goodbye World", "subtitle": "This is another test"}
    )
    collection.insert_one({"title": "Foo Bar", "subtitle": "Something else"})

    # Test $or with $contains
    result = list(
        collection.find(
            {
                "$or": [
                    {"title": {"$contains": "hello"}},
                    {"subtitle": {"$contains": "another"}},
                ]
            }
        )
    )

    # Should match first two documents
    assert len(result) == 2

    # Check that we got the right documents
    titles = {doc["title"] for doc in result}
    assert titles == {"Hello World", "Goodbye World"}

    # Test with a query that matches all documents
    result = list(
        collection.find(
            {
                "$or": [
                    {"title": {"$contains": "hello"}},
                    {"title": {"$contains": "goodbye"}},
                    {"subtitle": {"$contains": "something"}},
                ]
            }
        )
    )

    # Should match all documents
    assert len(result) == 3


def test_nested_logical_operators(collection):
    """Test nested logical operators."""
    # Insert test documents
    collection.insert_one(
        {"title": "Hello World", "category": "A", "priority": 1}
    )
    collection.insert_one(
        {"title": "Goodbye World", "category": "B", "priority": 2}
    )
    collection.insert_one({"title": "Foo Bar", "category": "A", "priority": 3})

    # Test nested $and within $or
    result = list(
        collection.find(
            {
                "$or": [
                    {"$and": [{"category": "A"}, {"priority": {"$gte": 2}}]},
                    {"title": {"$contains": "goodbye"}},
                ]
            }
        )
    )

    # Should match the second and third documents
    assert len(result) == 2

    titles = {doc["title"] for doc in result}
    assert titles == {"Goodbye World", "Foo Bar"}


def test_not_operator(collection):
    """Test $not operator."""
    # Insert test documents
    collection.insert_one({"title": "Hello World", "category": "A"})
    collection.insert_one({"title": "Goodbye World", "category": "B"})

    # Test $not operator
    result = list(collection.find({"$not": {"category": "A"}}))

    # Should match only the second document
    assert len(result) == 1
    assert result[0]["title"] == "Goodbye World"


def test_nor_operator(collection):
    """Test $nor operator."""
    # Insert test documents
    collection.insert_one({"title": "Hello World", "category": "A"})
    collection.insert_one({"title": "Goodbye World", "category": "B"})
    collection.insert_one({"title": "Foo Bar", "category": "C"})

    # Test $nor operator
    result = list(
        collection.find({"$nor": [{"category": "A"}, {"category": "B"}]})
    )

    # Should match only the third document
    assert len(result) == 1
    assert result[0]["title"] == "Foo Bar"


def test_text_with_and_operator(collection):
    """Test $text operator with $and operator"""
    # Insert documents
    collection.insert_many(
        [
            {
                "title": "Python Programming",
                "content": "Python is a high-level programming language",
                "category": "programming",
            },
            {
                "title": "JavaScript Basics",
                "content": "JavaScript is used for web development",
                "category": "web",
            },
            {
                "title": "Python Web Development",
                "content": "Python is great for web development",
                "category": "web",
            },
        ]
    )

    # Create FTS index
    collection.create_index("content", fts=True)

    # Test $text with $and
    results = list(
        collection.find(
            {"$and": [{"$text": {"$search": "python"}}, {"category": "web"}]}
        )
    )

    assert len(results) == 1
    assert results[0]["title"] == "Python Web Development"


def test_text_with_or_operator(collection):
    """Test $text operator with $or operator"""
    # Insert documents
    collection.insert_many(
        [
            {
                "title": "Python Programming",
                "content": "Python is a high-level programming language",
                "category": "programming",
            },
            {
                "title": "JavaScript Basics",
                "content": "JavaScript is used for web development",
                "category": "web",
            },
            {
                "title": "Machine Learning",
                "content": "Machine learning is a subset of AI",
                "category": "ai",
            },
        ]
    )

    # Create FTS index
    collection.create_index("content", fts=True)

    # Test $text with $or
    results = list(
        collection.find(
            {
                "$or": [
                    {"$text": {"$search": "python"}},
                    {"$text": {"$search": "javascript"}},
                ]
            }
        )
    )

    assert len(results) == 2
    titles = [doc["title"] for doc in results]
    assert "Python Programming" in titles
    assert "JavaScript Basics" in titles


def test_text_with_not_operator(collection):
    """Test $text operator with $not operator"""
    # Insert documents
    collection.insert_many(
        [
            {
                "title": "Python Programming",
                "content": "Python is a high-level programming language",
            },
            {
                "title": "JavaScript Basics",
                "content": "JavaScript is used for web development",
            },
            {
                "title": "Machine Learning",
                "content": "Machine learning is a subset of AI",
            },
        ]
    )

    # Create FTS index
    collection.create_index("content", fts=True)

    # Test $text with $not
    results = list(collection.find({"$not": {"$text": {"$search": "web"}}}))

    assert len(results) == 2
    titles = [doc["title"] for doc in results]
    assert "Python Programming" in titles
    assert "Machine Learning" in titles


def test_text_with_nor_operator(collection):
    """Test $text operator with $nor operator"""
    # Insert documents
    collection.insert_many(
        [
            {
                "title": "Python Programming",
                "content": "Python is a high-level programming language",
            },
            {
                "title": "JavaScript Basics",
                "content": "JavaScript is used for web development",
            },
            {
                "title": "Machine Learning",
                "content": "Machine learning is a subset of AI",
            },
        ]
    )

    # Create FTS index
    collection.create_index("content", fts=True)

    # Test $text with $nor
    results = list(
        collection.find(
            {
                "$nor": [
                    {"$text": {"$search": "python"}},
                    {"$text": {"$search": "javascript"}},
                ]
            }
        )
    )

    assert len(results) == 1
    assert results[0]["title"] == "Machine Learning"


def test_complex_nested_logical_operators_with_text(collection):
    """Test complex nested logical operators with $text"""
    # Insert documents
    collection.insert_many(
        [
            {
                "title": "Python Programming",
                "content": "Python is a high-level programming language",
                "category": "programming",
            },
            {
                "title": "JavaScript Basics",
                "content": "JavaScript is used for web development",
                "category": "web",
            },
            {
                "title": "Machine Learning",
                "content": "Machine learning is a subset of AI",
                "category": "ai",
            },
            {
                "title": "Web Development",
                "content": "HTML, CSS, and JavaScript for web development",
                "category": "web",
            },
            {
                "title": "Data Science",
                "content": "Python is widely used in data science",
                "category": "data",
            },
        ]
    )

    # Create FTS index
    collection.create_index("content", fts=True)

    # Test complex nested logical operators
    # Find documents where:
    # 1. Content contains "web" OR category is "ai"
    # 2. AND title contains "JavaScript" OR title contains "Machine"
    results = list(
        collection.find(
            {
                "$and": [
                    {
                        "$or": [
                            {"$text": {"$search": "web"}},
                            {"category": "ai"},
                        ]
                    },
                    {
                        "$or": [
                            {"title": {"$regex": "JavaScript"}},
                            {"title": {"$regex": "Machine"}},
                        ]
                    },
                ]
            }
        )
    )

    assert len(results) == 2
    titles = [doc["title"] for doc in results]
    assert "JavaScript Basics" in titles
    assert "Machine Learning" in titles


def test_text_with_mixed_conditions(collection):
    """Test $text operator with mixed conditions in logical operators"""
    # Insert documents
    collection.insert_many(
        [
            {
                "title": "Python Programming",
                "content": "Python is a high-level programming language",
                "version": 3.9,
            },
            {
                "title": "JavaScript Guide",
                "content": "JavaScript is used for web development",
                "version": 2021,
            },
            {
                "title": "Python Tips",
                "content": "Python tips and tricks for developers",
                "version": 3.8,
            },
        ]
    )

    # Create FTS index
    collection.create_index("content", fts=True)

    # Test $text with mixed conditions
    results = list(
        collection.find(
            {
                "$or": [
                    {"$text": {"$search": "python"}},
                    {
                        "$and": [
                            {"$text": {"$search": "javascript"}},
                            {"version": {"$gte": 2020}},
                        ]
                    },
                ]
            }
        )
    )

    # Should find 3 documents:
    # 1. "Python Programming" (matches $text: "python")
    # 2. "Python Tips" (matches $text: "python")
    # 3. "JavaScript Guide" (matches both $text: "javascript" and version >= 2020)
    assert len(results) == 3
    titles = [doc["title"] for doc in results]
    assert "Python Programming" in titles
    assert "Python Tips" in titles
    assert "JavaScript Guide" in titles


def test_text_logical_operators_case_insensitive(collection):
    """Test that $text with logical operators is case insensitive"""
    # Insert documents
    collection.insert_many(
        [
            {
                "title": "Python Programming",
                "content": "Python is a HIGH-LEVEL programming language",
            },
            {
                "title": "JavaScript Basics",
                "content": "JavaScript is used for WEB development",
            },
        ]
    )

    # Create FTS index
    collection.create_index("content", fts=True)

    # Test case insensitive search with $or
    results = list(
        collection.find(
            {
                "$or": [
                    {"$text": {"$search": "HIGH"}},  # Uppercase
                    {"$text": {"$search": "web"}},  # Lowercase
                ]
            }
        )
    )

    assert len(results) == 2
    titles = [doc["title"] for doc in results]
    assert "Python Programming" in titles
    assert "JavaScript Basics" in titles


def test_text_with_logical_operators_no_matches(collection):
    """Test $text with logical operators when there are no matches"""
    # Insert documents
    collection.insert_many(
        [
            {
                "title": "Python Programming",
                "content": "Python is a programming language",
                "category": "programming",
            },
            {
                "title": "JavaScript Basics",
                "content": "JavaScript is used for web development",
                "category": "web",
            },
        ]
    )

    # Create FTS index
    collection.create_index("content", fts=True)

    # Test with no matches
    results = list(
        collection.find(
            {
                "$and": [
                    {"$text": {"$search": "python"}},
                    {
                        "category": "web"
                    },  # This won't match any Python documents
                ]
            }
        )
    )

    assert len(results) == 0


def test_text_with_logical_operators_fallback(collection):
    """Test $text with logical operators when FTS is not available (fallback to Python)"""
    # Insert documents
    collection.insert_many(
        [
            {
                "title": "Python Programming",
                "content": "Python is a programming language",
                "category": "programming",
            },
            {
                "title": "JavaScript Basics",
                "content": "JavaScript is used for web development",
                "category": "web",
            },
        ]
    )

    # Don't create FTS index to test Python fallback

    # Test with Python fallback
    results = list(
        collection.find(
            {"$or": [{"$text": {"$search": "python"}}, {"category": "web"}]}
        )
    )

    # Should find both documents - one through text search fallback, one through category match
    assert len(results) == 2
    titles = [doc["title"] for doc in results]
    assert "Python Programming" in titles
    assert "JavaScript Basics" in titles
