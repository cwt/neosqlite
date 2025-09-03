"""
Test to verify the enhanced text search is working in aggregation pipelines.
"""

import pytest
from neosqlite import Connection


def test_hybrid_text_search_in_aggregation():
    """Test that aggregation pipelines with $text operators work with hybrid processing."""
    with Connection(":memory:") as conn:
        # Create test data
        collection = conn.test_collection
        collection.insert_many(
            [
                {
                    "name": "Python Developer",
                    "description": "Works with Python programming language",
                    "status": "active",
                },
                {
                    "name": "Java Developer",
                    "description": "Works with Java programming language",
                    "status": "active",
                },
                {
                    "name": "JavaScript Developer",
                    "description": "Works with JavaScript programming language",
                    "status": "inactive",
                },
                {
                    "name": "Python Data Scientist",
                    "description": "Works with Python for data analysis",
                    "status": "active",
                },
                {
                    "name": "Java Backend Engineer",
                    "description": "Works with Java for backend services",
                    "status": "inactive",
                },
            ]
        )

        # Test pipeline that would benefit from hybrid processing
        # First filter by status (SQL), then text search (Python), then sort (SQL)
        pipeline = [
            {"$match": {"status": "active"}},  # Should filter to 3 docs
            {
                "$match": {"$text": {"$search": "python"}}
            },  # Should filter to 2 docs
            {"$sort": {"name": 1}},  # Should sort the results
        ]

        result = list(collection.aggregate(pipeline))

        # Should find 2 documents
        assert len(result) == 2

        # Both should be active status
        assert all(doc["status"] == "active" for doc in result)

        # Both should contain "python" in their description
        assert all("python" in doc["description"].lower() for doc in result)

        # Should be sorted by name
        names = [doc["name"] for doc in result]
        assert names == sorted(names)

        # Specific documents should be found
        assert result[0]["name"] == "Python Data Scientist"
        assert result[1]["name"] == "Python Developer"


def test_international_characters_in_aggregation():
    """Test that international characters work in aggregation pipeline text search."""
    with Connection(":memory:") as conn:
        # Create test data with international characters
        collection = conn.test_collection
        collection.insert_many(
            [
                {
                    "name": "José María",
                    "description": "Software engineer from España",
                    "status": "active",
                },
                {
                    "name": "François Dubois",
                    "description": "Développeur from France",
                    "status": "active",
                },
                {
                    "name": "Björk Guðmundsdóttir",
                    "description": "Artist from Ísland",
                    "status": "inactive",
                },
                {
                    "name": "Müller Schmidt",
                    "description": "Engineer from Deutschland",
                    "status": "active",
                },
            ]
        )

        # Test pipeline with international character search
        pipeline = [
            {"$match": {"status": "active"}},  # Should filter to 3 docs
            {
                "$match": {"$text": {"$search": "jose"}}
            },  # Should match José (diacritic insensitive)
            {"$limit": 1},  # Should limit to 1 doc
        ]

        result = list(collection.aggregate(pipeline))

        # Should find 1 document
        assert len(result) == 1

        # Should be the José document
        assert result[0]["name"] == "José María"


if __name__ == "__main__":
    pytest.main([__file__])
