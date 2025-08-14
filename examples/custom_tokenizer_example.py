#!/usr/bin/env python3
"""
Example showing how to use custom FTS5 tokenizers with neosqlite.

This example demonstrates:
1. Loading a custom ICU tokenizer
2. Creating FTS indexes with custom tokenizers
3. Performing text searches with improved language support
"""

import neosqlite
import tempfile
import os


def example_with_custom_tokenizer():
    """
    Example showing how to use a custom FTS5 tokenizer with neosqlite.

    Note: This example assumes you have built the FTS5 ICU tokenizer
    and the library is available at the specified path.
    """

    # Path to the custom tokenizer (update this to your actual path)
    tokenizer_path = (
        "/home/cwt/Projects/fts5-icu-tokenizer/build/libfts5_icu.so"
    )

    # Check if the tokenizer exists
    if not os.path.exists(tokenizer_path):
        print(f"Note: Custom tokenizer not found at {tokenizer_path}")
        print(
            "This example will show the API usage but won't actually load the tokenizer."
        )
        print("\nExample usage:")
        print("--------------")

        # Show how to use the feature
        example_code = f"""
import neosqlite

# Load custom tokenizer when creating connection
conn = neosqlite.Connection(
    ":memory:", 
    tokenizers=[("icu", "{tokenizer_path}")]
)

# Create a collection
articles = conn["articles"]

# Insert sample documents with multilingual text
articles.insert_many([
    {{"title": "English Article", "content": "This is an English text for searching."}},
    {{"title": "Mixed Language", "content": "甜蜜蜜, you smile so sweetly - หวานปานน้ำผึ้ง, your smile is as sweet as honey"}},
])

# Create FTS index with custom tokenizer
articles.create_index("content", fts=True, tokenizer="icu")

# Perform text search - this will now work better with mixed languages
results = list(articles.find({{"$text": {{"$search": "甜蜜蜜"}}}}))
print(f"Found {{len(results)}} documents containing '甜蜜蜜'")

conn.close()
"""
        print(example_code)
        return

    # If the tokenizer exists, demonstrate the feature
    print("Custom tokenizer found. Demonstrating usage...")

    try:
        # Create a temporary database file
        with tempfile.NamedTemporaryFile(
            suffix=".db", delete=False
        ) as tmp_file:
            db_path = tmp_file.name

        # Load custom tokenizer when creating connection
        conn = neosqlite.Connection(
            db_path, tokenizers=[("icu", tokenizer_path)]
        )

        # Create a collection
        articles = conn["articles"]

        # Insert sample documents with multilingual text
        articles.insert_many(
            [
                {
                    "title": "English Article",
                    "content": "This is an English text for searching.",
                },
                {
                    "title": "Mixed Language",
                    "content": "甜蜜蜜, you smile so sweetly - หวานปานน้ำผึ้ง, your smile is as sweet as honey",
                },
            ]
        )

        # Create FTS index with custom tokenizer
        articles.create_index("content", fts=True, tokenizer="icu")

        # Perform text search - this will now work better with mixed languages
        results = list(articles.find({"$text": {"$search": "甜蜜蜜"}}))
        print(f"Found {len(results)} documents containing '甜蜜蜜'")

        # Clean up
        conn.close()
        os.unlink(db_path)

        print("✓ Custom tokenizer example completed successfully!")

    except Exception as e:
        print(f"Error using custom tokenizer: {e}")
        print(
            "This might be due to SQLite configuration or tokenizer compatibility issues."
        )


def example_without_tokenizer():
    """Example showing the difference between default and custom tokenizers."""
    print("\nComparison example:")
    print("------------------")

    # Create connection without custom tokenizer
    conn = neosqlite.Connection(":memory:")
    articles = conn["articles"]

    # Insert sample document
    articles.insert_one(
        {
            "title": "Multilingual Content",
            "content": "Python programming 甜蜜蜜 甜甜甜",
        }
    )

    # Create FTS index with default tokenizer
    articles.create_index("content", fts=True)

    print("Created FTS index with default tokenizer")

    # Show index information
    indexes = articles.list_indexes()
    print(f"Available indexes: {indexes}")

    conn.close()


if __name__ == "__main__":
    print("neosqlite Custom FTS5 Tokenizer Example")
    print("======================================")

    example_with_custom_tokenizer()
    example_without_tokenizer()

    print("\nFor more information, see:")
    print(
        "- https://hg.sr.ht/~cwt/fts5-icu-tokenizer for building custom tokenizers"
    )
    print("- The TEXT_SEARCH.md documentation in the neosqlite repository")
