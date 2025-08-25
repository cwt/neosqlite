#!/usr/bin/env python3

import sys
import os

# Example usage with the actual tokenizer
example_code = """
import neosqlite

# Load custom tokenizer when creating connection
# Note: This will only work if you have built the tokenizer and the path is correct
try:
    conn = neosqlite.Connection(
        ":memory:",
        tokenizers=[("icu", "/home/cwt/Projects/fts5-icu-tokenizer/build/libfts5_icu.so")]
    )

    # Create a collection
    articles = conn["articles"]

    # Insert sample documents with multilingual text
    articles.insert_many([
        {"title": "English Article", "content": "This is an English text for searching."},
        {"title": "Mixed Language", "content": "甜蜜蜜, you smile so sweetly - หวานปานน้ำผึ้ง, your smile is as sweet as honey"},
    ])

    # Create FTS index with custom tokenizer
    articles.create_index("content", fts=True, tokenizer="icu")

    # Perform text search - this will now work better with mixed languages
    results = list(articles.find({"$text": {"$search": "甜蜜蜜"}}))
    print(f"Found {len(results)} documents containing '甜蜜蜜'")

    conn.close()

except Exception as e:
    print(f"Example usage (this is expected to fail if tokenizer is not available): {e}")
"""

print("Example usage of custom FTS5 tokenizer with neosqlite:")
print(example_code)
