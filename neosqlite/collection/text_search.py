"""
Enhanced text search functionality for NeoSQLite with international character support.
"""

from functools import lru_cache
from typing import Any, Dict
import re
import unicodedata


class TextSearchOptimizer:
    """
    Optimize text search operations with caching and Unicode support.

    This class provides optimized text search functionality with:
    - LRU caching for compiled regex patterns
    - Unicode normalization for international character support
    - Diacritic-insensitive matching
    - Case-insensitive searching
    """

    @staticmethod
    @lru_cache(maxsize=1000)
    def compile_pattern(search_term: str):
        """
        Compile and cache regex patterns for better performance.

        Args:
            search_term: The term to search for

        Returns:
            Compiled regex pattern or None if compilation fails
        """
        try:
            return re.compile(
                re.escape(search_term), re.IGNORECASE | re.UNICODE
            )
        except re.error:
            return None

    @staticmethod
    def normalize_text(text: str) -> str:
        """
        Normalize text for comparison by removing diacritics.

        Args:
            text: Text to normalize

        Returns:
            Normalized text with diacritics removed
        """
        # Normalize to decomposed form (NFD)
        normalized = unicodedata.normalize("NFD", text.lower())
        # Remove combining characters (diacritics)
        return "".join(c for c in normalized if unicodedata.category(c) != "Mn")

    @staticmethod
    @lru_cache(maxsize=1000)
    def get_normalized_pattern(search_term: str):
        """
        Get normalized pattern for diacritic-insensitive matching.

        Args:
            search_term: The term to search for

        Returns:
            Compiled regex pattern for normalized text or None if compilation fails
        """
        try:
            normalized = TextSearchOptimizer.normalize_text(search_term)
            return re.compile(re.escape(normalized), re.IGNORECASE | re.UNICODE)
        except re.error:
            return None


def unified_text_search(document: Dict[str, Any], search_term: str) -> bool:
    """
    Unified text search function that works with both simple queries and aggregation pipelines.

    This function provides enhanced text search capabilities with:
    - Case-insensitive matching
    - Unicode support for international characters
    - Diacritic-insensitive matching (e.g., 'cafe' matches 'café')
    - Support for nested documents and arrays
    - Proper handling of special characters

    Args:
        document: The document to search in
        search_term: The term to search for

    Returns:
        True if the document contains the search term, False otherwise
    """
    if not isinstance(search_term, str) or not search_term:
        return False

    # Get compiled patterns for performance
    exact_pattern = TextSearchOptimizer.compile_pattern(search_term)
    normalized_pattern = TextSearchOptimizer.get_normalized_pattern(search_term)

    def search_in_value(value: Any, term: str) -> bool:
        """
        Search for the term in a single value.

        Args:
            value: The value to search in
            term: The search term

        Returns:
            True if the value contains the search term, False otherwise
        """
        if isinstance(value, str):
            # Try exact match with regex first (handles Unicode properly)
            if exact_pattern and exact_pattern.search(value):
                return True

            # Try normalized match (diacritic-insensitive)
            if normalized_pattern:
                normalized_value = TextSearchOptimizer.normalize_text(value)
                if normalized_pattern.search(normalized_value):
                    return True

            # Fallback to simple case-insensitive substring matching
            return term.lower() in value.lower()

        elif isinstance(value, dict):
            # Recursively search in nested dictionaries
            return search_in_document(value, term)

        elif isinstance(value, list):
            # Search in list items
            return any(search_in_value(item, term) for item in value)

        return False

    def search_in_document(doc: Dict[str, Any], term: str) -> bool:
        """
        Recursively search for the term in a document.

        Args:
            doc: The document to search in
            term: The search term

        Returns:
            True if the document contains the search term, False otherwise
        """
        # Search in all values of the document
        return any(search_in_value(value, term) for value in doc.values())

    return search_in_document(document, search_term)


def simple_text_contains(text: str, search_term: str) -> bool:
    """
    Simple case-insensitive text containment check.

    This is a fast fallback for basic ASCII text matching.

    Args:
        text: The text to search in
        search_term: The term to search for

    Returns:
        True if the text contains the search term, False otherwise
    """
    if not isinstance(text, str) or not isinstance(search_term, str):
        return False
    return search_term.lower() in text.lower()
