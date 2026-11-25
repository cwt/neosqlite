"""Enhanced text search functionality for NeoSQLite.

Provides Unicode-aware, diacritic-insensitive, and case-insensitive text
matching with LRU caching for compiled regular expressions.
"""

import logging
import re
import unicodedata
from functools import lru_cache
from typing import Any

logger = logging.getLogger(__name__)


class TextSearchOptimizer:
    """Optimize text search operations with caching and Unicode support.

    Provides LRU caching for compiled regex patterns, NFD Unicode
    normalization for diacritic-insensitive matching, and case-insensitive
    searching for international characters.
    """

    @staticmethod
    @lru_cache(maxsize=512)
    def compile_pattern(search_term: str):
        """Compile and cache an escaped regex pattern for a search term.

        Compiles ``re.escape(search_term)`` with ``re.IGNORECASE | re.UNICODE``
        flags and caches up to 512 patterns.

        Args:
            search_term: The substring to escape and compile into a regex.

        Returns:
            re.Pattern | None: Compiled regex pattern, or None if
                ``re.compile`` raises ``re.error``.
        """
        try:
            return re.compile(
                re.escape(search_term), re.IGNORECASE | re.UNICODE
            )
        except re.error as e:
            logger.debug(f"Regex compilation failed: {e}")
            return None

    @staticmethod
    def normalize_text(text: str) -> str:
        """Normalize text by lowercasing and stripping diacritics.

        Converts ``text`` to lowercase, applies Unicode NFD decomposition, and
        removes combining non-spacing marks (category ``Mn``).

        Args:
            text: Input text string to normalize.

        Returns:
            str: Lowercased text with combining diacritical marks removed.
        """
        # Normalize to decomposed form (NFD)
        normalized = unicodedata.normalize("NFD", text.lower())
        # Remove combining characters (diacritics)
        return "".join(c for c in normalized if unicodedata.category(c) != "Mn")

    @staticmethod
    @lru_cache(maxsize=512)
    def get_normalized_pattern(search_term: str):
        """Compile and cache a diacritic-stripped regex for a search term.

        Normalizes ``search_term`` via ``normalize_text()``, escapes it, and
        compiles with ``re.IGNORECASE | re.UNICODE`` flags.

        Args:
            search_term: The term to normalize and compile into a regex.

        Returns:
            re.Pattern | None: Compiled regex pattern for normalized text, or
                None if ``re.compile`` raises ``re.error``.
        """
        try:
            normalized = TextSearchOptimizer.normalize_text(search_term)
            return re.compile(re.escape(normalized), re.IGNORECASE | re.UNICODE)
        except re.error as e:
            logger.debug(f"Regex compilation failed: {e}")
            return None


def unified_text_search(document: dict[str, Any], search_term: str) -> bool:
    """Search a document recursively for a text term.

    Works with both simple queries and aggregation pipelines, providing
    case-insensitive, Unicode-aware, and diacritic-insensitive matching
    across nested dictionaries and lists.

    Args:
        document: The document dictionary to search.
        search_term: Non-empty string term to search for.

    Returns:
        bool: True if any string value in ``document`` matches
            ``search_term``, False otherwise (including empty or non-string
            terms).
    """
    if not isinstance(search_term, str) or not search_term:
        return False

    # Get compiled patterns for performance
    exact_pattern = TextSearchOptimizer.compile_pattern(search_term)
    normalized_pattern = TextSearchOptimizer.get_normalized_pattern(search_term)

    def search_in_value(value: Any, term: str) -> bool:
        """Search for the term in a single field value.

        Matches strings via exact regex, diacritic-normalized regex, or
        case-insensitive substring fallback, and recurses into dicts and
        lists.

        Args:
            value: Field value to inspect (string, dict, list, or other).
            term: Search term string.

        Returns:
            bool: True if ``value`` matches or contains ``term``, False
                otherwise.
        """
        match value:
            case str():
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

            case dict():
                # Recursively search in nested dictionaries
                return search_in_document(value, term)

            case list():
                # Search in list items
                return any(search_in_value(item, term) for item in value)

            case _:
                return False

    def search_in_document(doc: dict[str, Any], term: str) -> bool:
        """Recursively search all values of a document for the term.

        Args:
            doc: Dictionary whose values are searched.
            term: Search term string.

        Returns:
            bool: True if any value in ``doc`` matches ``term``, False
                otherwise.
        """
        # Search in all values of the document
        return any(search_in_value(value, term) for value in doc.values())

    return search_in_document(document, search_term)
