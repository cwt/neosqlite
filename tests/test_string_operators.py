"""
Tests for string operators in $expr.

Covers: $strcasecmp, $substrBytes, and verification of existing string operators
"""

import neosqlite
from neosqlite.collection.query_helper import (
    get_force_fallback,
    set_force_fallback,
)


class TestStrcasecmpOperator:
    """Test $strcasecmp operator."""

    def test_strcasecmp_equal(self):
        """Test $strcasecmp with equal strings."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"a": "hello", "b": "hello"})

            result = list(
                coll.aggregate(
                    [{"$project": {"result": {"$strcasecmp": ["$a", "$b"]}}}]
                )
            )

            assert len(result) == 1
            assert result[0]["result"] == 0

    def test_strcasecmp_case_insensitive(self):
        """Test $strcasecmp is case insensitive."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"a": "Hello", "b": "HELLO"})

            result = list(
                coll.aggregate(
                    [{"$project": {"result": {"$strcasecmp": ["$a", "$b"]}}}]
                )
            )

            assert len(result) == 1
            assert result[0]["result"] == 0

    def test_strcasecmp_less_than(self):
        """Test $strcasecmp when first string is less."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({})

            result = list(
                coll.aggregate(
                    [{"$project": {"result": {"$strcasecmp": ["abc", "xyz"]}}}]
                )
            )

            assert len(result) == 1
            assert result[0]["result"] == -1

    def test_strcasecmp_greater_than(self):
        """Test $strcasecmp when first string is greater."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({})

            result = list(
                coll.aggregate(
                    [{"$project": {"result": {"$strcasecmp": ["xyz", "abc"]}}}]
                )
            )

            assert len(result) == 1
            assert result[0]["result"] == 1

    def test_strcasecmp_with_kill_switch(self):
        """Test $strcasecmp with kill switch enabled."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"a": "Test", "b": "TEST"})

            original_state = get_force_fallback()
            try:
                set_force_fallback(True)

                result = list(
                    coll.aggregate(
                        [
                            {
                                "$project": {
                                    "result": {"$strcasecmp": ["$a", "$b"]}
                                }
                            }
                        ]
                    )
                )

                assert len(result) == 1
                assert result[0]["result"] == 0
            finally:
                set_force_fallback(original_state)


class TestSubstrBytesOperator:
    """Test $substrBytes operator."""

    def test_substr_bytes_basic(self):
        """Test $substrBytes with basic ASCII string."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({})

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "result": {"$substrBytes": ["hello", 1, 3]}
                            }
                        }
                    ]
                )
            )

            assert len(result) == 1
            assert result[0]["result"] == "ell"

    def test_substr_bytes_with_field(self):
        """Test $substrBytes with field reference."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"text": "Hello World"})

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "result": {"$substrBytes": ["$text", 0, 5]}
                            }
                        }
                    ]
                )
            )

            assert len(result) == 1
            assert result[0]["result"] == "Hello"

    def test_substr_bytes_utf8(self):
        """Test $substrBytes with UTF-8 string."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({})

            # "你好" in UTF-8 is 6 bytes (3 bytes per character)
            result = list(
                coll.aggregate(
                    [{"$project": {"result": {"$substrBytes": ["你好", 0, 3]}}}]
                )
            )

            assert len(result) == 1
            # Should return first character
            assert result[0]["result"] == "你"

    def test_substr_bytes_with_kill_switch(self):
        """Test $substrBytes with kill switch enabled."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"text": "Test String"})

            original_state = get_force_fallback()
            try:
                set_force_fallback(True)

                result = list(
                    coll.aggregate(
                        [
                            {
                                "$project": {
                                    "result": {"$substrBytes": ["$text", 5, 6]}
                                }
                            }
                        ]
                    )
                )

                assert len(result) == 1
                assert result[0]["result"] == "String"
            finally:
                set_force_fallback(original_state)


class TestExistingStringOperators:
    """Test existing string operators for regression."""

    def test_toLower(self):
        """Test $toLower operator."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"text": "HELLO"})

            result = list(
                coll.aggregate([{"$project": {"lower": {"$toLower": "$text"}}}])
            )

            assert result[0]["lower"] == "hello"

    def test_toUpper(self):
        """Test $toUpper operator."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"text": "hello"})

            result = list(
                coll.aggregate([{"$project": {"upper": {"$toUpper": "$text"}}}])
            )

            assert result[0]["upper"] == "HELLO"

    def test_trim(self):
        """Test $trim operator."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"text": "  hello  "})

            result = list(
                coll.aggregate(
                    [{"$project": {"trimmed": {"$trim": {"input": "$text"}}}}]
                )
            )

            assert result[0]["trimmed"] == "hello"

    def test_ltrim(self):
        """Test $ltrim operator."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"text": "  hello"})

            result = list(
                coll.aggregate(
                    [{"$project": {"trimmed": {"$ltrim": {"input": "$text"}}}}]
                )
            )

            assert result[0]["trimmed"] == "hello"

    def test_rtrim(self):
        """Test $rtrim operator."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"text": "hello  "})

            result = list(
                coll.aggregate(
                    [{"$project": {"trimmed": {"$rtrim": {"input": "$text"}}}}]
                )
            )

            assert result[0]["trimmed"] == "hello"

    def test_split(self):
        """Test $split operator."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"text": "a,b,c"})

            result = list(
                coll.aggregate(
                    [{"$project": {"parts": {"$split": ["$text", ","]}}}]
                )
            )

            assert result[0]["parts"] == ["a", "b", "c"]

    def test_replaceOne(self):
        """Test $replaceOne operator."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"text": "hello world"})

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "replaced": {
                                    "$replaceOne": {
                                        "input": "$text",
                                        "find": "world",
                                        "replacement": "there",
                                    }
                                }
                            }
                        }
                    ]
                )
            )

            assert result[0]["replaced"] == "hello there"

    def test_replaceAll(self):
        """Test $replaceAll operator."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"text": "aaa"})

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "replaced": {
                                    "$replaceAll": {
                                        "input": "$text",
                                        "find": "a",
                                        "replacement": "b",
                                    }
                                }
                            }
                        }
                    ]
                )
            )

            assert result[0]["replaced"] == "bbb"

    def test_strLenBytes(self):
        """Test $strLenBytes operator."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"text": "hello"})

            result = list(
                coll.aggregate(
                    [{"$project": {"len": {"$strLenBytes": "$text"}}}]
                )
            )

            assert result[0]["len"] == 5

    def test_strLenCP(self):
        """Test $strLenCP operator."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"text": "你好"})

            result = list(
                coll.aggregate([{"$project": {"len": {"$strLenCP": "$text"}}}])
            )

            # 2 code points (Chinese characters)
            assert result[0]["len"] == 2

    def test_substr(self):
        """Test $substr operator."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"text": "Hello World"})

            result = list(
                coll.aggregate(
                    [{"$project": {"sub": {"$substr": ["$text", 0, 5]}}}]
                )
            )

            assert result[0]["sub"] == "Hello"

    def test_substrCP(self):
        """Test $substrCP operator."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"text": "你好世界"})

            result = list(
                coll.aggregate(
                    [{"$project": {"sub": {"$substrCP": ["$text", 1, 2]}}}]
                )
            )

            assert result[0]["sub"] == "好世"

    def test_indexOfBytes(self):
        """Test $indexOfBytes operator."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"text": "hello world"})

            result = list(
                coll.aggregate(
                    [
                        {
                            "$project": {
                                "idx": {"$indexOfBytes": ["$text", "world"]}
                            }
                        }
                    ]
                )
            )

            assert result[0]["idx"] == 6

    def test_indexOfCP(self):
        """Test $indexOfCP operator."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"text": "你好世界"})

            result = list(
                coll.aggregate(
                    [{"$project": {"idx": {"$indexOfCP": ["$text", "世界"]}}}]
                )
            )

            assert result[0]["idx"] == 2


class TestStringOperatorsKillSwitch:
    """Test string operators with kill switch."""

    def test_all_string_operators_with_kill_switch(self):
        """Test all string operators work with kill switch enabled."""
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_collection
            coll.insert_one({"text": "Hello World"})

            original_state = get_force_fallback()
            try:
                set_force_fallback(True)

                result = list(
                    coll.aggregate(
                        [
                            {
                                "$project": {
                                    "lower": {"$toLower": "$text"},
                                    "upper": {"$toUpper": "$text"},
                                    "trimmed": {"$trim": {"input": "$text"}},
                                    "len": {"$strLenBytes": "$text"},
                                    "lenCP": {"$strLenCP": "$text"},
                                }
                            }
                        ]
                    )
                )

                assert len(result) == 1
                assert result[0]["lower"] == "hello world"
                assert result[0]["upper"] == "HELLO WORLD"
                assert result[0]["trimmed"] == "Hello World"
                assert result[0]["len"] == 11
                assert result[0]["lenCP"] == 11
            finally:
                set_force_fallback(original_state)
