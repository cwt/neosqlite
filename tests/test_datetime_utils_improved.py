"""
Test for datetime utilities to improve coverage.
"""

from neosqlite.collection.datetime_utils import (
    is_datetime_value,
    is_datetime_regex,
)


def test_is_datetime_value_basic():
    """Test basic datetime value detection."""
    # Test datetime string
    assert is_datetime_value("2023-01-15T10:30:00") is True
    assert is_datetime_value("2023-01-15") is True
    assert is_datetime_value("2023-01-15T10:30:00+05:30") is True
    assert is_datetime_value("2023-01-15T10:30:00Z") is True
    assert is_datetime_value("2023-01-15 10:30:00") is True
    assert is_datetime_value("01/15/2023") is True
    assert is_datetime_value("01-15-2023") is True
    assert is_datetime_value("2023/01/15") is True


def test_is_datetime_value_non_datetime():
    """Test non-datetime value detection."""
    assert is_datetime_value("hello world") is False
    assert is_datetime_value("not-a-date") is False
    assert is_datetime_value(123) is False
    assert is_datetime_value([]) is False
    assert is_datetime_value({}) is False
    assert is_datetime_value(None) is False


def test_is_datetime_value_datetime_objects():
    """Test datetime and date object detection."""
    import datetime

    # Test datetime object
    dt = datetime.datetime(2023, 1, 1, 10, 30, 0)
    assert is_datetime_value(dt) is True

    # Test date object
    date = datetime.date(2023, 1, 1)
    assert is_datetime_value(date) is True


def test_is_datetime_value_with_whitespace():
    """Test datetime value detection with whitespace."""
    assert is_datetime_value(" 2023-01-15 ") is True
    assert is_datetime_value("\t2023-01-15T10:30:00\n") is True


def test_is_datetime_value_nested_dict():
    """Test datetime value detection in nested dict."""
    nested_dict_with_dt = {"nested": {"date": "2023-01-15T10:30:00"}}
    assert is_datetime_value(nested_dict_with_dt) is True

    nested_dict_without_dt = {"nested": {"other": "not a date"}}
    assert is_datetime_value(nested_dict_without_dt) is False


def test_is_datetime_regex_basic():
    """Test basic datetime regex pattern detection."""
    # Test escaped patterns that would appear in actual regex strings
    assert is_datetime_regex(r"\d{4}-\d{2}-\d{2}") is True  # Date pattern
    assert (
        is_datetime_regex(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}") is True
    )  # Datetime pattern
    assert is_datetime_regex(r"\d{2}/\d{2}/\d{4}") is True  # US date format
    assert is_datetime_regex(r"\d{4}/\d{2}/\d{2}") is True  # Alt date format
    assert is_datetime_regex(r"\d{2}-\d{2}-\d{4}") is True  # Common date format


def test_is_datetime_regex_containing_patterns():
    """Test regex patterns containing datetime indicators."""
    assert is_datetime_regex(r"Start \d{4}-\d{2}-\d{2} end") is True
    assert (
        is_datetime_regex(
            r"Prefix and \d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2} suffix"
        )
        is True
    )


def test_is_datetime_regex_non_datetime():
    """Test non-datetime regex patterns."""
    assert is_datetime_regex("hello world") is False
    assert is_datetime_regex("regular text") is False
    assert is_datetime_regex("") is False
    assert is_datetime_regex("no datetime patterns here") is False


def test_is_datetime_regex_non_string():
    """Test datetime regex detection with non-string input."""
    assert is_datetime_regex(123) is False
    assert is_datetime_regex(None) is False
    assert is_datetime_regex([]) is False
    assert is_datetime_regex({}) is False
    assert is_datetime_regex(True) is False
