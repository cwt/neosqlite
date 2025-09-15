"""
Tests for $inc and $mul operation validation functions and behavior.
"""

import math
import pytest
from neosqlite.collection.query_helper import (
    _is_numeric_value,
    _validate_inc_mul_field_value,
)
from neosqlite.exceptions import MalformedQueryException


class TestIsNumericValue:
    """Test cases for the _is_numeric_value function."""

    def test_integer_values(self):
        """Test that integer values are recognized as numeric."""
        assert _is_numeric_value(0) is True
        assert _is_numeric_value(1) is True
        assert _is_numeric_value(-1) is True
        assert _is_numeric_value(100) is True
        assert _is_numeric_value(-100) is True

    def test_float_values(self):
        """Test that float values are recognized as numeric."""
        assert _is_numeric_value(0.0) is True
        assert _is_numeric_value(1.5) is True
        assert _is_numeric_value(-1.5) is True
        assert _is_numeric_value(3.14159) is True

    def test_nan_values(self):
        """Test that NaN values are not recognized as numeric."""
        assert _is_numeric_value(float("nan")) is False
        assert _is_numeric_value(math.nan) is False

    def test_infinity_values(self):
        """Test that infinity values are not recognized as numeric."""
        assert _is_numeric_value(float("inf")) is False
        assert _is_numeric_value(float("-inf")) is False
        assert _is_numeric_value(math.inf) is False
        assert _is_numeric_value(-math.inf) is False

    def test_string_values(self):
        """Test that string values are not recognized as numeric."""
        assert _is_numeric_value("") is False
        assert _is_numeric_value("123") is False
        assert _is_numeric_value("123.45") is False
        assert _is_numeric_value("not_a_number") is False
        assert _is_numeric_value("0") is False
        assert _is_numeric_value("-1") is False

    def test_none_values(self):
        """Test that None values are not recognized as numeric."""
        assert _is_numeric_value(None) is False

    def test_boolean_values(self):
        """Test that boolean values are not recognized as numeric."""
        assert _is_numeric_value(True) is False
        assert _is_numeric_value(False) is False

    def test_list_values(self):
        """Test that list values are not recognized as numeric."""
        assert _is_numeric_value([]) is False
        assert _is_numeric_value([1, 2, 3]) is False

    def test_dict_values(self):
        """Test that dict values are not recognized as numeric."""
        assert _is_numeric_value({}) is False
        assert _is_numeric_value({"key": "value"}) is False

    def test_other_types(self):
        """Test that other types are not recognized as numeric."""
        assert _is_numeric_value(object()) is False
        assert _is_numeric_value(lambda x: x) is False


class TestValidateIncMulFieldValue:
    """Test cases for the _validate_inc_mul_field_value function."""

    def test_valid_numeric_values(self):
        """Test that valid numeric values pass validation."""
        # Should not raise any exceptions
        _validate_inc_mul_field_value("test_field", 10, "$inc")
        _validate_inc_mul_field_value("test_field", -5, "$inc")
        _validate_inc_mul_field_value("test_field", 0, "$inc")
        _validate_inc_mul_field_value("test_field", 3.14, "$inc")
        _validate_inc_mul_field_value("test_field", -2.5, "$inc")
        _validate_inc_mul_field_value("test_field", 0.0, "$inc")

    def test_none_values(self):
        """Test that None values pass validation."""
        # Should not raise any exceptions
        _validate_inc_mul_field_value("test_field", None, "$inc")
        _validate_inc_mul_field_value("test_field", None, "$mul")

    def test_invalid_string_values(self):
        """Test that string values raise MalformedQueryException."""
        with pytest.raises(MalformedQueryException) as exc_info:
            _validate_inc_mul_field_value("test_field", "not_a_number", "$inc")
        assert "Cannot apply $inc to a value of non-numeric type" in str(
            exc_info.value
        )
        assert "str" in str(exc_info.value)
        assert "'not_a_number'" in str(exc_info.value)

        with pytest.raises(MalformedQueryException) as exc_info:
            _validate_inc_mul_field_value("test_field", "123", "$mul")
        assert "Cannot apply $mul to a value of non-numeric type" in str(
            exc_info.value
        )
        assert "str" in str(exc_info.value)
        assert "'123'" in str(exc_info.value)

    def test_invalid_other_types(self):
        """Test that other invalid types raise MalformedQueryException."""
        with pytest.raises(MalformedQueryException) as exc_info:
            _validate_inc_mul_field_value("test_field", [], "$inc")
        assert "Cannot apply $inc to a value of non-numeric type" in str(
            exc_info.value
        )
        assert "list" in str(exc_info.value)

        with pytest.raises(MalformedQueryException) as exc_info:
            _validate_inc_mul_field_value("test_field", {}, "$mul")
        assert "Cannot apply $mul to a value of non-numeric type" in str(
            exc_info.value
        )
        assert "dict" in str(exc_info.value)

        with pytest.raises(MalformedQueryException) as exc_info:
            _validate_inc_mul_field_value("test_field", True, "$inc")
        assert "Cannot apply $inc to a value of non-numeric type" in str(
            exc_info.value
        )
        assert "bool" in str(exc_info.value)
