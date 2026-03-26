"""Test Utility Functions."""


class TestUtilityFunctions:
    """Test utility functions."""

    def test_get_next_request_id(self):
        from nx_27017.nx_27017 import _get_next_request_id

        id1 = _get_next_request_id()
        id2 = _get_next_request_id()
        assert id2 > id1

    def test_extract_session_id_none(self):
        from nx_27017.nx_27017 import _extract_session_id

        result = _extract_session_id(None)
        assert result is None

    def test_extract_session_id_oid_format(self):
        from nx_27017.nx_27017 import _extract_session_id

        doc = {"$oid": "507f1f77bcf86cd799439011"}
        result = _extract_session_id(doc)
        assert result == "507f1f77bcf86cd799439011"

    def test_extract_session_id_binary_format(self):
        from bson import Binary
        from nx_27017.nx_27017 import _extract_session_id

        doc = {"id": Binary(b"session123")}
        result = _extract_session_id(doc)
        assert result == "73657373696f6e313233"

    def test_extract_session_id_string_id(self):
        from nx_27017.nx_27017 import _extract_session_id

        doc = {"id": "simple_string_id"}
        result = _extract_session_id(doc)
        assert result == "simple_string_id"
