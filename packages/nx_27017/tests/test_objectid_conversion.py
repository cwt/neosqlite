"""Test ObjectId conversion."""

from bson import ObjectId


class TestConvertObjectIds:
    """Test ObjectId conversion utilities."""

    def test_convert_dict_with_objectid(self):
        from nx_27017.nx_27017 import _convert_objectids

        doc = {"_id": ObjectId("507f1f77bcf86cd799439011"), "name": "test"}
        result = _convert_objectids(doc)
        assert result["name"] == "test"

    def test_convert_dict_with_zero_id(self):
        from bson import Int64
        from nx_27017.nx_27017 import _convert_objectids

        doc = {"id": 0, "name": "test"}
        result = _convert_objectids(doc)
        assert result["id"] == Int64(0)

    def test_convert_nested_dict(self):
        from nx_27017.nx_27017 import _convert_objectids

        doc = {
            "name": "test",
            "nested": {"_id": ObjectId("507f1f77bcf86cd799439011")},
        }
        result = _convert_objectids(doc)
        assert result["name"] == "test"
        assert "_id" in result["nested"]

    def test_convert_list(self):
        from nx_27017.nx_27017 import _convert_objectids

        doc = [
            {"_id": ObjectId("507f1f77bcf86cd799439011")},
            {"name": "test"},
        ]
        result = _convert_objectids(doc)
        assert len(result) == 2

    def test_convert_nested_list(self):
        from nx_27017.nx_27017 import _convert_objectids

        doc = {
            "items": [
                {"_id": ObjectId("507f1f77bcf86cd799439011")},
                {"name": "test"},
            ]
        }
        result = _convert_objectids(doc)
        assert len(result["items"]) == 2
