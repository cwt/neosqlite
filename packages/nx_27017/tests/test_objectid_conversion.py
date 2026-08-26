"""Test ObjectId conversion."""

from bson import ObjectId as BsonObjectId
from nx_27017.utils import (
    convert_bson_to_neo_objectids,
    convert_json_to_neo_objectids,
    convert_neo_to_bson_objectids,
)

from neosqlite.objectid import ObjectId as NeoObjectId


class TestConvertObjectIds:
    """Test ObjectId conversion utilities."""

    def test_convert_dict_with_objectid(self):
        doc = {"_id": BsonObjectId("507f1f77bcf86cd799439011"), "name": "test"}
        result = convert_neo_to_bson_objectids(doc)
        assert result["name"] == "test"

    def test_convert_dict_with_zero_id(self):
        from bson import Int64

        doc = {"id": 0, "name": "test"}
        result = convert_neo_to_bson_objectids(doc)
        assert result["id"] == Int64(0)

    def test_convert_nested_dict(self):
        doc = {
            "name": "test",
            "nested": {"_id": BsonObjectId("507f1f77bcf86cd799439011")},
        }
        result = convert_neo_to_bson_objectids(doc)
        assert result["name"] == "test"
        assert "_id" in result["nested"]

    def test_convert_list(self):
        doc = [
            {"_id": BsonObjectId("507f1f77bcf86cd799439011")},
            {"name": "test"},
        ]
        result = convert_neo_to_bson_objectids(doc)
        assert len(result) == 2

    def test_convert_nested_list(self):
        doc = {
            "items": [
                {"_id": BsonObjectId("507f1f77bcf86cd799439011")},
                {"name": "test"},
            ]
        }
        result = convert_neo_to_bson_objectids(doc)
        assert len(result["items"]) == 2

    def test_convert_bson_to_neo_list_and_single_id(self):
        bson_oid = BsonObjectId("507f1f77bcf86cd799439011")
        neo_oid = convert_bson_to_neo_objectids(bson_oid)
        assert isinstance(neo_oid, NeoObjectId)
        assert str(neo_oid) == "507f1f77bcf86cd799439011"

        doc_list = [{"_id": bson_oid}, {"sub": [{"_id": bson_oid}]}]
        res_list = convert_bson_to_neo_objectids(doc_list)
        assert isinstance(res_list[0]["_id"], NeoObjectId)
        assert isinstance(res_list[1]["sub"][0]["_id"], NeoObjectId)

    def test_decimal_preserved_as_decimal128(self):
        from decimal import Decimal as PyDecimal

        from bson import Decimal128

        value = PyDecimal("0.123456789012345678901234567890")
        result = convert_neo_to_bson_objectids({"price": value})
        assert isinstance(result["price"], Decimal128)
        assert str(result["price"]) == "0.123456789012345678901234567890"
        # Float conversion would round away the extra precision.
        assert str(result["price"]) != str(float(value))

    def test_convert_json_to_neo_objectids(self):
        oid_str = "507f1f77bcf86cd799439011"
        assert convert_json_to_neo_objectids(None) is None
        assert convert_json_to_neo_objectids("string_val") == "string_val"
        assert convert_json_to_neo_objectids(123) == 123

        single_oid_doc = {"$oid": oid_str}
        res_oid = convert_json_to_neo_objectids(single_oid_doc)
        assert isinstance(res_oid, NeoObjectId)
        assert str(res_oid) == oid_str

        pipeline = [
            {"$match": {"_id": {"$oid": oid_str}}},
            {"$or": [{"nested": {"$oid": oid_str}}]},
        ]
        res_pipeline = convert_json_to_neo_objectids(pipeline)
        assert isinstance(res_pipeline[0]["$match"]["_id"], NeoObjectId)
        assert isinstance(res_pipeline[1]["$or"][0]["nested"], NeoObjectId)
