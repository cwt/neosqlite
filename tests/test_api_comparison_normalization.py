"""Type-aware comparison normalization tests."""

import os
import sys

from bson import ObjectId as BsonObjectId

from neosqlite.objectid import ObjectId

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "examples"))

from api_comparison.utils import compare_results  # noqa: E402


def test_scalar_id_values_are_compared():
    passed, _ = compare_results([{"_id": 1}], [{"_id": 1}])
    assert passed is True
    passed, _ = compare_results([{"_id": 2}], [{"_id": 1}])
    assert passed is False


def test_bool_and_int_ids_are_distinct():
    passed, _ = compare_results([{"_id": True}], [{"_id": 1}])
    assert passed is False


def test_nested_id_values_are_compared():
    passed, _ = compare_results([{"a": {"_id": 1}}], [{"a": {"_id": 2}}])
    assert passed is False


def test_objectid_field_does_not_equal_plain_string():
    oid = ObjectId()
    passed, _ = compare_results([{"v": oid}], [{"v": str(oid)}])
    assert passed is False


def test_encoded_objectid_matches_bson_objectid():
    oid = ObjectId()
    encoded = {"__neosqlite_objectid__": True, "id": str(oid)}
    passed, _ = compare_results(
        [{"v": encoded}], [{"v": BsonObjectId(str(oid))}]
    )
    assert passed is True


def test_generated_objectid_ids_only_compare_by_type():
    passed, _ = compare_results(
        [{"_id": ObjectId(), "v": 1}],
        [{"_id": BsonObjectId(), "v": 1}],
    )
    assert passed is True
