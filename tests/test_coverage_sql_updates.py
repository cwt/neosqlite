"""Comprehensive coverage tests for _sql_updates.py."""

import pytest

import neosqlite
from neosqlite.binary import Binary
from neosqlite.collection.query_helper._sql_updates import (
    _compile_elem_predicate,
    _compile_json_scalar_eq,
    _compile_json_value_operators,
    _contains_binary_value,
    _extract_preserved_json_expr,
    _format_sql_value_expr,
    _je_preserved_value_expr,
    _wrap_single_eval,
)
from neosqlite.objectid import ObjectId


class TestSqlUpdateModuleHelpers:
    """Cover pure SQL helper compiler functions."""

    def test_contains_binary_and_format_helpers(self):
        assert _contains_binary_value(Binary(b"abc")) is True
        assert _contains_binary_value({"a": [1, {"b": Binary(b"x")}]}) is True
        assert _contains_binary_value({"a": [1, "str", None]}) is False

        v_sql, v_params = _format_sql_value_expr(None)
        assert v_sql == "?" and v_params == [None]

        v_bool, p_bool = _format_sql_value_expr(True)
        assert "json(" in v_bool and p_bool == ["true"]

        v_obj, p_obj = _format_sql_value_expr({"k": 1})
        assert "json(" in v_obj and len(p_obj) == 1

        v_num, p_num = _format_sql_value_expr(42)
        assert v_num == "?" and p_num == [42]

        assert "CASE" in _je_preserved_value_expr("_je0")
        assert "CASE" in _extract_preserved_json_expr(
            "json_extract", "data", "$.x"
        )

        # _wrap_single_eval substitutes __TARGET__ placeholder using WITH/scalar subquery
        c = [0]
        wrapped, params = _wrap_single_eval(
            "json_set(data, '$.a', ?)",
            [1],
            lambda alias: (f"json_set({alias}, '$.b', ?)", [2]),
            c,
        )
        assert "_d0" in wrapped and params == [2, 1]

    def test_compile_json_scalar_eq_and_value_operators(self):
        # _compile_json_scalar_eq variants
        assert _compile_json_scalar_eq("t", "v", None) == ("t = 'null'", [])
        assert _compile_json_scalar_eq("t", "v", True) == ("t = 'true'", [])
        assert _compile_json_scalar_eq("t", "v", False) == ("t = 'false'", [])
        assert _compile_json_scalar_eq("t", "v", 5) == (
            "t IN ('integer', 'real') AND v = ?",
            [5],
        )
        assert _compile_json_scalar_eq("t", "v", "abc") == (
            "t = 'text' AND v = ?",
            ["abc"],
        )
        oid = ObjectId()
        assert _compile_json_scalar_eq("t", "v", oid) is None
        assert _compile_json_scalar_eq("t", "v", Binary(b"ab")) is None
        assert _compile_json_scalar_eq("t", "v", [1, 2]) is None
        assert _compile_json_scalar_eq("t", "v", {"a": 1}) is None

        # _compile_json_value_operators variants
        sql, params = _compile_json_value_operators(
            "t",
            "v",
            {
                "$eq": 1,
                "$ne": 2,
                "$gt": 0,
                "$gte": 0,
                "$lt": 10,
                "$lte": 9,
                "$in": [1, 2],
                "$nin": [3],
                "$exists": True,
                "$size": 2,
                "$regex": "^a",
                "$options": "i",
                "$type": "number",
            },
            safe_obj_expr="json(v)",
        )
        assert "REGEXP" in sql and "(?i:^a)" in params

        # Empty $in / $nin, $exists False, exists_always_true
        s_empty, _ = _compile_json_value_operators(
            "t", "v", {"$in": [], "$nin": []}
        )
        assert "0" in s_empty and "1" in s_empty

        s_ex_f, _ = _compile_json_value_operators(
            "t", "v", {"$exists": False}, exists_always_true=True
        )
        assert "0" in s_ex_f
        s_ex_t, _ = _compile_json_value_operators(
            "t", "v", {"$exists": True}, exists_always_true=True
        )
        assert "1" in s_ex_t

        # $type string/int aliases
        for btype in (
            "string",
            "double",
            "int",
            "bool",
            "array",
            "object",
            "null",
        ):
            st, _ = _compile_json_value_operators("t", "v", {"$type": btype})
            assert st is not None
        assert (
            _compile_json_value_operators("t", "v", {"$type": "minKey"}) is None
        )

        # Invalid/fallback cases returning None
        assert (
            _compile_json_value_operators("t", "v", {"$options": "i"}) is None
        )
        assert _compile_json_value_operators("t", "v", {"$regex": 123}) is None
        assert (
            _compile_json_value_operators(
                "t", "v", {"$regex": "^a", "$options": 123}
            )
            is None
        )
        s_size_neg, p_size_neg = _compile_json_value_operators(
            "t", "v", {"$size": -1}
        )
        assert "json_array_length" in s_size_neg and p_size_neg == [-1]
        assert _compile_json_value_operators("t", "v", {"$size": "bad"}) == (
            "0",
            [],
        )
        assert (
            _compile_json_value_operators("t", "v", {"$in": "not_list"}) is None
        )
        assert _compile_json_value_operators("t", "v", {"$nin": [[1]]}) is None
        assert _compile_json_value_operators("t", "v", {"$eq": [1]}) is None
        assert _compile_json_value_operators("t", "v", {"$ne": [1]}) is None
        assert (
            _compile_json_value_operators("t", "v", {"$gt": "not_num"})
            is not None
        )
        assert _compile_json_value_operators("t", "v", {"$gt": [1]}) is None
        assert _compile_json_value_operators("t", "v", {"$exists": "yes"}) == (
            "t IS NOT NULL",
            [],
        )
        assert _compile_json_value_operators("t", "v", {"$unknown": 1}) is None

    def test_compile_elem_predicate_variants(self):
        assert _compile_elem_predicate("_je0", 42)[0] == (
            "_je0.type IN ('integer', 'real') AND _je0.value = ?"
        )
        assert _compile_elem_predicate("_je0", Binary(b"ab")) is None
        assert _compile_elem_predicate("_je0", {}) == ("1", [])
        assert (
            _compile_elem_predicate("_je0", {"$gte": 1, "$lte": 5}) is not None
        )
        # Mixed $ operator and field name returns None
        assert _compile_elem_predicate("_je0", {"$gt": 1, "a": 2}) is None
        # Nested object field predicate with scalar, null, and operator dict
        s_obj, p_obj = _compile_elem_predicate(
            "_je0", {"a": 1, "b": None, "c": {"$regex": "^x", "$options": "i"}}
        )
        assert "json_extract" in s_obj and "(?i:^x)" in p_obj
        assert _compile_elem_predicate("_je0", {"a": {"$unknown": 1}}) is None


class TestLegacyAndDirectSqlUpdateMethods:
    """Cover _can_use_sql_updates, _perform_sql_update, _perform_enhanced_sql_update, clauses & validators."""

    def test_can_use_sql_updates_and_clauses(self):
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.items
            coll.insert_one({"_id": 1, "a": 1, "arr": [1, 2], "s": "txt"})
            h = coll.query_engine.helpers

            assert h._can_use_sql_updates({"$set": {"a": 2}}, 0) is False
            assert (
                h._can_use_sql_updates({"$unsupported": {"a": 1}}, 1) is False
            )
            assert (
                h._can_use_sql_updates(
                    {"$min": {"a": Binary(b"x")}}, 1, {"a": 1}
                )
                is False
            )
            assert (
                h._can_use_sql_updates({"$set": {"arr.$": 1}}, 1, {"arr": [1]})
                is False
            )
            assert (
                h._can_use_sql_updates({"$pull": {"s": 1}}, 1, {"s": "txt"})
                is False
            )
            assert (
                h._can_use_sql_updates(
                    {"$pull": {"arr": {"$gt": 1}}}, 1, {"arr": [1, 2]}
                )
                is False
            )
            assert (
                h._can_use_sql_updates(
                    {"$set": {"a": 2}, "$push": {"arr": 3}}, 1, {"arr": [1, 2]}
                )
                is True
            )

            # _build_update_clause coverage
            assert h._build_update_clause({}) is None
            assert (
                h._build_update_clause({"$set": {"d": {"nested": 1}}})
                is not None
            )
            assert (
                h._build_update_clause(
                    {
                        "$min": {"a": 0},
                        "$max": {"a": 10},
                        "$push": {"arr": 3},
                        "$pop": {"arr": 1},
                        "$pull": {"arr": 1},
                        "$pullAll": {"arr": [1, 2]},
                        "$currentDate": {"ts": True},
                    }
                )
                is not None
            )
            assert h._build_update_clause({"$addToSet": {"arr": 1}}) is None

            # _build_sql_update_clause coverage across all operators + errors
            for op, val in [
                ("$set", {"x": 1, "obj": {"k": 1}, "b": True, "n": None}),
                ("$unset", {"x": ""}),
                ("$inc", {"a": 2}),
                ("$mul", {"a": 3}),
                ("$min", {"a": 0}),
                ("$max", {"a": 10}),
                (
                    "$push",
                    {"arr": {"$each": [4, 5], "$position": 0, "$slice": 3}},
                ),
                ("$push", {"arr": {"$each": [6], "$slice": 0}}),
                ("$push", {"arr": {"$each": [7], "$slice": -2}}),
                ("$push", {"arr": 9}),
                ("$pop", {"arr": 1}),
                ("$pop", {"arr": -1}),
                ("$pull", {"arr": 1}),
                ("$pullAll", {"arr": [1, 2]}),
                ("$addToSet", {"arr": {"$each": [1, 2]}}),
                ("$addToSet", {"arr": 3}),
                ("$bit", {"a": {"and": 1, "or": 2, "xor": 4}}),
                ("$currentDate", {"ts": True, "ts2": {"$type": "date"}}),
                ("$rename", {"a": "a2"}),
                ("$setOnInsert", {"init": 1}),
            ]:
                clauses, params = h._build_sql_update_clause(op, val)
                assert isinstance(clauses, list)

            assert h._build_sql_update_clause("$badOp", {}) == ([], [])
            for bin_op in ("$inc", "$mul", "$min", "$max"):
                b_clauses, b_params = h._build_sql_update_clause(
                    bin_op, {"a": b"x"}
                )
                assert b_clauses and b_params

    def test_perform_enhanced_and_standard_sql_update_and_inc_mul_validation(
        self,
    ):
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.items
            doc_id = coll.insert_one(
                {
                    "a": 5,
                    "b": "hello",
                    "null_num": None,
                    "arr": [1, 2, 3, 4],
                    "tags": ["t1", "t2"],
                    "flags": 7,
                    "old": {"nested": 42},
                    "nested": {"k": 1},
                }
            ).inserted_id
            int_id = conn.db.execute("SELECT id FROM items").fetchone()[0]
            h = coll.query_engine.helpers

            # _perform_enhanced_sql_update across all operators
            updated = h._perform_enhanced_sql_update(
                doc_id,
                {
                    "$set": {"nested.k": 2, "obj": {"x": 1}, "b_flag": True},
                    "$unset": {"b": ""},
                    "$inc": {"a": 2, "missing_inc": 3, "null_num": 1},
                    "$mul": {"a": 2, "missing_mul": 5},
                    "$min": {"a": 1, "new_min": 9},
                    "$max": {"a": 100, "new_max": 3},
                    "$push": {
                        "arr": {"$each": [10, 11], "$position": 1, "$slice": 4},
                        "new_arr": {"$each": []},
                    },
                    "$pop": {"arr": 1},
                    "$pull": {"arr": 2},
                    "$pullAll": {"tags": ["t1"]},
                    "$addToSet": {
                        "tags": {"$each": ["t2", "t3"]},
                        "new_set": "first",
                    },
                    "$bit": {"flags": {"and": 3, "or": 8, "xor": 1}},
                    "$currentDate": {
                        "dt1": True,
                        "dt2": {"$type": "date"},
                        "ts": {"$type": "timestamp"},
                    },
                    "$rename": {"old.nested": "moved.nested", "missing": "no"},
                    "$setOnInsert": {"ignored": 1},
                },
                coll.find_one({"_id": doc_id}),
            )
            assert updated["nested"]["k"] == 2
            assert updated["moved"]["nested"] == 42
            assert "t1" not in updated["tags"] and "t3" in updated["tags"]
            assert updated["missing_inc"] == 3
            assert updated["missing_mul"] == 0

            # Negative position & slice=0 & negative slice & scalar push in enhanced update
            updated2 = h._perform_enhanced_sql_update(
                doc_id,
                {
                    "$push": {
                        "arr": {"$each": [99], "$position": -1, "$slice": -2},
                        "empty_after": {"$each": [1, 2], "$slice": 0},
                    },
                    "$pop": {"arr": -1},
                    "$pull": {"tags": "t2"},
                    "$pullAll": {"tags": []},
                    "$addToSet": {"bin_set": Binary(b"bin")},
                },
                updated,
            )
            assert updated2["empty_after"] == []

            # original_doc=None uses _get_document_fields + json_replace + Binary $set
            res_replaced = h._perform_enhanced_sql_update(
                doc_id, {"$set": {"a": 11, "bin_f": Binary(b"bb")}}
            )
            assert res_replaced["a"] == 11 and res_replaced["bin_f"] == b"bb"

            # Error branches in _perform_enhanced_sql_update & _perform_sql_update
            with pytest.raises(ValueError, match="Could not find integer ID"):
                h._perform_enhanced_sql_update(
                    ObjectId(), {"$set": {"a": 1}}, {}
                )
            with pytest.raises(RuntimeError):
                h._perform_sql_update(999999, {"$set": {"a": 1}})
            with pytest.raises(RuntimeError, match="No valid operations"):
                h._perform_sql_update(int_id, {})

            # _perform_sql_update works with integer row id for basic & extended ops
            res_basic = h._perform_sql_update(
                int_id, {"$set": {"basic": "ok"}, "$unset": {"b_flag": ""}}
            )
            assert res_basic["basic"] == "ok" and "b_flag" not in res_basic

            # _validate_inc_mul_sql & _validate_inc_mul_types_sql
            from neosqlite.collection.query_helper._sql_updates import (
                SqlUpdatesMixin,
            )

            assert h._validate_inc_mul_sql("", [], {"$set": {"a": 1}}) is True
            assert (
                h._validate_inc_mul_sql(
                    "WHERE id = ?", [int_id], {"$inc": {"a": 1}}, limit_one=True
                )
                is True
            )
            assert (
                h._validate_inc_mul_sql(
                    "WHERE id = ?",
                    [int_id],
                    {"$inc": {"basic": 1}},
                    limit_one=False,
                )
                is False
            )
            assert (
                SqlUpdatesMixin._validate_inc_mul_types_sql(
                    conn.db,
                    "items",
                    "WHERE id = ?",
                    [int_id],
                    {"$inc": {"a": 1}, "$mul": {"a": 2}},
                    h.jsonb.jsonb_supported,
                )
                is True
            )
            assert (
                SqlUpdatesMixin._validate_inc_mul_types_sql(
                    conn.db,
                    "items",
                    None,
                    [],
                    {"$inc": {"basic": 1}},
                    h.jsonb.jsonb_supported,
                )
                is False
            )
            assert (
                SqlUpdatesMixin._validate_inc_mul_types_sql(
                    conn.db,
                    "items",
                    None,
                    [],
                    {"$set": {"a": 1}},
                    h.jsonb.jsonb_supported,
                )
                is True
            )


class TestModernComposableSqlUpdatesEndToEnd:
    """Exercise every composable _build_set_data_expression path via Collection APIs."""

    def test_positional_and_array_filters_and_binary_and_id_fallback(self):
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.docs
            doc_id = coll.insert_one(
                {
                    "arr": [1, 5, 10],
                    "objs": [
                        {"id": 1, "tags": [{"k": 1}, {"k": 10}]},
                        {"id": 2, "tags": [{"k": 2}, {"k": 20}]},
                    ],
                    "bin": Binary(b"init"),
                }
            ).inserted_id

            # Nested positional $[] and $[elem] with multiple operators
            coll.update_one(
                {"_id": doc_id},
                {"$set": {"arr.$[el]": 99}},
                array_filters=[{"el": {"$gt": 4, "$lte": 10}}],
            )
            assert coll.find_one({"_id": doc_id})["arr"] == [1, 99, 99]

            # All-elements positional $[]
            coll.update_one(
                {"_id": doc_id},
                {"$set": {"objs.$[].tags.$[t].flag": True}},
                array_filters=[{"t": {"k": {"$gte": 10}}}],
            )
            doc = coll.find_one({"_id": doc_id})
            assert doc["objs"][0]["tags"][1]["flag"] is True
            assert "flag" not in doc["objs"][0]["tags"][0]

            # Nested positional $[a].tags.$[b].flag
            coll.update_one(
                {"_id": doc_id},
                {"$set": {"objs.$[o].tags.$[t].flag2": "yes"}},
                array_filters=[{"o": {"id": 2}}, {"t": {"k": {"$eq": 20}}}],
            )
            doc = coll.find_one({"_id": doc_id})
            assert doc["objs"][1]["tags"][1]["flag2"] == "yes"
            assert "flag2" not in doc["objs"][0]["tags"][1]

            # Binary values inside $set / $setOnInsert / $addToSet / $pull
            coll.update_one(
                {"_id": doc_id},
                {
                    "$set": {"bin2": Binary(b"abc")},
                    "$addToSet": {"bin_arr": Binary(b"b1")},
                },
            )
            doc = coll.find_one({"_id": doc_id})
            assert doc["bin2"] == b"abc"
            assert doc["bin_arr"] == [Binary(b"b1")]

            # Upsert with _id in $set / $setOnInsert uses Python fallback cleanly
            up = coll.update_one(
                {"_id": 999},
                {"$set": {"name": "upserted"}, "$setOnInsert": {"created": 1}},
                upsert=True,
            )
            assert up.upserted_id == 999

            # Updating _id directly returns None in SQL tier and falls back cleanly
            h = coll.query_engine.helpers
            assert h._build_set_data_expression({"$set": {"_id": 123}}) is None
            assert h._build_set_data_expression({}) is None
            assert h._build_set_data_expression({"$badOp": {"a": 1}}) is None
            assert h._build_set_data_expression({"$set": "bad"}) is None

    def test_pull_pullall_addtoset_push_rename_bit_minmax_currentdate(self):
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.ops
            coll.insert_many(
                [
                    {
                        "_id": 1,
                        "nums": [1, 2, 3, 4, 5],
                        "objs": [{"v": 1}, {"v": 10}],
                        "tags": ["a", "b", "c"],
                        "bits": 5,
                        "lo": 10,
                        "hi": 10,
                        "src": "keep",
                        "str_lo": "zebra",
                    },
                    {
                        "_id": 2,
                        "nums": [2, 6],
                        "objs": [{"v": 20}],
                        "tags": ["b"],
                        "bits": 3,
                        "lo": None,
                        "hi": None,
                        "src": "v2",
                        "str_lo": "apple",
                    },
                ]
            )

            # $pull with operator condition and dict match, $pullAll non-empty & empty
            coll.update_many(
                {},
                {
                    "$pull": {"objs": {"v": {"$gte": 10}}},
                    "$pullAll": {"tags": ["b"]},
                },
            )
            assert coll.find_one({"_id": 1})["objs"] == [{"v": 1}]
            assert coll.find_one({"_id": 2})["objs"] == []
            assert coll.find_one({"_id": 2})["tags"] == []

            coll.update_one({"_id": 1}, {"$pullAll": {"tags": []}})
            assert coll.find_one({"_id": 1})["tags"] == ["a", "c"]

            # $push with negative $position, $slice: 0, negative $slice
            coll.update_one(
                {"_id": 1},
                {
                    "$push": {
                        "nums": {
                            "$each": [99],
                            "$position": -1,
                            "$slice": -3,
                        },
                        "tags": {"$each": ["x"], "$slice": 0},
                    }
                },
            )
            d1 = coll.find_one({"_id": 1})
            assert len(d1["nums"]) == 3 and 99 in d1["nums"]
            assert d1["tags"] == []

            # $addToSet $each + fallback for >20 elements
            coll.update_one(
                {"_id": 1}, {"$addToSet": {"nums": {"$each": [7, 8, 99]}}}
            )
            assert 7 in coll.find_one({"_id": 1})["nums"]

            coll.update_one(
                {"_id": 2},
                {"$addToSet": {"nums": {"$each": list(range(25))}}},
            )
            assert len(coll.find_one({"_id": 2})["nums"]) >= 25

            # $rename (including missing source and self-rename), $bit, $currentDate, $min/$max, $pop
            coll.update_many(
                {},
                {
                    "$rename": {
                        "src": "dst",
                        "missing_field": "nowhere",
                        "bits": "bits",
                    },
                    "$bit": {"bits": {"and": 6, "or": 8, "xor": 1}},
                    "$currentDate": {"ts": True, "ts_d": {"$type": "date"}},
                    "$min": {"lo": 5, "str_lo": "mango"},
                    "$max": {"hi": 20, "new_hi": 99},
                    "$pop": {"nums": -1},
                },
            )
            d1_after = coll.find_one({"_id": 1})
            d2_after = coll.find_one({"_id": 2})
            assert "src" not in d1_after and d1_after["dst"] == "keep"
            assert d1_after["lo"] == 5 and d1_after["hi"] == 20
            assert d1_after["str_lo"] == "mango"
            assert d2_after["str_lo"] == "apple"
            assert d2_after["lo"] == 5 and d2_after["hi"] == 20
            assert d2_after["new_hi"] == 99

            # Update multi with missing field $inc fallback & $min/$max Binary fallback
            coll.update_one({"_id": 1}, {"$min": {"bin_cmp": Binary(b"\x01")}})
            assert coll.find_one({"_id": 1})["bin_cmp"] == Binary(b"\x01")
