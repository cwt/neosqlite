"""Comprehensive tests for SQL type converter operators in ExprEvaluator."""

from datetime import datetime, timezone

import pytest

import neosqlite
from neosqlite.binary import Binary
from neosqlite.collection.expr_evaluator import ExprEvaluator
from neosqlite.objectid import ObjectId


class TestTypeConvertersDirectAndSQL:
    """Test _get_operator_return_type, _get_literal_bson_type, and _convert_type_operator."""

    def test_get_operator_return_type_all_categories(self):
        evaluator = ExprEvaluator()

        # Numeric operators return "number"
        for op in (
            "$add",
            "$subtract",
            "$multiply",
            "$divide",
            "$mod",
            "$abs",
            "$ceil",
            "$floor",
            "$round",
            "$trunc",
            "$pow",
            "$sqrt",
            "$ln",
            "$log",
            "$log10",
            "$log2",
            "$exp",
            "$sin",
            "$cos",
            "$tan",
            "$asin",
            "$acos",
            "$atan",
            "$atan2",
            "$sinh",
            "$cosh",
            "$tanh",
            "$asinh",
            "$acosh",
            "$atanh",
            "$avg",
            "$min",
            "$max",
            "$sum",
            "$strLenBytes",
            "$strLenCP",
            "$indexOfBytes",
            "$indexOfCP",
            "$size",
            "$year",
            "$month",
            "$dayOfMonth",
            "$hour",
            "$minute",
            "$second",
            "$millisecond",
            "$dayOfWeek",
            "$dayOfYear",
            "$week",
            "$isoWeek",
            "$isoDayOfWeek",
            "$dateDiff",
            "$toInt",
            "$toLong",
            "$toDouble",
            "$toDecimal",
        ):
            assert evaluator._get_operator_return_type(op) == "number", op

        # Boolean operators return "bool"
        for op in (
            "$eq",
            "$ne",
            "$gt",
            "$gte",
            "$lt",
            "$lte",
            "$and",
            "$or",
            "$not",
            "$nor",
            "$in",
            "$isArray",
            "$regexMatch",
            "$allElementsTrue",
            "$anyElementTrue",
            "$setIsSubset",
            "$setEquals",
            "$isNumber",
            "$toBool",
        ):
            assert evaluator._get_operator_return_type(op) == "bool", op

        # String operators return "string"
        for op in (
            "$concat",
            "$toLower",
            "$toUpper",
            "$substr",
            "$trim",
            "$type",
            "$toString",
        ):
            assert evaluator._get_operator_return_type(op) == "string", op

        # Array operators return "array"
        for op in (
            "$split",
            "$slice",
            "$setDifference",
            "$setIntersection",
            "$setUnion",
            "$objectToArray",
        ):
            assert evaluator._get_operator_return_type(op) == "array", op

        # Object operators return "object"
        for op in ("$mergeObjects", "$setField", "$unsetField", "$getField"):
            assert evaluator._get_operator_return_type(op) == "object", op

        # Ambiguous / unsupported / dynamic return types return None
        for op in (
            "$degreesToRadians",
            "$radiansToDegrees",
            "$ifNull",
            "$cond",
            "$switch",
            "$literal",
            "$cmp",
            "$toObjectId",
            "$toDate",
            "$convert",
            "$unknown",
        ):
            assert evaluator._get_operator_return_type(op) is None, op

    def test_get_literal_bson_type_all_variants(self):
        evaluator = ExprEvaluator()

        assert evaluator._get_literal_bson_type(None) == "null"
        assert evaluator._get_literal_bson_type(True) == "bool"
        assert evaluator._get_literal_bson_type(False) == "bool"
        assert evaluator._get_literal_bson_type(42) == "number"
        assert evaluator._get_literal_bson_type(2**40) == "number"
        assert evaluator._get_literal_bson_type(3.14) == "number"
        assert evaluator._get_literal_bson_type("hello") == "string"
        assert evaluator._get_literal_bson_type([1, 2]) == "array"
        assert evaluator._get_literal_bson_type({"a": 1}) == "object"
        assert (
            evaluator._get_literal_bson_type(datetime.now(timezone.utc)) is None
        )
        assert evaluator._get_literal_bson_type(ObjectId()) is None
        assert evaluator._get_literal_bson_type(Binary(b"ab")) is None

    def test_convert_type_operator_errors_and_unsupported(self):
        evaluator = ExprEvaluator()

        # Wrong operand counts
        with pytest.raises(ValueError, match="requires exactly 1 operand"):
            evaluator._convert_type_operator("$toInt", [])
        with pytest.raises(ValueError, match="requires exactly 1 operand"):
            evaluator._convert_type_operator("$toInt", ["$a", "$b"])

        # $_id field references require Python fallback
        for op in ("$toBool", "$isNumber", "$type"):
            with pytest.raises(NotImplementedError, match=r"\$_id"):
                evaluator._convert_type_operator(op, "$_id")

        # Operators delegated to Python fallback
        with pytest.raises(NotImplementedError, match=r"\$toDecimal"):
            evaluator._convert_type_operator("$toDecimal", "$a")
        with pytest.raises(NotImplementedError, match=r"\$toObjectId"):
            evaluator._convert_type_operator("$toObjectId", "$a")
        with pytest.raises(NotImplementedError, match=r"\$convert"):
            evaluator._convert_type_operator(
                "$convert", {"input": "$a", "to": "int"}
            )
        with pytest.raises(
            NotImplementedError, match=r"Type operator \$toDate not supported"
        ):
            evaluator._convert_type_operator("$toDate", "$a")

        # $isNumber ambiguous expression & unknown operator
        with pytest.raises(
            NotImplementedError, match="Ambiguous type for \\$isNumber"
        ):
            evaluator._convert_type_operator(
                "$isNumber", {"$ifNull": ["$a", 1]}
            )
        with pytest.raises(NotImplementedError):
            evaluator._convert_type_operator("$isNumber", {"$unknown": 1})

    def test_to_long_to_bool_and_is_number_sql(self):
        evaluator = ExprEvaluator()

        # $toLong produces CAST(... AS INTEGER)
        sql, params = evaluator._convert_type_operator("$toLong", "$n")
        assert "CAST(" in sql and "AS INTEGER" in sql
        assert params == []

        # $toBool with sub-expression whose return type is bool/string/number/array/object/unknown
        sql, params = evaluator._convert_type_operator(
            "$toBool", {"$eq": ["$a", 1]}
        )
        assert sql == "json_extract(data, '$.a') = ?"
        assert params == [1]

        sql, params = evaluator._convert_type_operator(
            "$toBool", {"$concat": ["$s", "x"]}
        )
        assert "length(" in sql
        assert params == ["x"]

        sql, params = evaluator._convert_type_operator(
            "$toBool", {"$add": ["$a", 1]}
        )
        assert "!= 0" in sql
        assert params == [1]

        assert evaluator._convert_type_operator(
            "$toBool", {"$split": ["$a", ","]}
        ) == ("1", [])
        assert evaluator._convert_type_operator(
            "$toBool", {"$mergeObjects": [{"a": 1}]}
        ) == ("1", [])

        # Unknown/ambiguous return type dict falls back to typeof CASE in SQL
        sql, params = evaluator._convert_type_operator(
            "$toBool", {"$regexFind": {"input": "$a", "regex": "x"}}
        )
        assert "typeof(json(" in sql
        assert params == ["x"]

        # Literal $toBool cases
        assert evaluator._convert_type_operator("$toBool", None) == ("0", [])
        assert evaluator._convert_type_operator("$toBool", True) == (
            "?",
            [True],
        )
        assert evaluator._convert_type_operator("$toBool", False) == (
            "?",
            [False],
        )
        sql_lit, params_lit = evaluator._convert_type_operator("$toBool", 42)
        assert "!= 0" in sql_lit and params_lit == [42]
        sql_str, params_str = evaluator._convert_type_operator("$toBool", "hi")
        assert "length(" in sql_str and params_str == ["hi"]
        assert evaluator._convert_type_operator("$toBool", [[1, 2]]) == (
            "1",
            [],
        )
        # Empty/literal dict falls through to SQL typeof CASE
        sql_obj, params_obj = evaluator._convert_type_operator(
            "$toBool", {"k": "v"}
        )
        assert "typeof(json(" in sql_obj and params_obj == ['{"k": "v"}']

        # $isNumber with sub-expressions and literals
        assert evaluator._convert_type_operator(
            "$isNumber", {"$add": ["$a", 1]}
        ) == ("1", [])
        for op_expr in (
            {"$eq": ["$a", 1]},
            {"$concat": ["$a", "b"]},
            {"$split": ["$a", ","]},
            {"$mergeObjects": [{"a": 1}]},
        ):
            assert evaluator._convert_type_operator("$isNumber", op_expr) == (
                "0",
                [],
            )

        assert evaluator._convert_type_operator("$isNumber", 42) == ("1", [])
        assert evaluator._convert_type_operator("$isNumber", 3.14) == ("1", [])
        assert evaluator._convert_type_operator("$isNumber", True) == ("0", [])
        assert evaluator._convert_type_operator("$isNumber", "42") == ("0", [])
        assert evaluator._convert_type_operator("$isNumber", None) == ("0", [])
        assert evaluator._convert_type_operator("$isNumber", [[1, 2]]) == (
            "0",
            [],
        )
        # Unknown literal type in $isNumber raises NotImplementedError (ambiguous)
        with pytest.raises(
            NotImplementedError, match=r"Ambiguous type for \$isNumber"
        ):
            evaluator._convert_type_operator("$isNumber", Binary(b"x"))

    def test_type_operator_sql_expressions_and_literals(self):
        evaluator = ExprEvaluator()

        # Field ref
        sql, params = evaluator._convert_type_operator("$type", "$val")
        assert "__neosqlite_binary__" in sql
        assert "__neosqlite_objectid__" in sql
        assert "'binData'" in sql
        assert "'objectId'" in sql
        assert params == []

        # Sub-expressions covering inferred_type branches of $type
        assert evaluator._convert_type_operator(
            "$type", {"$eq": ["$a", 1]}
        ) == ("'bool'", [])
        assert evaluator._convert_type_operator(
            "$type", {"$concat": ["$a", "x"]}
        ) == ("'string'", [])
        assert evaluator._convert_type_operator(
            "$type", {"$split": ["$a", ","]}
        ) == ("'array'", [])
        sql_add, params_add = evaluator._convert_type_operator(
            "$type", {"$add": ["$a", 1]}
        )
        assert (
            "typeof(" in sql_add
            and "'int'" in sql_add
            and "'double'" in sql_add
        )
        assert params_add == [1]

        sql_merge, params_merge = evaluator._convert_type_operator(
            "$type", {"$mergeObjects": [{"a": 1}]}
        )
        assert "'object'" in sql_merge
        assert params_merge == ['{"a": 1}']

        sql_ambig, params_ambig = evaluator._convert_type_operator(
            "$type", {"$ifNull": ["$a", 1]}
        )
        assert "'unknown'" in sql_ambig

        # Literals covering all branches of $type inferred_type
        assert evaluator._convert_type_operator("$type", None) == ("'null'", [])
        assert evaluator._convert_type_operator("$type", True) == ("'bool'", [])
        assert evaluator._convert_type_operator("$type", "hi") == (
            "'string'",
            [],
        )
        assert evaluator._convert_type_operator("$type", [[1, 2]]) == (
            "'array'",
            [],
        )
        sql_num, params_num = evaluator._convert_type_operator("$type", 7)
        assert (
            "'int'" in sql_num and "'double'" in sql_num and params_num == [7]
        )

        sql_bin, params_bin = evaluator._convert_type_operator(
            "$type", Binary(b"ab")
        )
        assert "'unknown'" in sql_bin
        assert params_bin == [Binary(b"ab")]

    def test_end_to_end_find_and_aggregate_with_type_operators(self):
        with neosqlite.Connection(":memory:") as conn:
            coll = conn.test_types
            oid = ObjectId()
            coll.insert_many(
                [
                    {"name": "null", "val": None},
                    {"name": "bool_t", "val": True},
                    {"name": "bool_f", "val": False},
                    {"name": "int", "val": 42},
                    {"name": "float", "val": 3.5},
                    {"name": "str", "val": "hello"},
                    {"name": "arr", "val": [1, 2]},
                    {"name": "obj", "val": {"k": 1}},
                    {"name": "bin", "val": Binary(b"\x01\x02")},
                    {"name": "oid", "val": oid},
                ]
            )

            bin_docs = list(
                coll.find({"$expr": {"$eq": [{"$type": "$val"}, "binData"]}})
            )
            assert [d["name"] for d in bin_docs] == ["bin"]

            oid_docs = list(
                coll.find({"$expr": {"$eq": [{"$type": "$val"}, "objectId"]}})
            )
            assert [d["name"] for d in oid_docs] == ["oid"]

            str_docs = list(
                coll.find(
                    {
                        "$expr": {
                            "$eq": [
                                {"$type": {"$concat": ["$name", "!"]}},
                                "string",
                            ]
                        }
                    }
                )
            )
            assert len(str_docs) == 10

            lit_num_docs = list(coll.find({"$expr": {"$isNumber": 99}}))
            assert len(lit_num_docs) == 10

            lit_bool_docs = list(coll.find({"$expr": {"$toBool": 1}}))
            assert len(lit_bool_docs) == 10

            # $_id in $toBool, $isNumber, $type delegates cleanly to Python fallback
            id_type_docs = list(
                coll.find({"$expr": {"$ne": [{"$type": "$_id"}, "missing"]}})
            )
            assert len(id_type_docs) == 10

            id_bool_docs = list(coll.find({"$expr": {"$toBool": "$_id"}}))
            assert len(id_bool_docs) == 10

            id_is_num_docs = list(coll.find({"$expr": {"$isNumber": "$_id"}}))
            assert id_is_num_docs == []

            proj = list(
                coll.aggregate(
                    [
                        {"$match": {"name": "int"}},
                        {"$project": {"l": {"$toLong": "$val"}}},
                    ]
                )
            )
            assert proj[0]["l"] == 42
