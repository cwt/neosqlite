"""SQL converters for type converters operators."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass


from .base import BaseSqlMixin


class TypeConvertersMixin(BaseSqlMixin):

    def _get_operator_return_type(self, operator: str) -> str | None:
        """
        Infer the BSON return type of a MongoDB operator.

        Returns:
            BSON type name (e.g., 'number', 'bool', 'string', 'array', 'object')
            or None if the return type is ambiguous or unknown.
        """
        match operator:
            # Operators returning numbers
            case (
                "$add"
                | "$subtract"
                | "$multiply"
                | "$divide"
                | "$mod"
                | "$abs"
                | "$ceil"
                | "$floor"
                | "$round"
                | "$trunc"
                | "$pow"
                | "$sqrt"
                | "$ln"
                | "$log"
                | "$log10"
                | "$log2"
                | "$exp"
                | "$sin"
                | "$cos"
                | "$tan"
                | "$asin"
                | "$acos"
                | "$atan"
                | "$atan2"
                | "$sinh"
                | "$cosh"
                | "$tanh"
                | "$asinh"
                | "$acosh"
                | "$atanh"
                | "$size"
                | "$indexOfArray"
                | "$sum"
                | "$avg"
                | "$min"
                | "$max"
                | "$strLenBytes"
                | "$strLenCP"
                | "$indexOfBytes"
                | "$indexOfCP"
                | "$year"
                | "$month"
                | "$dayOfMonth"
                | "$hour"
                | "$minute"
                | "$second"
                | "$millisecond"
                | "$dayOfWeek"
                | "$dayOfYear"
                | "$week"
                | "$isoDayOfWeek"
                | "$isoWeek"
                | "$dateDiff"
                | "$binarySize"
                | "$bsonSize"
                | "$toInt"
                | "$toDouble"
                | "$toLong"
                | "$toDecimal"
            ):
                return "number"

            # Operators returning booleans
            case (
                "$eq"
                | "$ne"
                | "$gt"
                | "$gte"
                | "$lt"
                | "$lte"
                | "$and"
                | "$or"
                | "$not"
                | "$nor"
                | "$in"
                | "$isArray"
                | "$setEquals"
                | "$setIsSubset"
                | "$anyElementTrue"
                | "$allElementsTrue"
                | "$regexMatch"
                | "$isNumber"
                | "$toBool"
            ):
                return "bool"

            # Operators returning strings
            case (
                "$concat"
                | "$toLower"
                | "$toUpper"
                | "$substr"
                | "$substrBytes"
                | "$trim"
                | "$ltrim"
                | "$rtrim"
                | "$replaceAll"
                | "$replaceOne"
                | "$toString"
                | "$type"
            ):
                return "string"

            # Operators returning arrays
            case (
                "$slice"
                | "$setIntersection"
                | "$setUnion"
                | "$setDifference"
                | "$split"
                | "$objectToArray"
            ):
                return "array"

            # Operators returning objects
            case "$mergeObjects" | "$getField" | "$setField" | "$unsetField":
                return "object"

            case _:
                return None

    def _get_literal_bson_type(self, value: Any) -> str | None:
        """Get the BSON type name for a literal value."""
        if value is None:
            return "null"
        if isinstance(value, bool):
            return "bool"
        if isinstance(value, (int, float)):
            return "number"
        if isinstance(value, str):
            # Check if it's a field reference, which is not a literal
            if value.startswith("$"):
                return None
            return "string"
        if isinstance(value, list):
            return "array"
        if isinstance(value, dict):
            # Check if it's an expression
            if len(value) == 1 and next(iter(value.keys())).startswith("$"):
                return None
            return "object"
        return None

    def _convert_type_operator(
        self, operator: str, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert type conversion operators to SQL."""
        # Normalize operands to handle both single values and lists
        # MongoDB allows both: {$isNumber: "$field"} and {$isNumber: ["$field"]}
        if not isinstance(operands, list):
            operands = [operands]

        if len(operands) != 1:
            raise ValueError(f"{operator} requires exactly 1 operand")

        operand = operands[0]
        value_sql, value_params = self._convert_operand_to_sql(operand)

        match operator:
            case "$toString":
                # Cast to text
                sql = f"cast({value_sql} as text)"
            case "$toInt":
                # Cast to integer, handle non-numeric strings by returning NULL
                # SQLite CAST('abc' AS INTEGER) returns 0, we want NULL for compatibility
                sql = (
                    f"CASE WHEN typeof({value_sql}) IN ('integer', 'real') THEN CAST({value_sql} AS INTEGER) "
                    f"WHEN typeof({value_sql}) = 'text' AND (CAST({value_sql} AS INTEGER) != 0 OR {value_sql} IN ('0', '0.0')) "
                    f"THEN CAST({value_sql} AS INTEGER) ELSE NULL END"
                )
            case "$toDouble":
                # Cast to real/float, handle non-numeric strings by returning NULL
                sql = (
                    f"CASE WHEN typeof({value_sql}) IN ('integer', 'real') THEN CAST({value_sql} AS REAL) "
                    f"WHEN typeof({value_sql}) = 'text' AND (CAST({value_sql} AS REAL) != 0.0 OR {value_sql} IN ('0', '0.0')) "
                    f"THEN CAST({value_sql} AS REAL) ELSE NULL END"
                )
            case "$toLong":
                # SQLite integers are already 64-bit, same as toInt logic
                sql = (
                    f"CASE WHEN typeof({value_sql}) IN ('integer', 'real') THEN CAST({value_sql} AS INTEGER) "
                    f"WHEN typeof({value_sql}) = 'text' AND (CAST({value_sql} AS INTEGER) != 0 OR {value_sql} IN ('0', '0.0')) "
                    f"THEN CAST({value_sql} AS INTEGER) ELSE NULL END"
                )
            case "$toBool":
                if isinstance(operand, str) and operand.startswith("$"):
                    field_path = operand[1:]
                    from ...json_path_utils import parse_json_path

                    json_path = parse_json_path(field_path)
                    type_expr = f"json_type({self.data_column}, '{json_path}')"
                    sql = (
                        f"CASE WHEN {type_expr} = 'null' THEN json('false') "
                        f"WHEN {type_expr} = 'false' THEN json('false') "
                        f"WHEN {type_expr} = 'true' THEN json('true') "
                        f"WHEN {type_expr} IN ('integer', 'real') THEN CASE WHEN {value_sql} != 0 THEN json('true') ELSE json('false') END "
                        f"WHEN {type_expr} = 'text' THEN CASE WHEN length({value_sql}) > 0 THEN json('true') ELSE json('false') END "
                        f"WHEN {type_expr} IN ('array', 'object') THEN json('true') "
                        f"ELSE json('false') END"
                    )
                else:
                    inferred_type = None
                    if isinstance(operand, dict) and len(operand) == 1:
                        op_name = next(iter(operand.keys()))
                        if op_name.startswith("$"):
                            inferred_type = self._get_operator_return_type(
                                op_name
                            )
                    else:
                        inferred_type = self._get_literal_bson_type(operand)

                    if inferred_type == "bool":
                        sql = f"{value_sql}"
                    elif inferred_type == "number":
                        sql = f"CASE WHEN {value_sql} != 0 THEN json('true') ELSE json('false') END"
                    elif inferred_type == "string":
                        sql = f"CASE WHEN length({value_sql}) > 0 THEN json('true') ELSE json('false') END"
                    elif inferred_type in ("array", "object"):
                        sql = "json('true')"
                        value_params = []
                    elif inferred_type == "null":
                        sql = "json('false')"
                        value_params = []
                    else:
                        sql = (
                            f"CASE WHEN typeof({value_sql}) = 'text' THEN CASE WHEN length({value_sql}) > 0 THEN json('true') ELSE json('false') END "
                            f"WHEN typeof({value_sql}) = 'null' THEN json('false') "
                            f"ELSE CASE WHEN {value_sql} != 0 THEN json('true') ELSE json('false') END END"
                        )
            case "$toDecimal":
                # SQLite doesn't have native Decimal128, use REAL
                raise NotImplementedError(
                    "$toDecimal not supported in SQL tier (SQLite lacks Decimal128)"
                )
            case "$toObjectId":
                # Cannot convert to ObjectId in SQL
                raise NotImplementedError(
                    "$toObjectId not supported in SQL tier (use Python fallback)"
                )
            case "$isNumber":
                if isinstance(operand, str) and operand.startswith("$"):
                    # Direct field reference
                    field_path = operand[1:]
                    from ...json_path_utils import parse_json_path

                    json_path = parse_json_path(field_path)
                    type_expr = f"json_type({self.data_column}, '{json_path}')"
                    sql = f"CASE WHEN {type_expr} IN ('integer', 'real') THEN json('true') ELSE json('false') END"
                else:
                    # Computed expression or literal - try to infer type
                    inferred_type = None
                    if isinstance(operand, dict) and len(operand) == 1:
                        op_name = next(iter(operand.keys()))
                        if op_name.startswith("$"):
                            inferred_type = self._get_operator_return_type(
                                op_name
                            )
                    else:
                        inferred_type = self._get_literal_bson_type(operand)

                    if inferred_type == "number":
                        sql = "json('true')"
                        value_params = []
                    elif inferred_type is not None:
                        sql = "json('false')"
                        value_params = []
                    else:
                        raise NotImplementedError(
                            "Ambiguous type for $isNumber in SQL tier"
                        )
            case "$type":
                if isinstance(operand, str) and operand.startswith("$"):
                    # Direct field reference
                    field_path = operand[1:]
                    from ...json_path_utils import parse_json_path

                    json_path = parse_json_path(field_path)
                    type_expr = f"json_type({self.data_column}, '{json_path}')"
                    sql = (
                        f"CASE WHEN {type_expr} = 'null' THEN 'null' "
                        f"WHEN {type_expr} IN ('true', 'false') THEN 'bool' "
                        f"WHEN {type_expr} = 'integer' THEN 'int' "
                        f"WHEN {type_expr} = 'real' THEN 'double' "
                        f"WHEN {type_expr} = 'text' THEN 'string' "
                        f"WHEN {type_expr} = 'array' THEN 'array' "
                        f"WHEN {type_expr} = 'object' THEN "
                        f"  CASE WHEN json_extract({self.data_column}, '{json_path}.__neosqlite_binary__') = 1 THEN 'binData' "
                        f"  WHEN json_extract({self.data_column}, '{json_path}.__neosqlite_objectid__') = 1 THEN 'objectId' "
                        f"  ELSE 'object' END "
                        f"ELSE 'unknown' END"
                    )
                else:
                    # Computed expression or literal
                    inferred_type = None
                    if isinstance(operand, dict) and len(operand) == 1:
                        op_name = next(iter(operand.keys()))
                        if op_name.startswith("$"):
                            inferred_type = self._get_operator_return_type(
                                op_name
                            )
                    else:
                        inferred_type = self._get_literal_bson_type(operand)

                    if inferred_type == "number":
                        sql = f"CASE WHEN typeof({value_sql}) = 'integer' THEN 'int' ELSE 'double' END"
                    elif inferred_type == "bool":
                        sql = "'bool'"
                        value_params = []
                    elif inferred_type == "string":
                        sql = "'string'"
                        value_params = []
                    elif inferred_type == "array":
                        sql = "'array'"
                        value_params = []
                    elif inferred_type == "object":
                        sql = (
                            f"CASE WHEN typeof({value_sql}) = 'text' THEN "
                            f"  CASE WHEN json_extract({value_sql}, '$.__neosqlite_binary__') = 1 THEN 'binData' "
                            f"  WHEN json_extract({value_sql}, '$.__neosqlite_objectid__') = 1 THEN 'objectId' "
                            f"  ELSE 'object' END "
                            f"ELSE 'object' END"
                        )
                    elif inferred_type == "null":
                        sql = "'null'"
                        value_params = []
                    else:
                        # Fallback to typeof
                        sql = (
                            f"CASE WHEN typeof({value_sql}) = 'null' THEN 'null' "
                            f"WHEN typeof({value_sql}) = 'integer' THEN 'int' "
                            f"WHEN typeof({value_sql}) = 'real' THEN 'double' "
                            f"WHEN typeof({value_sql}) = 'text' THEN 'string' "
                            f"ELSE 'unknown' END"
                        )
            case "$convert":
                # $convert is complex - requires 'to' field specification
                # Fall back to Python
                raise NotImplementedError(
                    "$convert not supported in SQL tier (use Python fallback)"
                )
            case _:
                raise NotImplementedError(
                    f"Type operator {operator} not supported in SQL tier"
                )

        return sql, value_params
