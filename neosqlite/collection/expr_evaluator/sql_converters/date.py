"""SQL converters for date operators."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass


from .base import BaseSqlMixin


class DateMixin(BaseSqlMixin):
    """$year / $month / $dayOfMonth / $dateAdd / $dateSubtract / $dateDiff → SQL."""

    def _convert_date_operator(
        self, operator: str, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert date operators to SQL using strftime."""
        # Normalize operands to handle both single values and lists
        if not isinstance(operands, list):
            operands = [operands]

        if len(operands) != 1:
            raise ValueError(f"{operator} requires exactly 1 operand")

        value_sql, value_params = self._convert_operand_to_sql(operands[0])

        # SQLite strftime format codes
        match operator:
            case "$year":
                fmt = "%Y"
            case "$month":
                fmt = "%m"
            case "$dayOfMonth":
                fmt = "%d"
            case "$hour":
                fmt = "%H"
            case "$minute":
                fmt = "%M"
            case "$second":
                fmt = "%S"
            case "$dayOfWeek":
                fmt = "%w"
                sql = f"(CAST(strftime('{fmt}', {value_sql}) AS INTEGER) + 1)"
                return sql, value_params
            case "$dayOfYear":
                fmt = "%j"
            case "$week":
                fmt = "%U"
            case "$isoDayOfWeek":
                fmt = "%u"
            case "$isoWeek":
                fmt = "%V"
            case "$millisecond":
                fmt = "%f"
            case _:
                raise NotImplementedError(
                    f"Date operator {operator} not supported in SQL tier"
                )

        # For numeric results, cast to integer
        if operator == "$millisecond":
            sql = (
                f"cast(strftime('{fmt}', {value_sql}) * 1000 as integer) % 1000"
            )
        else:
            sql = f"cast(strftime('{fmt}', {value_sql}) as integer)"

        return sql, value_params

    def _convert_date_arithmetic_operator(
        self, operator: str, operands: list[Any]
    ) -> tuple[str, list[Any]]:
        """Convert $dateAdd/$dateSubtract operators to SQL.

        MongoDB syntax: {$dateAdd: [date, amount, unit]} or
                        {$dateAdd: {startDate: date, amount: N, unit: "day"}}
        SQLite: datetime(date, '+N unit' or '-N unit')
        """
        # Handle MongoDB dict format: {startDate, amount, unit}
        if isinstance(operands, dict):
            operands = [
                operands.get("startDate"),
                operands.get("amount"),
                operands.get("unit", "day"),
            ]

        if len(operands) < 2 or len(operands) > 3:
            raise ValueError(
                f"{operator} requires 2-3 operands: [date, amount, unit]"
            )

        date_sql, date_params = self._convert_operand_to_sql(operands[0])
        amount = operands[1]  # Should be a literal number
        unit = operands[2] if len(operands) > 2 else "day"  # Default to days

        # Validate unit
        valid_units = (
            "day",
            "hour",
            "minute",
            "second",
            "week",
            "month",
            "year",
        )
        if not isinstance(unit, str) or unit not in valid_units:
            raise ValueError(f"{operator} unit must be one of: {valid_units}")

        # Handle year/month specially (SQLite doesn't support directly)
        if unit == "year":
            amount = amount * 12
            unit = "month"

        # Determine sign based on operator
        sign = "+" if operator == "$dateAdd" else "-"

        # Handle week conversion to days
        sqlite_unit = unit
        if unit == "week":
            sqlite_unit = "day"
            if isinstance(amount, (int, float)):
                amount = amount * 7

        # Build the modifier
        if isinstance(amount, (int, float)):
            modifier = f"'{sign}{amount} {sqlite_unit}s'"
            # Use strftime with 'T' separator and 'Z' suffix so
            # neosqlite_json_loads recognizes the result as a UTC ISO
            # date and converts it back to a timezone-aware datetime
            sql = f"strftime('%Y-%m-%dT%H:%M:%SZ', {date_sql}, {modifier})"
            return sql, date_params
        else:
            # Amount is a field reference - need to use CASE or build dynamically
            # For simplicity, we'll use printf to build the modifier
            amount_sql, amount_params = self._convert_operand_to_sql(
                operands[1]
            )
            if sign == "-":
                amount_sql = f"-({amount_sql})"

            # Use strftime with 'T' separator and 'Z' suffix so
            # neosqlite_json_loads recognizes the result as a UTC ISO
            # date and converts it back to a timezone-aware datetime
            sql = f"strftime('%Y-%m-%dT%H:%M:%SZ', {date_sql}, printf('%+d {sqlite_unit}s', {amount_sql}))"
            return sql, date_params + amount_params

    def _convert_date_diff_operator(
        self, operands: list[Any]
    ) -> tuple[str, list[Any]]:
        """Convert $dateDiff operator to SQL.

        MongoDB syntax: {$dateDiff: [date1, date2, unit]} or
                        {$dateDiff: {startDate: date1, endDate: date2, unit: "day"}}
        SQLite: julianday(date2) - julianday(date1) for days
        """
        # Handle MongoDB dict format: {startDate, endDate, unit}
        if isinstance(operands, dict):
            operands = [
                operands.get("startDate"),
                operands.get("endDate"),
                operands.get("unit", "day"),
            ]

        if len(operands) < 2 or len(operands) > 3:
            raise ValueError(
                "$dateDiff requires 2-3 operands: [date1, date2, unit]"
            )

        date1_sql, date1_params = self._convert_operand_to_sql(operands[0])
        date2_sql, date2_params = self._convert_operand_to_sql(operands[1])
        unit = operands[2] if len(operands) > 2 else "day"

        # Validate unit
        valid_units = (
            "day",
            "hour",
            "minute",
            "second",
            "week",
            "month",
            "year",
        )
        if not isinstance(unit, str) or unit not in valid_units:
            raise ValueError(f"$dateDiff unit must be one of: {valid_units}")

        # For month and year, use SQLite strftime to extract components
        # and compute the difference directly (julianday-based division
        # is inaccurate for month/year units).
        if unit in ("month", "year"):
            sql = f"""(
                (strftime('%Y', {date2_sql}) - strftime('%Y', {date1_sql})) * 12
                + (strftime('%m', {date2_sql}) - strftime('%m', {date1_sql}))
            )"""
            if unit == "year":
                sql = f"cast({sql} / 12 as integer)"
            return sql, date2_params + date1_params

        # Base calculation: difference in days
        sql = f"(julianday({date2_sql}) - julianday({date1_sql}))"

        # Convert to requested unit
        unit_multipliers = {
            "day": 1,
            "week": 1.0 / 7,
            "hour": 24,
            "minute": 24 * 60,
            "second": 24 * 60 * 60,
        }

        multiplier = unit_multipliers.get(unit, 1)
        if multiplier != 1:
            sql = f"cast({sql} * {multiplier} as integer)"
        else:
            sql = f"cast({sql} as integer)"

        # Params must match placeholder order: date2 first, then date1
        return sql, date2_params + date1_params
