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

    def _convert_date_to_string_operator(
        self, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert $dateToString to SQLite strftime().

        MongoDB: { $dateToString: { format: "%Y-%m-%d", date: <expr> } }
                 or positional: [<format>, <date>]
        """
        if isinstance(operands, dict):
            fmt = operands.get("format", "%Y-%m-%d")
            date_operand = operands.get("date")
            timezone = operands.get("timezone")
        elif isinstance(operands, list) and len(operands) >= 2:
            fmt = operands[0]
            date_operand = operands[1]
            timezone = operands[2] if len(operands) > 2 else None
        else:
            raise ValueError("$dateToString requires format and date")

        # Only support literal format strings (not field references)
        if not isinstance(fmt, str):
            raise NotImplementedError(
                "$dateToString with dynamic format not supported in SQL tier"
            )

        # Only support UTC / no timezone in SQL tier
        if timezone is not None:
            tz_str = str(timezone).upper().replace(" ", "")
            if tz_str not in ("UTC", "+00:00", "Z"):
                raise NotImplementedError(
                    "$dateToString with non-UTC timezone not supported in SQL tier"
                )

        date_sql, date_params = self._convert_operand_to_sql(date_operand)

        # Convert MongoDB format to SQLite strftime format.
        # %L (milliseconds) -> %f (fractional seconds) in SQLite.
        sqlite_fmt = fmt.replace("%L", "%f")

        sql = f"strftime('{sqlite_fmt}', {date_sql})"
        return sql, date_params

    def _convert_date_trunc_operator(
        self, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert $dateTrunc to SQLite strftime().

        MongoDB: { $dateTrunc: { date: <expr>, unit: "hour" } }
        """
        if isinstance(operands, dict):
            date_operand = operands.get("date")
            unit = operands.get("unit", "day")
        elif isinstance(operands, list) and len(operands) >= 2:
            date_operand = operands[0]
            unit = operands[1]
        else:
            raise ValueError("$dateTrunc requires date and unit")

        if not isinstance(unit, str):
            raise NotImplementedError(
                "$dateTrunc with dynamic unit not supported in SQL tier"
            )

        date_sql, date_params = self._convert_operand_to_sql(date_operand)

        # Map MongoDB truncation units to strftime format strings.
        # The output includes 'T' separator and 'Z' suffix so
        # neosqlite_json_loads recognizes it as a UTC ISO date.
        unit_formats: dict[str, str] = {
            "year": "%Y-01-01T00:00:00Z",
            "month": "%Y-%m-01T00:00:00Z",
            "day": "%Y-%m-%dT00:00:00Z",
            "hour": "%Y-%m-%dT%H:00:00Z",
            "minute": "%Y-%m-%dT%H:%M:00Z",
            "second": "%Y-%m-%dT%H:%M:%SZ",
        }

        if unit not in unit_formats:
            raise NotImplementedError(
                f"$dateTrunc unit '{unit}' not supported in SQL tier"
            )

        fmt = unit_formats[unit]
        sql = f"strftime('{fmt}', {date_sql})"
        return sql, date_params

    def _convert_date_from_parts_operator(
        self, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert $dateFromParts to SQLite date construction.

        MongoDB: { $dateFromParts: { year: <expr>, month: <expr>, ... } }
        """
        if not isinstance(operands, dict):
            raise ValueError("$dateFromParts requires a dictionary")

        # Timezone not supported in SQL tier
        if operands.get("timezone") is not None:
            raise NotImplementedError(
                "$dateFromParts with timezone not supported in SQL tier"
            )

        year_sql, params = self._convert_operand_to_sql(operands.get("year"))

        def _field_or_default(key: str, default: int) -> tuple[str, list[Any]]:
            val = operands.get(key)
            if val is None:
                return str(default), []
            s, p = self._convert_operand_to_sql(val)
            return f"COALESCE(CAST({s} AS INTEGER), {default})", p

        month_sql, mp = _field_or_default("month", 1)
        day_sql, dp = _field_or_default("day", 1)
        hour_sql, hp = _field_or_default("hour", 0)
        minute_sql, mip = _field_or_default("minute", 0)
        second_sql, sp = _field_or_default("second", 0)
        ms_sql, msp = _field_or_default("millisecond", 0)

        all_params = params + mp + dp + hp + mip + sp + msp

        # Build ISO 8601 string via printf + strftime.
        # The 'T' separator and 'Z' suffix ensure neosqlite_json_loads
        # recognises the result as a UTC datetime.
        sql = (
            f"strftime('%Y-%m-%dT%H:%M:%fZ',"
            f" printf('%04d', CAST({year_sql} AS INTEGER)) || '-' ||"
            f" printf('%02d', {month_sql}) || '-' ||"
            f" printf('%02d', {day_sql}) || 'T' ||"
            f" printf('%02d', {hour_sql}) || ':' ||"
            f" printf('%02d', {minute_sql}) || ':' ||"
            f" printf('%02d', {second_sql}) || '.' ||"
            f" printf('%03d', {ms_sql}))"
        )
        return sql, all_params

    def _convert_date_to_parts_operator(
        self, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert $dateToParts to SQLite strftime + json_object.

        MongoDB: { $dateToParts: { date: <expr> } }
        """
        if isinstance(operands, dict):
            date_operand = operands.get("date")
        elif isinstance(operands, list) and len(operands) >= 1:
            date_operand = operands[0]
        else:
            raise ValueError("$dateToParts requires date")

        # Timezone / iso8601 / unit not supported in SQL tier
        if isinstance(operands, dict) and (
            operands.get("timezone") is not None
            or operands.get("iso8601") is not None
            or operands.get("unit") is not None
        ):
            raise NotImplementedError(
                "$dateToParts with timezone/iso8601/unit not supported in SQL tier"
            )

        date_sql, date_params = self._convert_operand_to_sql(date_operand)

        sql = (
            f"json_object("
            f"'year', CAST(strftime('%Y', {date_sql}) AS INTEGER),"
            f" 'month', CAST(strftime('%m', {date_sql}) AS INTEGER),"
            f" 'day', CAST(strftime('%d', {date_sql}) AS INTEGER),"
            f" 'hour', CAST(strftime('%H', {date_sql}) AS INTEGER),"
            f" 'minute', CAST(strftime('%M', {date_sql}) AS INTEGER),"
            f" 'second', CAST(strftime('%S', {date_sql}) AS INTEGER),"
            f" 'millisecond',"
            f" CAST(CAST(strftime('%f', {date_sql}) * 1000 AS INTEGER) % 1000"
            f" AS INTEGER)"
            f")"
        )
        return sql, date_params

    def _convert_date_from_string_operator(
        self, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert $dateFromString to SQLite strftime parsing.

        MongoDB: { $dateFromString: { dateString: <expr> } }

        SQLite strftime can parse ISO 8601 strings; non-ISO formats
        and timezone/onError/onNull options fall back to Python.
        """
        if isinstance(operands, dict):
            date_string_operand = operands.get("dateString")
            timezone = operands.get("timezone")
            on_error = operands.get("onError")
            on_null = operands.get("onNull")
        elif isinstance(operands, list) and len(operands) >= 1:
            date_string_operand = operands[0]
            timezone = operands[1] if len(operands) > 1 else None
            on_error = operands[2] if len(operands) > 2 else None
            on_null = operands[3] if len(operands) > 3 else None
        else:
            raise ValueError("$dateFromString requires dateString")

        if timezone is not None or on_error is not None or on_null is not None:
            raise NotImplementedError(
                "$dateFromString with timezone/onError/onNull not supported in SQL tier"
            )

        string_sql, string_params = self._convert_operand_to_sql(
            date_string_operand
        )

        # Use strftime to convert ISO string back to a standardised format.
        # The 'T' separator and 'Z' suffix make neosqlite_json_loads produce UTC.
        # COALESCE handles on_null: returns NULL if input is NULL.
        sql = (
            f"CASE WHEN {string_sql} IS NULL THEN NULL"
            f" ELSE strftime('%Y-%m-%dT%H:%M:%SZ', {string_sql}) END"
        )
        return sql, string_params
