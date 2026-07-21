"""Date extract and date-arithmetic Python evaluators."""

from __future__ import annotations

import calendar
import logging
from datetime import datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)

from .base import BasePythonMixin


class DatePythonMixin(BasePythonMixin):
    """Date component extraction and date arithmetic operators."""

    def _evaluate_date_python(
        self, operator: str, operands: list[Any], document: dict[str, Any]
    ) -> int | None:
        """Evaluate date operators in Python.

        MongoDB compatibility: Date operators require the field to be stored as
        BSON Date/datetime type. String dates are NOT automatically converted,
        matching MongoDB's behavior.
        """

        # Handle both list and single operand formats
        if not isinstance(operands, list):
            operands = [operands]
        if len(operands) != 1:
            raise ValueError(f"{operator} requires exactly 1 operand")

        value = self._evaluate_operand_python(operands[0], document)
        if value is None:
            return None

        # MongoDB compatibility: Only accept datetime objects, not strings
        # MongoDB's $year, $month, etc. fail with "can't convert from BSON type string to Date"
        if isinstance(value, datetime):
            dt = value
        elif isinstance(value, str):
            # Reject string dates to match MongoDB behavior
            raise ValueError(
                f"${operator} requires a date type field, got string. "
                "Store dates as datetime objects, not ISO strings."
            )
        else:
            return None

        # Extract date components
        match operator:
            case "$year":
                return dt.year
            case "$month":
                return dt.month
            case "$dayOfMonth":
                return dt.day
            case "$hour":
                return dt.hour
            case "$minute":
                return dt.minute
            case "$second":
                return dt.second
            case "$millisecond":
                return dt.microsecond // 1000
            case "$dayOfWeek":
                # MongoDB uses 1 (Sunday) to 7 (Saturday)
                # Python's weekday() returns 0 (Monday) to 6 (Sunday)
                return ((dt.weekday() + 1) % 7) + 1
            case "$dayOfYear":
                return dt.timetuple().tm_yday
            case "$week":
                # Week of year (0-53)
                return int(dt.strftime("%U"))
            case "$isoDayOfWeek":
                return dt.isocalendar()[2]  # 1=Monday
            case "$isoWeek":
                return dt.isocalendar()[1]
            case _:
                raise NotImplementedError(
                    f"Date operator {operator} not supported in Python evaluation"
                )

    def _evaluate_date_arithmetic_python(
        self, operator: str, operands: list[Any], document: dict[str, Any]
    ) -> Any:
        """Evaluate $dateAdd, $dateSubtract, $dateDiff operators in Python."""
        match operator:
            case "$dateAdd" | "$dateSubtract":
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

                value = self._evaluate_operand_python(operands[0], document)
                if value is None:
                    return None

                amount = self._evaluate_operand_python(operands[1], document)
                if amount is None:
                    return None

                unit = operands[2] if len(operands) > 2 else "day"

                # Parse date value
                if isinstance(value, str):
                    try:
                        dt = datetime.fromisoformat(
                            value.replace("Z", "+00:00")
                        )
                    except ValueError:
                        return None
                elif isinstance(value, datetime):
                    dt = value
                else:
                    return None

                # Create timedelta based on unit
                if unit == "year":
                    # Handle years separately (not supported by timedelta directly)
                    years = amount if operator == "$dateAdd" else -amount
                    try:
                        new_dt = dt.replace(year=dt.year + int(years))
                        dt = new_dt
                    except ValueError:
                        # Handle Feb 29 edge case
                        new_dt = dt.replace(year=dt.year + int(years), day=28)
                        dt = new_dt
                elif unit == "month":
                    # Handle months separately
                    months = amount if operator == "$dateAdd" else -amount
                    new_month = dt.month + int(months)
                    new_year = dt.year + (new_month - 1) // 12
                    new_month = ((new_month - 1) % 12) + 1
                    try:
                        dt = dt.replace(year=new_year, month=new_month)
                    except ValueError:
                        # Handle day overflow (e.g., Jan 31 + 1 month)
                        last_day = calendar.monthrange(new_year, new_month)[1]
                        dt = dt.replace(
                            year=new_year,
                            month=new_month,
                            day=min(dt.day, last_day),
                        )
                else:
                    # Convert to timedelta
                    delta_kwargs = {
                        f"{unit}s": (
                            amount if operator == "$dateAdd" else -amount
                        )
                    }
                    delta = timedelta(**delta_kwargs)
                    dt = dt + delta

                # Return datetime object (MongoDB compatibility)
                return dt
            case "$dateDiff":
                # Handle MongoDB dict format: {startDate, endDate, unit}
                if isinstance(operands, dict):
                    start_operand = operands.get("startDate")
                    end_operand = operands.get("endDate")
                    unit = operands.get("unit", "day")
                    # Evaluate operands
                    start = self._evaluate_operand_python(
                        start_operand, document
                    )
                    end = self._evaluate_operand_python(end_operand, document)
                else:
                    if len(operands) < 2:
                        raise ValueError(
                            "$dateDiff requires startDate and endDate"
                        )
                    start = self._evaluate_operand_python(operands[0], document)
                    end = self._evaluate_operand_python(operands[1], document)
                    unit = (
                        self._evaluate_operand_python(operands[2], document)
                        if len(operands) > 2
                        else "day"
                    )

                if start is None or end is None:
                    return None

                # Parse dates
                if isinstance(start, str):
                    try:
                        start = datetime.fromisoformat(
                            start.replace("Z", "+00:00")
                        )
                    except ValueError:
                        return None
                if isinstance(end, str):
                    try:
                        end = datetime.fromisoformat(end.replace("Z", "+00:00"))
                    except ValueError:
                        return None

                if not isinstance(start, datetime) or not isinstance(
                    end, datetime
                ):
                    return None

                # Calculate difference
                delta = end - start

                match unit:
                    case "year":
                        return end.year - start.year
                    case "month":
                        return (end.year - start.year) * 12 + (
                            end.month - start.month
                        )
                    case "day":
                        return delta.days
                    case "hour":
                        return int(delta.total_seconds() / 3600)
                    case "minute":
                        return int(delta.total_seconds() / 60)
                    case "second":
                        return int(delta.total_seconds())
                    case "millisecond":
                        return int(delta.total_seconds() * 1000)
                    case "week":
                        return delta.days // 7
                    case _:
                        return delta.days
            case "$dateFromString":
                # Handle MongoDB dict format: {dateString, timezone, onError, onNull}
                if isinstance(operands, dict):
                    date_string_operand = operands.get("dateString")
                    timezone = operands.get("timezone")
                    on_error = operands.get("onError")
                    on_null = operands.get("onNull")
                    # Evaluate the dateString operand
                    date_string = self._evaluate_operand_python(
                        date_string_operand, document
                    )
                else:
                    if len(operands) < 1:
                        raise ValueError("$dateFromString requires dateString")
                    date_string = self._evaluate_operand_python(
                        operands[0], document
                    )
                    timezone = (
                        self._evaluate_operand_python(operands[1], document)
                        if len(operands) > 1
                        else None
                    )
                    on_error = (
                        self._evaluate_operand_python(operands[2], document)
                        if len(operands) > 2
                        else None
                    )
                    on_null = (
                        self._evaluate_operand_python(operands[3], document)
                        if len(operands) > 3
                        else None
                    )

                if date_string is None:
                    return on_null

                try:
                    # If already a datetime, return it
                    if isinstance(date_string, datetime):
                        return date_string

                    # Parse ISO 8601 date string
                    if isinstance(date_string, str):
                        # Handle various ISO 8601 formats
                        date_string = date_string.replace("Z", "+00:00")
                        dt = datetime.fromisoformat(date_string)

                        # Handle timezone if specified
                        if timezone and dt.tzinfo is None:
                            # Simple timezone handling (e.g., "+05:30")
                            try:
                                from datetime import timezone as tz

                                if str(timezone).startswith("+") or str(
                                    timezone
                                ).startswith("-"):
                                    tz_str = str(timezone)
                                    hours = int(tz_str[1:3])
                                    minutes = (
                                        int(tz_str[4:6])
                                        if len(tz_str) > 4
                                        else 0
                                    )
                                    offset_seconds = hours * 3600 + minutes * 60
                                    if tz_str[0] == "-":
                                        offset_seconds = -offset_seconds
                                    dt = dt.replace(tzinfo=tz.utc)  # Simplified
                            except (ValueError, TypeError, AttributeError) as e:
                                logger.debug(
                                    f"Failed to parse timezone in $dateFromString: {e}"
                                )
                                pass

                        return dt
                    return None
                except Exception as e:
                    logger.debug(f"Failed to evaluate $dateFromString: {e}")
                    return on_error
            case "$dateToString":
                # Handle MongoDB dict format: {format, date, timezone}
                if isinstance(operands, dict):
                    fmt = operands.get("format", "%Y-%m-%d")
                    date_operand = operands.get("date")
                    timezone = operands.get("timezone")
                    # Evaluate the date operand
                    date_val = self._evaluate_operand_python(
                        date_operand, document
                    )
                else:
                    if len(operands) < 2:
                        raise ValueError(
                            "$dateToString requires format and date"
                        )
                    fmt = self._evaluate_operand_python(operands[0], document)
                    date_val = self._evaluate_operand_python(
                        operands[1], document
                    )
                    timezone = operands[2] if len(operands) > 2 else None

                if date_val is None:
                    return None

                # Parse date
                if isinstance(date_val, str):
                    try:
                        date_val = datetime.fromisoformat(
                            date_val.replace("Z", "+00:00")
                        )
                    except ValueError:
                        return None

                if not isinstance(date_val, datetime):
                    return None

                # Convert MongoDB format to Python strftime format
                # MongoDB uses %Y, %m, %d, %H, %M, %S, %L (milliseconds), %Z (timezone)
                python_fmt = fmt.replace("%L", "%f")[
                    :19
                ]  # %f gives microseconds, we'll truncate

                result = date_val.strftime(python_fmt)

                # Handle milliseconds (%L)
                if "%L" in fmt:
                    ms = date_val.microsecond // 1000
                    result = result.replace(
                        str(date_val.microsecond)[:3].zfill(3), str(ms).zfill(3)
                    )

                return result
            case "$dateFromParts":
                # Handle MongoDB dict format: {year, month, day, hour, minute, second, millisecond, timezone}
                if not isinstance(operands, dict):
                    raise ValueError("$dateFromParts requires a dictionary")

                year = self._evaluate_operand_python(
                    operands.get("year"), document
                )
                month = (
                    self._evaluate_operand_python(
                        operands.get("month"), document
                    )
                    or 1
                )
                day = (
                    self._evaluate_operand_python(operands.get("day"), document)
                    or 1
                )
                hour = (
                    self._evaluate_operand_python(
                        operands.get("hour"), document
                    )
                    or 0
                )
                minute = (
                    self._evaluate_operand_python(
                        operands.get("minute"), document
                    )
                    or 0
                )
                second = (
                    self._evaluate_operand_python(
                        operands.get("second"), document
                    )
                    or 0
                )
                millisecond = (
                    self._evaluate_operand_python(
                        operands.get("millisecond"), document
                    )
                    or 0
                )
                timezone = operands.get("timezone")

                if year is None:
                    return None

                try:
                    dt = datetime(
                        year=int(year),
                        month=int(month),
                        day=int(day),
                        hour=int(hour),
                        minute=int(minute),
                        second=int(second),
                        microsecond=(
                            int(millisecond) * 1000 if millisecond else 0
                        ),
                    )
                    return dt
                except (ValueError, TypeError):
                    return None
            case "$dateToParts":
                # Handle MongoDB dict format: {date, timezone, unit}
                if isinstance(operands, dict):
                    date_operand = operands.get("date")
                    timezone = operands.get("timezone")
                    unit = operands.get("unit")
                    # Evaluate the date operand
                    date_val = self._evaluate_operand_python(
                        date_operand, document
                    )
                else:
                    if len(operands) < 1:
                        raise ValueError("$dateToParts requires date")
                    date_val = self._evaluate_operand_python(
                        operands[0], document
                    )
                    timezone = (
                        self._evaluate_operand_python(operands[1], document)
                        if len(operands) > 1
                        else None
                    )
                    unit = (
                        self._evaluate_operand_python(operands[2], document)
                        if len(operands) > 2
                        else None
                    )

                if date_val is None:
                    return None

                # Parse date
                if isinstance(date_val, str):
                    try:
                        date_val = datetime.fromisoformat(
                            date_val.replace("Z", "+00:00")
                        )
                    except ValueError:
                        return None

                if not isinstance(date_val, datetime):
                    return None

                # Build parts dictionary
                parts = {
                    "year": date_val.year,
                    "month": date_val.month,
                    "day": date_val.day,
                    "hour": date_val.hour,
                    "minute": date_val.minute,
                    "second": date_val.second,
                    "millisecond": date_val.microsecond // 1000,
                }

                # If unit is specified, only return parts up to that unit
                match unit:
                    case "year":
                        return {"year": parts["year"]}
                    case "month":
                        return {"year": parts["year"], "month": parts["month"]}
                    case "day":
                        return {
                            "year": parts["year"],
                            "month": parts["month"],
                            "day": parts["day"],
                        }
                    case "hour":
                        return {
                            k: v
                            for k, v in parts.items()
                            if k in ["year", "month", "day", "hour"]
                        }
                    case "minute":
                        return {
                            k: v
                            for k, v in parts.items()
                            if k in ["year", "month", "day", "hour", "minute"]
                        }
                    case "second":
                        return {
                            k: v
                            for k, v in parts.items()
                            if k
                            in [
                                "year",
                                "month",
                                "day",
                                "hour",
                                "minute",
                                "second",
                            ]
                        }
                    case _:
                        return parts
            case "$dateTrunc":
                # Handle MongoDB dict format: {date, unit, startOfWeek}
                if isinstance(operands, dict):
                    date_operand = operands.get("date")
                    unit = operands.get("unit", "day")
                    # Evaluate the date operand
                    date_val = self._evaluate_operand_python(
                        date_operand, document
                    )
                else:
                    if len(operands) < 2:
                        raise ValueError("$dateTrunc requires date and unit")
                    date_val = self._evaluate_operand_python(
                        operands[0], document
                    )
                    unit = self._evaluate_operand_python(operands[1], document)

                if date_val is None:
                    return None

                # Parse date
                if isinstance(date_val, str):
                    try:
                        date_val = datetime.fromisoformat(
                            date_val.replace("Z", "+00:00")
                        )
                    except ValueError:
                        return None

                if not isinstance(date_val, datetime):
                    return None

                # Truncate based on unit
                if unit == "year":
                    return date_val.replace(
                        month=1,
                        day=1,
                        hour=0,
                        minute=0,
                        second=0,
                        microsecond=0,
                    )
                elif unit == "quarter":
                    # Round down to start of quarter
                    quarter_month = ((date_val.month - 1) // 3) * 3 + 1
                    return date_val.replace(
                        month=quarter_month,
                        day=1,
                        hour=0,
                        minute=0,
                        second=0,
                        microsecond=0,
                    )
                elif unit == "month":
                    return date_val.replace(
                        day=1, hour=0, minute=0, second=0, microsecond=0
                    )
                elif unit == "week":
                    # Round down to start of week (Monday by default)
                    days_since_monday = date_val.weekday()
                    return (
                        date_val - timedelta(days=days_since_monday)
                    ).replace(hour=0, minute=0, second=0, microsecond=0)
                elif unit == "day":
                    return date_val.replace(
                        hour=0, minute=0, second=0, microsecond=0
                    )
                elif unit == "hour":
                    return date_val.replace(minute=0, second=0, microsecond=0)
                elif unit == "minute":
                    return date_val.replace(second=0, microsecond=0)
                elif unit == "second":
                    return date_val.replace(microsecond=0)
                else:
                    return date_val
            case _:
                raise NotImplementedError(
                    f"Date arithmetic operator {operator} not supported in Python evaluation"
                )
