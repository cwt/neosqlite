"""SQL converters for string operators."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass


from .base import BaseSqlMixin


class StringMixin(BaseSqlMixin):
    """$concat / $toLower / $toUpper / $substr / $trim / $regexMatch / $replaceAll → SQL."""

    def _build_pattern_with_options(self, regex: str, options: str) -> str:
        """Build regex pattern with inline flags."""
        if not options:
            return regex
        flag_str = ""
        for char in options.lower():
            if char in "imsx":
                flag_str += char
        return f"(?{flag_str}){regex}" if flag_str else regex

    def _convert_string_operator(
        self, operator: str, operands: list[Any]
    ) -> tuple[str, list[Any]]:
        """Convert string operators to SQL."""
        match operator:
            case "$concat":
                if len(operands) < 1:
                    raise ValueError("$concat requires at least 1 operand")
                sql_parts = []
                all_params = []
                for operand in operands:
                    operand_sql, operand_params = self._convert_operand_to_sql(
                        operand
                    )
                    sql_parts.append(operand_sql)
                    all_params.extend(operand_params)
                sql = f"({' || '.join(sql_parts)})"
                return sql, all_params
            case "$toLower":
                if len(operands) != 1:
                    raise ValueError("$toLower requires exactly 1 operand")
                value_sql, value_params = self._convert_operand_to_sql(
                    operands[0]
                )
                sql = f"lower({value_sql})"
                return sql, value_params
            case "$toUpper":
                if len(operands) != 1:
                    raise ValueError("$toUpper requires exactly 1 operand")
                value_sql, value_params = self._convert_operand_to_sql(
                    operands[0]
                )
                sql = f"upper({value_sql})"
                return sql, value_params
            case "$strLenBytes":
                if len(operands) != 1:
                    raise ValueError("$strLenBytes requires exactly 1 operand")
                value_sql, value_params = self._convert_operand_to_sql(
                    operands[0]
                )
                sql = f"length({value_sql})"
                return sql, value_params
            case "$substr":
                if len(operands) != 3:
                    raise ValueError("$substr requires exactly 3 operands")
                str_sql, str_params = self._convert_operand_to_sql(operands[0])
                start_sql, start_params = self._convert_operand_to_sql(
                    operands[1]
                )
                len_sql, len_params = self._convert_operand_to_sql(operands[2])
                sql = f"substr({str_sql}, {start_sql} + 1, {len_sql})"
                return sql, str_params + start_params + len_params
            case "$trim":
                if not isinstance(operands, dict) or "input" not in operands:
                    raise ValueError("$trim requires 'input' field")
                input_sql, input_params = self._convert_operand_to_sql(
                    operands["input"]
                )
                if "chars" in operands:
                    chars_sql, chars_params = self._convert_operand_to_sql(
                        operands["chars"]
                    )
                    sql = f"trim({input_sql}, {chars_sql})"
                    return sql, input_params + chars_params
                else:
                    sql = f"trim({input_sql})"
                    return sql, input_params
            case "$ltrim":
                if not isinstance(operands, dict) or "input" not in operands:
                    raise ValueError("$ltrim requires 'input' field")
                input_sql, input_params = self._convert_operand_to_sql(
                    operands["input"]
                )
                if "chars" in operands:
                    chars_sql, chars_params = self._convert_operand_to_sql(
                        operands["chars"]
                    )
                    sql = f"ltrim({input_sql}, {chars_sql})"
                    return sql, input_params + chars_params
                else:
                    sql = f"ltrim({input_sql})"
                    return sql, input_params
            case "$rtrim":
                if not isinstance(operands, dict) or "input" not in operands:
                    raise ValueError("$rtrim requires 'input' field")
                input_sql, input_params = self._convert_operand_to_sql(
                    operands["input"]
                )
                if "chars" in operands:
                    chars_sql, chars_params = self._convert_operand_to_sql(
                        operands["chars"]
                    )
                    sql = f"rtrim({input_sql}, {chars_sql})"
                    return sql, input_params + chars_params
                else:
                    sql = f"rtrim({input_sql})"
                    return sql, input_params
            case "$indexOfBytes":
                if len(operands) < 2:
                    raise ValueError(
                        "$indexOfBytes requires string and substring"
                    )
                string_sql, string_params = self._convert_operand_to_sql(
                    operands[0]
                )
                substr_sql, substr_params = self._convert_operand_to_sql(
                    operands[1]
                )
                sql = f"(instr({string_sql}, {substr_sql}) - 1)"
                return sql, string_params + substr_params
            case "$strcasecmp":
                # Case-insensitive string comparison using SQLite's COLLATE NOCASE
                if len(operands) != 2:
                    raise ValueError("$strcasecmp requires exactly 2 operands")
                str1_sql, str1_params = self._convert_operand_to_sql(
                    operands[0]
                )
                str2_sql, str2_params = self._convert_operand_to_sql(
                    operands[1]
                )
                # Use CASE expression to return -1, 0, or 1
                sql = f"""
                    CASE
                        WHEN {str1_sql} COLLATE NOCASE < {str2_sql} COLLATE NOCASE THEN -1
                        WHEN {str1_sql} COLLATE NOCASE > {str2_sql} COLLATE NOCASE THEN 1
                        ELSE 0
                    END
                """
                return sql, str1_params + str2_params
            case "$substrBytes":
                # Substring by bytes - SQLite's substr works on characters, not bytes
                # For ASCII this is the same, for UTF-8 we need special handling
                if len(operands) != 3:
                    raise ValueError("$substrBytes requires exactly 3 operands")
                str_sql, str_params = self._convert_operand_to_sql(operands[0])
                start_sql, start_params = self._convert_operand_to_sql(
                    operands[1]
                )
                len_sql, len_params = self._convert_operand_to_sql(operands[2])
                # Use substr - note this works on characters in SQLite
                # For true byte-level operations, would need hex/unescape
                sql = f"substr({str_sql}, {start_sql} + 1, {len_sql})"
                return sql, str_params + start_params + len_params
            case "$regexMatch":
                # $regexMatch format: {input, regex, options?}
                if not isinstance(operands, dict) or "input" not in operands:
                    raise ValueError("$regexMatch requires 'input' and 'regex'")

                input_sql, input_params = self._convert_operand_to_sql(
                    operands["input"]
                )
                regex = operands.get("regex", "")
                options = operands.get("options", "")
                pattern = self._build_pattern_with_options(regex, options)

                sql = f"CASE WHEN {input_sql} REGEXP ? THEN json('true') ELSE json('false') END"
                return sql, input_params + [pattern]
            case "$regexFind":
                # $regexFind format: {input, regex, options?}
                if not isinstance(operands, dict) or "input" not in operands:
                    raise ValueError("$regexFind requires 'input' and 'regex'")

                input_sql, input_params = self._convert_operand_to_sql(
                    operands["input"]
                )
                regex = operands.get("regex", "")
                options = operands.get("options", "")
                pattern = self._build_pattern_with_options(regex, options)

                sql = f"json(REGEXP_FIND(?, {input_sql}))"
                return sql, input_params + [pattern]
            case "$regexFindAll":
                # $regexFindAll format: {input, regex, options?}
                if not isinstance(operands, dict) or "input" not in operands:
                    raise ValueError(
                        "$regexFindAll requires 'input' and 'regex'"
                    )

                input_sql, input_params = self._convert_operand_to_sql(
                    operands["input"]
                )
                regex = operands.get("regex", "")
                options = operands.get("options", "")
                pattern = self._build_pattern_with_options(regex, options)

                sql = f"json(REGEXP_FIND_ALL(?, {input_sql}))"
                return sql, input_params + [pattern]
            case "$split":
                # Recursive CTE duplicates ? placeholders causing param mismatch.
                raise NotImplementedError(
                    "Operator $split not supported in SQL tier"
                )
            case "$replaceAll":
                # Handle MongoDB dict format: {input, find, replacement}
                if isinstance(operands, dict):
                    string_operand = operands.get("input")
                    find_operand = operands.get("find")
                    replace_operand = operands.get("replacement")
                else:
                    # Handle list format
                    if len(operands) != 3:
                        raise ValueError(
                            "$replaceAll requires string, find, and replacement"
                        )
                    string_operand = operands[0]
                    find_operand = operands[1]
                    replace_operand = operands[2]

                string_sql, string_params = self._convert_operand_to_sql(
                    string_operand
                )

                # Check if it's a regex replace (MongoDB 4.4+)
                # MongoDB doesn't natively support regex in $replaceAll (it uses $replaceOne/$replaceAll for strings)
                # but we can support it if the find operand is a regex expression
                if isinstance(find_operand, dict) and "$regex" in find_operand:
                    regex = find_operand["$regex"]
                    options = find_operand.get("$options", "")
                    pattern = self._build_pattern_with_options(regex, options)

                    replace_sql, replace_params = self._convert_operand_to_sql(
                        replace_operand
                    )
                    # count=0 for replaceAll
                    sql = f"REGEXP_REPLACE({string_sql}, ?, {replace_sql}, 0)"
                    return sql, string_params + [pattern] + replace_params

                find_sql, find_params = self._convert_operand_to_sql(
                    find_operand
                )
                replace_sql, replace_params = self._convert_operand_to_sql(
                    replace_operand
                )
                sql = f"replace({string_sql}, {find_sql}, {replace_sql})"
                return sql, string_params + find_params + replace_params
            case "$replaceOne":
                # Handle MongoDB dict format: {input, find, replacement}
                if isinstance(operands, dict):
                    string_operand = operands.get("input")
                    find_operand = operands.get("find")
                    replace_operand = operands.get("replacement")
                else:
                    if len(operands) != 3:
                        raise ValueError(
                            "$replaceOne requires string, find, and replacement"
                        )
                    string_operand = operands[0]
                    find_operand = operands[1]
                    replace_operand = operands[2]

                string_sql, string_params = self._convert_operand_to_sql(
                    string_operand
                )

                # Check for regex replace
                if isinstance(find_operand, dict) and "$regex" in find_operand:
                    regex = find_operand["$regex"]
                    options = find_operand.get("$options", "")
                    pattern = self._build_pattern_with_options(regex, options)

                    replace_sql, replace_params = self._convert_operand_to_sql(
                        replace_operand
                    )
                    # count=1 for replaceOne
                    sql = f"REGEXP_REPLACE({string_sql}, ?, {replace_sql}, 1)"
                    return sql, string_params + [pattern] + replace_params

                find_sql, find_params = self._convert_operand_to_sql(
                    find_operand
                )
                replace_sql, replace_params = self._convert_operand_to_sql(
                    replace_operand
                )
                # Use instr() and substr() to replace only first occurrence
                # Note: string_sql and find_sql are used multiple times, so we
                # need to duplicate params for each occurrence
                sql = (
                    f"CASE WHEN instr({string_sql}, {find_sql}) > 0 THEN "
                    f"substr({string_sql}, 1, instr({string_sql}, {find_sql}) - 1) || "
                    f"{replace_sql} || "
                    f"substr({string_sql}, instr({string_sql}, {find_sql}) + length({find_sql})) "
                    f"ELSE {string_sql} END"
                )
                # Duplicate params to match SQL order:
                # 1. instr(string, find) - string_params + find_params
                # 2. instr(string, find) - string_params + find_params
                # 3. replace - replace_params
                # 4. instr(string, find) - string_params + find_params
                # 5. length(find) - find_params
                all_params = (
                    string_params
                    + find_params  # 1st instr
                    + string_params
                    + find_params  # 2nd instr
                    + replace_params  # replacement
                    + string_params
                    + find_params  # 3rd instr
                    + find_params  # length
                )
                return sql, all_params
            case "$strLenCP":
                # Normalize operands
                if not isinstance(operands, list):
                    operands = [operands]
                if len(operands) != 1:
                    raise ValueError("$strLenCP requires exactly 1 operand")
                string_sql, string_params = self._convert_operand_to_sql(
                    operands[0]
                )
                # For BMP characters, length in bytes = length in code points
                sql = f"length({string_sql})"
                return sql, string_params
            case "$indexOfCP":
                if len(operands) < 2:
                    raise ValueError("$indexOfCP requires string and substring")
                string_sql, string_params = self._convert_operand_to_sql(
                    operands[0]
                )
                substr_sql, substr_params = self._convert_operand_to_sql(
                    operands[1]
                )
                # SQLite instr(haystack, needle) returns 1-based index, convert to 0-based
                # Note: The haystack comes first, needle second (opposite of MongoDB's order)
                sql = f"instr({string_sql}, {substr_sql}) - 1"
                return sql, string_params + substr_params
            case _:
                raise NotImplementedError(
                    f"String operator {operator} not supported in SQL tier"
                )
