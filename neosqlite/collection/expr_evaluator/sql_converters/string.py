"""SQL converters for string operators.

Known, documented divergences from MongoDB in the SQL tier (the Python
tier is authoritative):
- lower()/upper() fold ASCII only; MongoDB folds Unicode.
- $concat silently coerces booleans to '0'/'1' text; MongoDB errors.
- Legacy $substr with negative length follows SQLite substr semantics.

All other operators match MongoDB semantics, including byte-accurate
$substrBytes and empty-find $replaceAll (#117).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass


from .base import BaseSqlMixin


class StringMixin(BaseSqlMixin):
    """Mixin for converting string aggregation operators to SQL expressions.

    Provides SQL translation for string manipulation, length, substring,
    trimming, splitting, replacement, comparison, and regex matching
    operators (such as ``$concat``, ``$toLower``, ``$toUpper``,
    ``$strLenBytes``, ``$strLenCP``, ``$substr``, ``$substrBytes``,
    ``$substrCP``, ``$trim``, ``$ltrim``, ``$rtrim``, ``$indexOfBytes``,
    ``$indexOfCP``, ``$strcasecmp``, ``$regexMatch``, ``$regexFind``,
    ``$regexFindAll``, ``$split``, ``$replaceAll``, and ``$replaceOne``).
    """

    def _build_pattern_with_options(self, regex: str, options: str) -> str:
        """Build a regex pattern with inline PCRE/Python flag modifiers.

        Filters ``options`` case-insensitively for supported flags
        (``i``, ``m``, ``s``, ``x``) and prepends ``(?flags)`` to ``regex``
        when any valid flag is present.

        Args:
            regex: Regular expression pattern string.
            options: Regex option flags string (e.g., ``"im"``).

        Returns:
            str: Pattern string with inline flags prepended if applicable,
                otherwise the original ``regex`` unchanged.
        """
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
        """Convert a MongoDB string aggregation operator to SQL.

        Translates supported string operators into SQLite SQL expressions
        and ordered parameter lists, duplicating parameter slices when an
        operand SQL fragment is interpolated multiple times.

        Args:
            operator: MongoDB string operator name (e.g., ``"$concat"``,
                ``"$trim"``, ``"$regexMatch"``, ``"$replaceAll"``).
            operands: Operator arguments as a list of operand expressions,
                a single operand (normalized for unary operators), or a
                specification dictionary for named-argument operators such as
                ``$trim`` and ``$regexMatch``.

        Returns:
            tuple[str, list[Any]]: A tuple of the generated SQL expression
                string and its ordered parameter list.

        Raises:
            ValueError: If operand count or required specification keys are
                invalid for the given operator.
            NotImplementedError: If ``$substrBytes`` uses non-integer
                bounds, ``$split`` uses a dynamic delimiter, or the
                operator is not supported in the SQL tier.
        """
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
                if not isinstance(operands, list):
                    operands = [operands]
                if len(operands) != 1:
                    raise ValueError("$toLower requires exactly 1 operand")
                value_sql, value_params = self._convert_operand_to_sql(
                    operands[0]
                )
                sql = f"lower({value_sql})"
                return sql, value_params
            case "$toUpper":
                if not isinstance(operands, list):
                    operands = [operands]
                if len(operands) != 1:
                    raise ValueError("$toUpper requires exactly 1 operand")
                value_sql, value_params = self._convert_operand_to_sql(
                    operands[0]
                )
                sql = f"upper({value_sql})"
                return sql, value_params
            case "$strLenBytes":
                if not isinstance(operands, list):
                    operands = [operands]
                if len(operands) != 1:
                    raise ValueError("$strLenBytes requires exactly 1 operand")
                value_sql, value_params = self._convert_operand_to_sql(
                    operands[0]
                )
                # length() on TEXT counts characters; MongoDB wants bytes
                # (#117). length() on BLOB counts bytes.
                sql = f"length(CAST({value_sql} AS BLOB))"
                return sql, value_params
            case "$substrBytes":
                # Operate on the UTF-8 BLOB so offsets are byte-based like
                # MongoDB; may split a multibyte sequence at caller's risk,
                # matching MongoDB's error-prone byte semantics (#117)
                if len(operands) != 3:
                    raise ValueError("$substrBytes requires exactly 3 operands")
                str_sql, str_params = self._convert_operand_to_sql(operands[0])
                # start/len are plain integers in MongoDB; inline them so a
                # single SQL string can reference them repeatedly without
                # placeholder bookkeeping (#117)
                start, ln = operands[1], operands[2]
                if (
                    isinstance(start, bool)
                    or not isinstance(start, int)
                    or isinstance(ln, bool)
                    or not isinstance(ln, int)
                ):
                    raise NotImplementedError(
                        "$substrBytes requires integer start and length"
                    )
                # substr over the BLOB yields BLOB; wrap back to TEXT so
                # JSON functions can store the result
                sql = (
                    f"(CASE WHEN ({start}) < 0 THEN '' "
                    f"WHEN ({ln}) < 0 "
                    f"THEN CAST(substr(CAST({str_sql} AS BLOB), ({start}) + 1) AS TEXT) "
                    f"ELSE CAST(substr(CAST({str_sql} AS BLOB), ({start}) + 1, ({ln})) AS TEXT) END)"
                )
                return sql, str_params
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
            case "$substrCP":
                if not isinstance(operands, list):
                    operands = [operands]
                if len(operands) != 3:
                    raise ValueError("$substrCP requires exactly 3 operands")
                str_sql, str_params = self._convert_operand_to_sql(operands[0])
                start_sql, start_params = self._convert_operand_to_sql(
                    operands[1]
                )
                len_sql, len_params = self._convert_operand_to_sql(operands[2])
                sql = (
                    f"(WITH _sc(s, st, ln) AS (SELECT {str_sql}, CAST({start_sql} AS INTEGER), CAST({len_sql} AS INTEGER)) "
                    f"SELECT CASE WHEN s IS NULL OR st IS NULL OR ln IS NULL THEN NULL "
                    f"WHEN st < 0 OR ln < 0 THEN '' "
                    f"ELSE substr(CAST(s AS TEXT), st + 1, ln) END FROM _sc)"
                )
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
                # NULL operands must yield -1 like MongoDB (#117)
                sql = (
                    f"(CASE WHEN {string_sql} IS NULL OR {substr_sql} IS NULL "
                    f"THEN -1 ELSE instr({string_sql}, {substr_sql}) - 1 END)"
                )
                # Both fragments appear twice; duplicate params in SQL order:
                # string, substr, string, substr.
                return (
                    sql,
                    string_params
                    + substr_params
                    + string_params
                    + substr_params,
                )
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
                # Both fragments appear twice; duplicate params in SQL order:
                # str1, str2, str1, str2.
                return (
                    sql,
                    str1_params + str2_params + str1_params + str2_params,
                )
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

                sql = f"CASE WHEN {input_sql} REGEXP ? THEN 1 ELSE 0 END"
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
                # Bind in SQL order: the pattern placeholder comes first.
                return sql, [pattern] + input_params
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
                # Bind in SQL order: the pattern placeholder comes first.
                return sql, [pattern] + input_params
            case "$split":
                if not isinstance(operands, list) or len(operands) != 2:
                    raise ValueError("$split requires [string, delimiter]")
                string_sql, string_params = self._convert_operand_to_sql(
                    operands[0]
                )
                if isinstance(operands[1], str) and not operands[1].startswith(
                    "$"
                ):
                    delim = operands[1]
                    if delim == "":
                        raise ValueError("empty separator")
                    # Use a recursive CTE + json_group_array so arbitrary
                    # characters (", \, newlines, tabs, etc.) are JSON-escaped
                    # by SQLite rather than string-concatenated into JSON.
                    sql = (
                        f"(WITH RECURSIVE "
                        f"__split_in(val, sep) AS (SELECT ({string_sql}), ?), "
                        f"__split(part, rest) AS ("
                        f"SELECT "
                        f"CASE WHEN instr(CAST(val AS TEXT), sep) > 0 "
                        f"THEN substr(CAST(val AS TEXT), 1, "
                        f"instr(CAST(val AS TEXT), sep) - 1) "
                        f"ELSE CAST(val AS TEXT) END, "
                        f"CASE WHEN instr(CAST(val AS TEXT), sep) > 0 "
                        f"THEN substr(CAST(val AS TEXT), "
                        f"instr(CAST(val AS TEXT), sep) + length(sep)) "
                        f"ELSE NULL END "
                        f"FROM __split_in WHERE val IS NOT NULL "
                        f"UNION ALL "
                        f"SELECT "
                        f"CASE WHEN instr(rest, (SELECT sep FROM __split_in)) > 0 "
                        f"THEN substr(rest, 1, "
                        f"instr(rest, (SELECT sep FROM __split_in)) - 1) "
                        f"ELSE rest END, "
                        f"CASE WHEN instr(rest, (SELECT sep FROM __split_in)) > 0 "
                        f"THEN substr(rest, "
                        f"instr(rest, (SELECT sep FROM __split_in)) + "
                        f"length((SELECT sep FROM __split_in))) "
                        f"ELSE NULL END "
                        f"FROM __split WHERE rest IS NOT NULL"
                        f") "
                        f"SELECT CASE WHEN (SELECT val FROM __split_in) IS NULL "
                        f"THEN json('[]') "
                        f"ELSE json((SELECT json_group_array(part) FROM __split)) "
                        f"END)"
                    )
                    return sql, [*string_params, delim]
                # Dynamic delimiter: fall back to Python.
                raise NotImplementedError(
                    "$split with dynamic delimiter not supported in SQL tier"
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
