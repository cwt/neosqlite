"""String and regex Python evaluators."""

from __future__ import annotations

import re
from typing import Any

from .base import BasePythonMixin


class StringPythonMixin(BasePythonMixin):
    """String and regex operators."""

    def _evaluate_string_python(
        self, operator: str, operands: Any, document: dict[str, Any]
    ) -> Any:
        """Evaluate string operators in Python.

        Args:
            operator: The string operator ($toUpper, $toLower, etc.)
            operands: The operand(s). Can be:
                      - A single value for simple cases like {"$toUpper": "$field"}
                      - A list of values for array format
                      - A dict for operators like $trim, $regexMatch
            document: The document to evaluate against
        """
        # Normalize operands to handle both single values and lists
        # MongoDB allows both: {$toUpper: "$field"} and {$toUpper: ["$field"]}
        # But some operators like $trim, $regexMatch, $replaceAll use dict format
        if operator in (
            "$trim",
            "$ltrim",
            "$rtrim",
            "$regexMatch",
            "$regexFind",
            "$regexFindAll",
            "$replaceAll",
            "$replaceOne",
        ):
            # These operators use dict format, don't normalize
            pass
        elif not isinstance(operands, list):
            operands = [operands]

        match operator:
            case "$concat":
                values = [
                    self._evaluate_operand_python(op, document)
                    for op in operands
                ]
                # MongoDB: if any argument resolves to null, return null.
                if any(v is None for v in values):
                    return None
                return "".join(str(v) for v in values)
            case "$toLower":
                if len(operands) != 1:
                    raise ValueError("$toLower requires exactly 1 operand")
                value = self._evaluate_operand_python(operands[0], document)
                return str(value).lower() if value is not None else None
            case "$toUpper":
                if len(operands) != 1:
                    raise ValueError("$toUpper requires exactly 1 operand")
                value = self._evaluate_operand_python(operands[0], document)
                return str(value).upper() if value is not None else None
            case "$strLenBytes":
                if len(operands) != 1:
                    raise ValueError("$strLenBytes requires exactly 1 operand")
                value = self._evaluate_operand_python(operands[0], document)
                return (
                    len(str(value).encode("utf-8"))
                    if value is not None
                    else None
                )
            case "$substr":
                if len(operands) != 3:
                    raise ValueError("$substr requires exactly 3 operands")
                string = self._evaluate_operand_python(operands[0], document)
                start = self._evaluate_operand_python(operands[1], document)
                length = self._evaluate_operand_python(operands[2], document)
                if (
                    string is not None
                    and start is not None
                    and length is not None
                ):
                    return str(string)[int(start) : int(start) + int(length)]
                return None
            case "$trim":
                if not isinstance(operands, dict) or "input" not in operands:
                    raise ValueError("$trim requires 'input' field")
                input_val = self._evaluate_operand_python(
                    operands["input"], document
                )
                if input_val is None:
                    return None
                chars = operands.get("chars")
                if chars is not None:
                    chars_val = self._evaluate_operand_python(chars, document)
                    if chars_val is not None:
                        return str(input_val).strip(str(chars_val))
                return str(input_val).strip()
            case "$ltrim":
                if not isinstance(operands, dict) or "input" not in operands:
                    raise ValueError("$ltrim requires 'input' field")
                input_val = self._evaluate_operand_python(
                    operands["input"], document
                )
                if input_val is None:
                    return None
                chars = operands.get("chars")
                if chars is not None:
                    chars_val = self._evaluate_operand_python(chars, document)
                    if chars_val is not None:
                        return str(input_val).lstrip(str(chars_val))
                return str(input_val).lstrip()
            case "$rtrim":
                if not isinstance(operands, dict) or "input" not in operands:
                    raise ValueError("$rtrim requires 'input' field")
                input_val = self._evaluate_operand_python(
                    operands["input"], document
                )
                if input_val is None:
                    return None
                chars = operands.get("chars")
                if chars is not None:
                    chars_val = self._evaluate_operand_python(chars, document)
                    if chars_val is not None:
                        return str(input_val).rstrip(str(chars_val))
                return str(input_val).rstrip()
            case "$indexOfBytes":
                if len(operands) < 2:
                    raise ValueError(
                        "$indexOfBytes requires string and substring"
                    )
                string = self._evaluate_operand_python(operands[0], document)
                substr = self._evaluate_operand_python(operands[1], document)
                if substr is None or string is None:
                    return -1
                idx = str(string).find(str(substr))
                return idx
            case "$regexMatch":
                if not isinstance(operands, dict) or "input" not in operands:
                    raise ValueError("$regexMatch requires 'input' and 'regex'")
                input_val = self._evaluate_operand_python(
                    operands["input"], document
                )
                regex = operands.get("regex", "")
                options = operands.get("options", "")
                if input_val is None:
                    return False

                flags = 0
                if "i" in options.lower():
                    flags |= re.IGNORECASE
                if "m" in options.lower():
                    flags |= re.MULTILINE
                if "s" in options.lower():
                    flags |= re.DOTALL
                if "x" in options.lower():
                    flags |= re.VERBOSE

                return bool(re.search(regex, str(input_val), flags))
            case "$split":
                if len(operands) != 2:
                    raise ValueError("$split requires string and delimiter")
                string = self._evaluate_operand_python(operands[0], document)
                delimiter = self._evaluate_operand_python(operands[1], document)
                if string is None or delimiter is None:
                    return []
                return str(string).split(str(delimiter))
            case "$replaceAll":
                # Handle MongoDB dict format: {input, find, replacement}
                if isinstance(operands, dict):
                    string = self._evaluate_operand_python(
                        operands.get("input"), document
                    )
                    find = self._evaluate_operand_python(
                        operands.get("find"), document
                    )
                    replacement = self._evaluate_operand_python(
                        operands.get("replacement"), document
                    )
                else:
                    # Handle list format
                    if len(operands) != 3:
                        raise ValueError(
                            "$replaceAll requires string, find, and replacement"
                        )
                    string = self._evaluate_operand_python(
                        operands[0], document
                    )
                    find = self._evaluate_operand_python(operands[1], document)
                    replacement = self._evaluate_operand_python(
                        operands[2], document
                    )
                if string is None:
                    return None
                return str(string).replace(str(find), str(replacement))
            case "$replaceOne":
                # Handle MongoDB dict format: {input, find, replacement}
                if isinstance(operands, dict):
                    string = self._evaluate_operand_python(
                        operands.get("input"), document
                    )
                    find = self._evaluate_operand_python(
                        operands.get("find"), document
                    )
                    replacement = self._evaluate_operand_python(
                        operands.get("replacement"), document
                    )
                else:
                    if len(operands) != 3:
                        raise ValueError(
                            "$replaceOne requires string, find, and replacement"
                        )
                    string = self._evaluate_operand_python(
                        operands[0], document
                    )
                    find = self._evaluate_operand_python(operands[1], document)
                    replacement = self._evaluate_operand_python(
                        operands[2], document
                    )
                if string is None:
                    return None
                # Replace only first occurrence
                return str(string).replace(str(find), str(replacement), 1)
            case "$strLenCP":
                # String length in code points (Unicode characters)
                # Handle both list and single operand formats
                if not isinstance(operands, list):
                    operands = [operands]
                if len(operands) != 1:
                    raise ValueError("$strLenCP requires exactly 1 operand")
                value = self._evaluate_operand_python(operands[0], document)
                if value is None:
                    return None
                return len(str(value))
            case "$substrCP":
                # Substring by code points (not implemented - use $substr)
                # Handle both list and single operand formats
                if not isinstance(operands, list):
                    operands = [operands]
                if len(operands) != 3:
                    raise ValueError("$substrCP requires exactly 3 operands")
                string = self._evaluate_operand_python(operands[0], document)
                start = self._evaluate_operand_python(operands[1], document)
                length = self._evaluate_operand_python(operands[2], document)
                if (
                    string is not None
                    and start is not None
                    and length is not None
                ):
                    # For BMP characters, this is the same as $substr
                    # For full Unicode support, would need proper code point handling
                    return str(string)[int(start) : int(start) + int(length)]
                return None
            case "$indexOfCP":
                # Find substring by code points
                if len(operands) < 2:
                    raise ValueError("$indexOfCP requires string and substring")
                string = self._evaluate_operand_python(operands[0], document)
                substr = self._evaluate_operand_python(operands[1], document)
                if substr is None or string is None:
                    return -1
                idx = str(string).find(str(substr))
                return idx
            case "$strcasecmp":
                # Case-insensitive string comparison
                if len(operands) != 2:
                    raise ValueError("$strcasecmp requires exactly 2 operands")
                str1 = self._evaluate_operand_python(operands[0], document)
                str2 = self._evaluate_operand_python(operands[1], document)
                if str1 is None or str2 is None:
                    return None
                # Return -1, 0, or 1 like MongoDB
                s1 = str(str1).lower()
                s2 = str(str2).lower()
                if s1 < s2:
                    return -1
                elif s1 > s2:
                    return 1
                else:
                    return 0
            case "$substrBytes":
                # Substring by bytes (for UTF-8 encoded strings)
                if len(operands) != 3:
                    raise ValueError("$substrBytes requires exactly 3 operands")
                string = self._evaluate_operand_python(operands[0], document)
                start = self._evaluate_operand_python(operands[1], document)
                length = self._evaluate_operand_python(operands[2], document)
                if string is None or start is None or length is None:
                    return None
                # Encode to UTF-8, slice by bytes, decode back
                encoded = str(string).encode("utf-8")
                sliced = encoded[int(start) : int(start) + int(length)]
                try:
                    return sliced.decode("utf-8")
                except UnicodeDecodeError:
                    # If we cut in the middle of a multi-byte character, return what we can
                    return sliced.decode("utf-8", errors="ignore")
            case "$regexFind":
                if not isinstance(operands, dict) or "input" not in operands:
                    raise ValueError("$regexFind requires 'input' and 'regex'")
                input_val = self._evaluate_operand_python(
                    operands["input"], document
                )
                regex = operands.get("regex", "")
                options = operands.get("options", "")
                if input_val is None:
                    return None

                flags = 0
                if "i" in options.lower():
                    flags |= re.IGNORECASE
                if "m" in options.lower():
                    flags |= re.MULTILINE
                if "s" in options.lower():
                    flags |= re.DOTALL
                if "x" in options.lower():
                    flags |= re.VERBOSE

                match_result = re.search(regex, str(input_val), flags)
                if match_result:
                    result = {
                        "match": match_result.group(),
                        "idx": match_result.start(),
                        "captures": (
                            list(match_result.groups())
                            if match_result.groups()
                            else []
                        ),
                    }
                    return result
                return None
            case "$regexFindAll":
                if not isinstance(operands, dict) or "input" not in operands:
                    raise ValueError(
                        "$regexFindAll requires 'input' and 'regex'"
                    )
                input_val = self._evaluate_operand_python(
                    operands["input"], document
                )
                regex = operands.get("regex", "")
                options = operands.get("options", "")
                if input_val is None:
                    return []

                flags = 0
                if "i" in options.lower():
                    flags |= re.IGNORECASE
                if "m" in options.lower():
                    flags |= re.MULTILINE
                if "s" in options.lower():
                    flags |= re.DOTALL
                if "x" in options.lower():
                    flags |= re.VERBOSE

                matches = list(re.finditer(regex, str(input_val), flags))
                all_results: list[dict[str, Any]] = []
                for match_result in matches:
                    match_obj: dict[str, Any] = {
                        "match": match_result.group(),
                        "idx": match_result.start(),
                        "captures": (
                            list(match_result.groups())
                            if match_result.groups()
                            else []
                        ),
                    }
                    all_results.append(match_obj)
                return all_results
            case _:
                raise NotImplementedError(
                    f"String operator {operator} not supported in Python evaluation"
                )
