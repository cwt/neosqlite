"""SQL converters for data size operators."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass


from .base import BaseSqlMixin


class DataSizeMixin(BaseSqlMixin):

    def _convert_data_size_operator(
        self, operator: str, operands: Any
    ) -> tuple[str, list[Any]]:
        """Convert $binarySize and $bsonSize operators to SQL."""
        if not isinstance(operands, list):
            operands = [operands]

        if len(operands) != 1:
            raise ValueError(f"{operator} requires exactly 1 operand")

        value_sql, value_params = self._convert_operand_to_sql(operands[0])

        if operator == "$binarySize":
            # In NeoSQLite, binary data is stored as base64 in a JSON object:
            # {"__neosqlite_binary__": true, "data": "...", "subtype": 0}
            # The 'data' field is base64 encoded.

            # Use 'json_extract' (not jsonb) to ensure we get a text string
            # if the value is extracted from a JSON document.

            # If value_sql is a field reference, it might be jsonb_extract.
            # We want the text version for base64 length calculation.
            text_value_sql = value_sql.replace("jsonb_extract", "json_extract")

            # Extract the base64 string if it's a binary object
            base64_data = f"CASE WHEN typeof({text_value_sql}) = 'text' AND json_extract({text_value_sql}, '$.__neosqlite_binary__') = 1 THEN json_extract({text_value_sql}, '$.data') ELSE {text_value_sql} END"

            # Simple base64 decoded length approximation: (len * 3 / 4)
            # We use CAST AS TEXT to ensure we don't have any JSONB weirdness
            return (
                f"((length(CAST({base64_data} AS TEXT)) * 3) / 4)",
                value_params,
            )

        else:  # $bsonSize
            # MongoDB $bsonSize returns the size of the document in BSON bytes.
            # In NeoSQLite, we return the size of the JSON representation.
            # Use json() to ensure we are measuring the serialized string size,
            # and octet_length/length to get the byte count.
            return f"length(json({value_sql}))", value_params
