"""
Type conversion utilities for expression evaluation.

This module re-exports type conversion functions from the shared
collection.type_utils module for backward compatibility.

All type conversion functions are now defined in collection.type_utils
to avoid code duplication across subpackages.
"""

from __future__ import annotations

from ..type_utils import (
    _convert_to_bindata as _convert_to_bindata,
)
from ..type_utils import (
    _convert_to_bool as _convert_to_bool,
)
from ..type_utils import (
    _convert_to_bsonbindata as _convert_to_bsonbindata,
)
from ..type_utils import (
    _convert_to_bsonregex as _convert_to_bsonregex,
)
from ..type_utils import (
    _convert_to_date as _convert_to_date,
)
from ..type_utils import (
    _convert_to_decimal as _convert_to_decimal,
)
from ..type_utils import (
    _convert_to_double as _convert_to_double,
)

# Re-export all type conversion functions from shared module
from ..type_utils import (
    _convert_to_int as _convert_to_int,
)
from ..type_utils import (
    _convert_to_long as _convert_to_long,
)
from ..type_utils import (
    _convert_to_null as _convert_to_null,
)
from ..type_utils import (
    _convert_to_objectid as _convert_to_objectid,
)
from ..type_utils import (
    _convert_to_regex as _convert_to_regex,
)
from ..type_utils import (
    _convert_to_string as _convert_to_string,
)
from ..type_utils import (
    get_bson_type as get_bson_type,
)

__all__ = [
    "_convert_to_int",
    "_convert_to_long",
    "_convert_to_double",
    "_convert_to_decimal",
    "_convert_to_string",
    "_convert_to_bool",
    "_convert_to_objectid",
    "_convert_to_bindata",
    "_convert_to_bsonbindata",
    "_convert_to_regex",
    "_convert_to_bsonregex",
    "_convert_to_date",
    "_convert_to_null",
    "get_bson_type",
]
