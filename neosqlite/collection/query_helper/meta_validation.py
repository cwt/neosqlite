"""MongoDB-compatible validation and normalization of ``$meta`` operators.

MongoDB only supplies text score metadata when a ``$text`` match has run in the
current pipeline scope. Without it, ``$meta: "textScore"`` is an error rather
than a zero value. ``$meta: "indexKey"`` is a no-op: in ``$addFields`` /
``$project`` the field is omitted entirely, while nested inside a larger
expression it evaluates to ``null``. Unknown ``$meta`` fields raise an error.

Doing this once before tier dispatch keeps SQL, temporary-table, and Python
execution consistent, regardless of which tier ends up running the pipeline.
"""

from __future__ import annotations

from typing import Any

from ...exceptions import MalformedQueryException

_TEXT_SCORE_ERROR = (
    "query requires text score metadata, but it is not available"
)
_INDEX_KEY_NULL = {"$literal": None}
_FIELD_SETTING_STAGES = {"$addFields", "$project", "$set"}


def _meta_field(value: Any) -> str | None:
    """Return the field name of a single-key ``{"$meta": ...}`` node."""
    if isinstance(value, dict) and len(value) == 1 and "$meta" in value:
        field = value["$meta"]
        return field if isinstance(field, str) else None
    return None


def _validate_meta_field(field: str, has_text_score: bool) -> None:
    """Raise when a ``$meta`` field is invalid in the current scope."""
    if field == "textScore":
        if not has_text_score:
            raise MalformedQueryException(_TEXT_SCORE_ERROR)
    elif field != "indexKey":
        raise MalformedQueryException(f"Unsupported $meta field: {field}")


def _normalize_value(
    value: Any, has_text_score: bool, rewrite_text_score: bool = False
) -> tuple[Any, bool]:
    """Validate ``$meta`` nodes inside an arbitrary expression value.

    Returns the (possibly rewritten) value and whether it changed. Nested
    ``$meta: "indexKey"`` nodes become ``{"$literal": None}`` so they evaluate
    to ``null`` like MongoDB.

    ``rewrite_text_score`` is used inside ``$facet`` sub-pipelines, which
    NeoSQLite executes as separate aggregations. Reading the persisted
    ``_textScore`` field keeps their score available without re-running the
    outer ``$text`` match.
    """
    field = _meta_field(value)
    if field is not None:
        _validate_meta_field(field, has_text_score)
        if field == "indexKey":
            return _INDEX_KEY_NULL, True
        if field == "textScore" and rewrite_text_score:
            return "$_textScore", True
        return value, False

    if isinstance(value, dict):
        changed = False
        result: dict[str, Any] = {}
        for key, item in value.items():
            new_item, item_changed = _normalize_value(
                item, has_text_score, rewrite_text_score
            )
            result[key] = new_item
            changed = changed or item_changed
        return (result if changed else value), changed

    if isinstance(value, list):
        changed = False
        result_list: list[Any] = []
        for item in value:
            new_item, item_changed = _normalize_value(
                item, has_text_score, rewrite_text_score
            )
            result_list.append(new_item)
            changed = changed or item_changed
        return (result_list if changed else value), changed

    return value, False


def _normalize_stages(
    stages: list[Any],
    has_text_score: bool,
    rewrite_text_score: bool = False,
) -> tuple[list[Any], bool]:
    """Validate and normalize one pipeline (or sub-pipeline) scope.

    ``$facet`` sub-pipelines inherit the caller's text score metadata, while
    ``$lookup`` sub-pipelines run against another collection and start fresh.
    """
    changed = False
    result: list[Any] = []
    scope_has_text_score = has_text_score

    for stage in stages:
        if not isinstance(stage, dict) or not stage:
            result.append(stage)
            continue

        stage_name = next(iter(stage))
        spec = stage[stage_name]

        if stage_name in _FIELD_SETTING_STAGES and isinstance(spec, dict):
            new_spec: dict[str, Any] = {}
            for field, value in spec.items():
                meta_field = _meta_field(value)
                if meta_field is not None:
                    _validate_meta_field(meta_field, scope_has_text_score)
                    if meta_field == "indexKey":
                        changed = True
                        continue
                new_value, value_changed = _normalize_value(
                    value, scope_has_text_score, rewrite_text_score
                )
                new_spec[field] = new_value
                changed = changed or value_changed
            result.append({stage_name: new_spec})

        elif stage_name == "$facet" and isinstance(spec, dict):
            new_spec = {}
            for facet_name, sub_pipeline in spec.items():
                new_sub, sub_changed = _normalize_stages(
                    sub_pipeline, scope_has_text_score, rewrite_text_score=True
                )
                new_spec[facet_name] = new_sub
                changed = changed or sub_changed
            result.append({stage_name: new_spec})

        elif (
            stage_name == "$lookup"
            and isinstance(spec, dict)
            and isinstance(spec.get("pipeline"), list)
        ):
            new_sub, sub_changed = _normalize_stages(
                spec["pipeline"], False, rewrite_text_score=False
            )
            new_spec = dict(spec)
            new_spec["pipeline"] = new_sub
            result.append({stage_name: new_spec})
            changed = changed or sub_changed

        else:
            new_value, value_changed = _normalize_value(
                spec, scope_has_text_score, rewrite_text_score
            )
            if value_changed:
                new_stage = dict(stage)
                new_stage[stage_name] = new_value
                result.append(new_stage)
            else:
                result.append(stage)
            changed = changed or value_changed

        if (
            stage_name == "$match"
            and isinstance(spec, dict)
            and "$text" in spec
        ):
            scope_has_text_score = True

    return (result if changed else stages), changed


def normalize_meta_pipeline(
    pipeline: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Validate ``$meta`` usage and normalize ``indexKey`` to MongoDB shape.

    The original pipeline object is returned unchanged when no rewriting is
    needed, so the common case adds no copying overhead.

    Args:
        pipeline: Aggregation pipeline stages to inspect.

    Returns:
        list[dict[str, Any]]: The original or a normalized pipeline.

    Raises:
        MalformedQueryException: If ``$meta: "textScore"`` appears without
            text score metadata in scope, or an unknown ``$meta`` field is
            used.
    """
    normalized, _ = _normalize_stages(pipeline, False)
    return normalized
