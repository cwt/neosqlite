"""Normalize MongoDB stage aliases before tier dispatch.

Rewrites are applied once at the pipeline entry point so Tier 1, Tier 2,
Tier 3, ``explain()``, and nested ``$facet`` / ``$lookup`` pipelines all see
the same expanded stages.
"""

from __future__ import annotations

from typing import Any

from ...exceptions import MalformedQueryException

_SORT_BY_COUNT_FIELD = "count"


def _normalize_aliases(stages: list[Any]) -> tuple[list[Any], bool]:
    """Rewrite alias stages in one pipeline scope."""
    changed = False
    result: list[Any] = []

    for stage in stages:
        if not isinstance(stage, dict) or not stage:
            result.append(stage)
            continue

        stage_name = next(iter(stage))
        spec = stage[stage_name]

        if stage_name == "$set":
            if not isinstance(spec, dict):
                raise MalformedQueryException(
                    "$set specification must be an object"
                )
            changed = True
            result.append({"$addFields": spec})
            continue

        if stage_name == "$sortByCount":
            changed = True
            result.extend(
                [
                    {
                        "$group": {
                            "_id": spec,
                            _SORT_BY_COUNT_FIELD: {"$sum": 1},
                        }
                    },
                    {"$sort": {_SORT_BY_COUNT_FIELD: -1}},
                ]
            )
            continue

        if stage_name == "$facet" and isinstance(spec, dict):
            new_spec: dict[str, Any] = {}
            for facet_name, sub_pipeline in spec.items():
                new_sub, sub_changed = _normalize_aliases(sub_pipeline)
                new_spec[facet_name] = new_sub
                changed = changed or sub_changed
            result.append({stage_name: new_spec})
            continue

        if (
            stage_name == "$lookup"
            and isinstance(spec, dict)
            and isinstance(spec.get("pipeline"), list)
        ):
            new_sub, sub_changed = _normalize_aliases(spec["pipeline"])
            new_spec = dict(spec)
            new_spec["pipeline"] = new_sub
            result.append({stage_name: new_spec})
            changed = changed or sub_changed
            continue

        if (
            stage_name == "$unionWith"
            and isinstance(spec, dict)
            and isinstance(spec.get("pipeline"), list)
        ):
            new_sub, sub_changed = _normalize_aliases(spec["pipeline"])
            new_spec = dict(spec)
            new_spec["pipeline"] = new_sub
            result.append({stage_name: new_spec})
            changed = changed or sub_changed
            continue

        result.append(stage)

    return (result if changed else stages), changed


def normalize_pipeline_aliases(
    pipeline: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Expand ``$set`` and ``$sortByCount`` into canonical stages.

    Args:
        pipeline: Aggregation pipeline stages to inspect.

    Returns:
        list[dict[str, Any]]: The original pipeline when nothing changed, or a
        new pipeline with alias stages expanded.
    """
    normalized, _ = _normalize_aliases(pipeline)
    return normalized
