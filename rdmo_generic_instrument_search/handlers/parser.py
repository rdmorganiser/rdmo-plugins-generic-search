from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

import jmespath

logger = logging.getLogger("rdmo.generic_search.handlers")


def _eval_jmespath(expr: str, data: Any) -> Any:
    try:
        return jmespath.search(expr, data)
    except Exception as e:
        logger.debug("JMESPath error for %r: %s", expr, e)
        return None


def _render(expr: str, ctx: Mapping[str, Any] | None) -> str:
    if not ctx:
        return expr
    try:
        return expr.format(**ctx)
    except Exception:
        return expr


def _first_meaningful(v: Any) -> Any:
    """Pick the first non-empty scalar from lists like ['x', None, ''], otherwise return v."""
    if isinstance(v, list):
        for item in v:
            if item not in (None, "", [], {}):
                return item
        return None
    return v


def map_jamespath_to_attribute_uri(
    attribute_mapping: Mapping[str, str],
    data: dict,
    *,
    context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Evaluate a mapping of:
        JMESPath (with optional `{lang}` and `||` fallbacks) -> attribute_uri

    Behavior:
      - `{lang}` gets formatted from `context={"lang": ...}`.
      - `a || b || c` tries a,b,c and picks the first non-empty.
      - If an expression yields a list but the mapping key does NOT contain `[]`,
        the first non-empty element is taken (quality-of-life for scalar attributes).
    """
    mapped_values: dict[str, Any] = {}

    for raw_key, attribute_uri in attribute_mapping.items():
        candidates = [k.strip() for k in raw_key.split("||") if k.strip()]

        value = None
        for cand in candidates:
            expr = _render(cand, context)
            v = _eval_jmespath(expr, data)
            expects_list = cand.endswith("[]")
            if not expects_list:
                v = _first_meaningful(v)
            if v not in (None, "", [], {}):
                value = v
                break

        # Always include the attribute; your updater deletes on None/""/[].
        mapped_values[attribute_uri] = value

    logger.debug("mapped_values %s", mapped_values)
    return mapped_values
