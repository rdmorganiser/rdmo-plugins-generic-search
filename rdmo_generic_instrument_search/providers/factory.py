from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from rdmo_generic_instrument_search.config_utils import load_config_from_settings
from rdmo_generic_instrument_search.providers.recipe import InstrumentSearchProvider

logger = logging.getLogger("rdmo.generic_search.providers.factory")

CONFIG_ROOT = "generic_search"


@dataclass(slots=True)
class RecipeDefaults:
    max_hits: int = 10
    lang: str | None = None


def _merge_recipe_defaults(entry: dict[str, Any], defaults: RecipeDefaults) -> dict[str, Any]:
    # Only apply keys that the recipe engine understands as defaults
    merged = dict(entry)
    merged.setdefault("max_hits", defaults.max_hits)
    # search.lang can be defaulted if not explicitly set
    search = dict(merged.get("search") or {})
    search.setdefault("lang", defaults.lang)
    merged["search"] = search
    return merged


def _validate_provider_entry(idx: int, entry: dict[str, Any]) -> None:
    prefix = entry.get("id_prefix")
    engine = entry.get("engine")
    if not prefix or not isinstance(prefix, str):
        raise ValueError(f"[generic_search.providers[{idx}]] missing/invalid id_prefix")
    if engine != "recipe":
        raise ValueError(f"[generic_search.providers[{idx}]] unknown engine={engine!r} (only 'recipe' supported)")
    search = entry.get("search") or {}
    mode = search.get("mode")
    if mode not in {"server", "client_filter", "sparql"}:
        raise ValueError(f"[{prefix}] search.mode must be one of 'server'|'client_filter'|'sparql'")
    # mode-specific requirements
    if mode in {"server", "client_filter"}:
        for key in ("url", "items_path", "id_path"):
            if not search.get(key):
                raise ValueError(f"[{prefix}] search.{key} is required for mode={mode}")
    if mode == "sparql":
        for key in ("endpoint", "query", "id_path"):
            if not search.get(key):
                raise ValueError(f"[{prefix}] search.{key} is required for mode=sparql")
    # detail presence
    detail = entry.get("detail") or {}
    steps = detail.get("steps")
    if not steps or not isinstance(steps, list):
        raise ValueError(f"[{prefix}] detail.steps must be a non-empty array")


def build_providers() -> dict[str, InstrumentSearchProvider]:
    """
    Build all providers from the new schema:
      [generic_search]
        defaults.recipe?
        providers = [ { engine='recipe', id_prefix=..., search={...}, detail={...} }, ... ]
    """
    root = load_config_from_settings().get(CONFIG_ROOT, {}) or {}
    defaults_tbl = (root.get("defaults") or {}).get("recipe") or {}
    defaults = RecipeDefaults(
        max_hits=int(defaults_tbl.get("max_hits") or 10),
        lang=(defaults_tbl.get("lang") or None),
    )
    providers = root.get("providers") or []
    if not isinstance(providers, list):
        raise ValueError("[generic_search.providers] must be an array of tables")

    result: dict[str, InstrumentSearchProvider] = {}
    seen: set[str] = set()
    for idx, entry in enumerate(providers):
        _validate_provider_entry(idx, entry)
        merged = _merge_recipe_defaults(entry, defaults)
        prov = InstrumentSearchProvider.from_dict(merged)
        if prov.id_prefix in seen:
            raise ValueError(f"Duplicate id_prefix={prov.id_prefix!r} in providers")
        seen.add(prov.id_prefix)
        result[prov.id_prefix] = prov
        logger.debug("built provider id_prefix=%s engine=recipe", prov.id_prefix)

    logger.info("generic-search: loaded %d providers", len(result))
    return result
