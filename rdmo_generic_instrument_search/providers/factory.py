from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from django.utils.module_loading import import_string

from rdmo_generic_instrument_search.config_utils import load_config_from_settings
from rdmo_generic_instrument_search.providers.recipe import InstrumentSearchProvider

logger = logging.getLogger("rdmo.generic_search.providers.factory")

CONFIG_ROOT = "generic_search"

# Simple registry. Add more built-ins here if you create them.
ENGINE_REGISTRY: dict[str, Callable[[Any], Any]] = {
    "recipe": InstrumentSearchProvider,
}


@dataclass(slots=True)
class RecipeDefaults:
    max_hits: int = 10
    lang: str | None = "en"


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

    # mode validation only for the recipe engine (others validate themselves)
    if engine == "recipe":
        search = entry.get("search") or {}
        mode = (search.get("mode") or "").lower()
        allowed_modes = {"server", "client_filter", "sparql", "wikidata_action"}
        if mode not in allowed_modes:
            raise ValueError(f"[{prefix}] search.mode must be one of {sorted(allowed_modes)!r}")

        # mode-specific requirements
        if mode in {"server", "client_filter"}:
            for key in ("url", "items_path", "id_path"):
                if not search.get(key):
                    if (
                        key == "url" and
                        (entry['base_url'].startswith("static://") or entry['base_url'].startswith("file://"))
                    ):
                        continue
                    raise ValueError(f"[{prefix}] search.{key} is required for mode={mode}")
        if mode == "sparql":
            for key in ("endpoint", "query", "id_path"):
                if not search.get(key):
                    raise ValueError(f"[{prefix}] search.{key} is required for mode=sparql")


def _resolve_engine(engine: str) -> Callable[Any, Any]:
    """
    Return a provider class for the given engine name.
    Supports:
      - registered short names (e.g. "recipe")
      - dotted paths "pkg.mod:Class" or "pkg.mod.Class"
    """
    if engine in ENGINE_REGISTRY:
        return ENGINE_REGISTRY[engine]

    dotted = engine.replace(":", ".")
    cls = import_string(dotted)
    return cls


def build_providers() -> dict[str, Any]:
    """
    Build all providers from the new schema:

      [generic_search]
        defaults.recipe?
        providers = [
          { engine='recipe'|'pkg.mod:Class', id_prefix='...', search={...}, detail={...} },
          ...
        ]
    """
    root = load_config_from_settings().get(CONFIG_ROOT, {}) or {}
    providers = root.get("providers") or []
    if not isinstance(providers, list):
        raise ValueError("[generic_search.providers] must be an array of tables")

    # recipe defaults (applied to recipe engine only)
    defaults_tbl = (root.get("defaults") or {}).get("recipe") or {}
    recipe_defaults = RecipeDefaults(
        max_hits=int(defaults_tbl.get("max_hits") or 10),
        lang=(defaults_tbl.get("lang") or None),
    )

    built: dict[str, Any] = {}
    seen: set[str] = set()

    for idx, entry in enumerate(providers):
        _validate_provider_entry(idx, entry)

        engine = entry["engine"]
        provider_cls = _resolve_engine(engine)

        # per-engine pre-merge
        cfg = entry
        if engine == "recipe":
            cfg = _merge_recipe_defaults(entry, recipe_defaults)

        # instantiate using classmethod from_dict if available, else pass kwargs
        if hasattr(provider_cls, "from_dict") and callable(provider_cls.from_dict):
            provider = provider_cls.from_dict(cfg)
        else:
            provider = provider_cls(**cfg)

        # enforce unique id_prefix and key the dict by it
        prefix = getattr(provider, "id_prefix", None)
        if not prefix:
            raise ValueError(f"[generic_search.providers[{idx}]] provider missing id_prefix after build")
        if prefix in seen:
            raise ValueError(f"Duplicate id_prefix={prefix!r} in providers")
        seen.add(prefix)
        if not provider.available:
            logger.debug("ignored(unavailable) id_prefix=%s engine=%s ", prefix, engine)
            continue

        built[prefix] = provider
        logger.debug("built provider id_prefix=%s engine=%s", prefix, engine)

    logger.info("generic-search: loaded %d providers", len(built))
    return built
