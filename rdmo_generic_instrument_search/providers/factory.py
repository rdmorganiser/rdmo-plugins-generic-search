from __future__ import annotations

from django.utils.module_loading import import_string

from rdmo_generic_instrument_search.config_utils import load_config_from_settings

from .registry import PROVIDER_REGISTRY

CONFIG_KEY = "InstrumentsOptionSetProvider"


def _resolve(name: str, dotted: str | None):
    return import_string(dotted) if dotted else PROVIDER_REGISTRY[name]


def build_providers() -> dict[str, object]:
    """Return {id_prefix: provider_instance} for fast lookup in handlers."""
    cfg = load_config_from_settings().get(CONFIG_KEY, {}) or {}
    instances: dict[str, object] = {}

    for class_name, entries in (cfg.get("providers") or {}).items():
        for raw in entries:
            params = dict(raw)
            dotted = params.pop("class", None)
            cls = _resolve(class_name, dotted)
            # Map recipe structures if present
            recipe_search = params.pop("search", None)
            recipe_detail = params.pop("detail", {}).get("requests", None)

            prov = cls(**params)  # id_prefix, base_url, text_prefix, max_hits...
            if recipe_search and hasattr(prov, "recipe_search"):
                prov.recipe_search = recipe_search
            if recipe_detail and hasattr(prov, "recipe_detail"):
                from .recipe import RequestSpec

                prov.recipe_detail = [
                    RequestSpec(url=r["url"], into=r.get("into", "$"), merge_included=bool(r.get("merge", {}).get("included")))
                    for r in recipe_detail
                ]
            instances[prov.id_prefix] = prov
    return instances
