from __future__ import annotations

from rdmo_generic_instrument_search.config_utils import load_config_from_settings

from .recipe import InstrumentSearchProvider

CONFIG_KEY = "GenericSearchProvider"


def build_providers() -> dict[str, InstrumentSearchProvider]:
    cfg = load_config_from_settings().get("GenericSearchProvider", {}) or {}
    result: dict[str, InstrumentSearchProvider] = {}
    for class_name, providers in (cfg.get("providers") or {}).items():
        for provider_entry in providers:
            prov = InstrumentSearchProvider.from_dict(provider_entry)
            result[prov.id_prefix] = prov
    return result
