from __future__ import annotations

import logging

from rdmo_generic_instrument_search.config_utils import load_config_from_settings
from rdmo_generic_instrument_search.handlers.generic import GenericDetailHandler

logger = logging.getLogger("rdmo.generic_search.handlers.factory")


def build_handlers_by_catalog() -> dict:
    out: dict[str, list[GenericDetailHandler]] = {}
    root = load_config_from_settings().get("generic_search", {}) or {}
    handlers_tbl = root.get("handlers") or {}
    total = 0
    if not isinstance(handlers_tbl, dict):
        raise ValueError("[generic_search.handlers] must be a table of handler families")

    for _key, handler_config in (root.get("handlers") or {}).items():
        handler = GenericDetailHandler(
            name=_key,
            catalog_uri=handler_config["catalog_uri"],
            auto_complete_field_uri=handler_config["auto_complete_field_uri"],
            attribute_mapping=handler_config.get("attribute_mapping", {}),
        )
        out.setdefault(handler.catalog_uri, []).append(handler)
        total += 1
    logger.info("generic-search: loaded %d handler config(s) across %d catalog(s)", total, len(out))
    return out
