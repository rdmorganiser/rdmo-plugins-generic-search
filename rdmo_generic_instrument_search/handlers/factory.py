from __future__ import annotations

import logging

from rdmo_generic_instrument_search.config_utils import load_config_from_settings
from rdmo_generic_instrument_search.handlers.generic import GenericDetailHandler

logger = logging.getLogger("rdmo.generic_search.handlers.factory")


def build_handlers_by_catalog() -> dict:
    out: dict[str, list[GenericDetailHandler]] = {}
    root = load_config_from_settings().get("generic_search", {}) or {}
    handlers_tbl = root.get("handlers") or {}
    if not isinstance(handlers_tbl, dict):
        raise ValueError("[generic_search.handlers] must be a table of handler configurations")

    total = 0
    for _key, handler_config in (root.get("handlers") or {}).items():
        handler_config["name"] = _key  # inject handler key as name
        handler = GenericDetailHandler.from_dict(handler_config)
        if not handler.available:
            logger.debug("skipped handler %s", handler.name)
            continue

        out.setdefault(handler.catalog_uri, []).append(handler)
        logger.debug("built handler %s", handler.name)
        total += 1
    logger.info("generic-search: loaded %d handler config(s) across %d catalog(s)", total, len(out))
    return out
