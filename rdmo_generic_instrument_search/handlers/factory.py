from __future__ import annotations

import logging

from rdmo_generic_instrument_search.config_utils import load_config_from_settings
from rdmo_generic_instrument_search.handlers.generic import GenericDetailHandler

logger = logging.getLogger("rdmo.generic_search.handlers.factory")


def build_handlers_by_catalog() -> dict:
    out: dict[str, list[GenericDetailHandler]] = {}
    for _x, handler_configs in load_config_from_settings().get("handlers", {}).items():
        for handler_config in handler_configs.get("config", []):
            handler = GenericDetailHandler(
                id_prefix=handler_config["id_prefix"],
                catalog_uri=handler_config["catalog_uri"],
                auto_complete_field_uri=handler_config["auto_complete_field_uri"],
                attribute_mapping=handler_config.get("attribute_mapping", {}),
            )
            out.setdefault(handler.catalog_uri, []).append(handler)

    return out
