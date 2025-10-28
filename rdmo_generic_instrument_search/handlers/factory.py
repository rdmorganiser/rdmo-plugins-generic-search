from __future__ import annotations

import logging
from dataclasses import dataclass

from rdmo_generic_instrument_search.config_utils import load_config_from_settings
from rdmo_generic_instrument_search.handlers.generic import GenericDetailHandler

logger = logging.getLogger(__name__)


@dataclass
class HandlerInstanceData:
    id_prefix: str
    handler: GenericDetailHandler
    auto_complete_field_uri: str
    catalog_uri: str


def build_handlers_by_catalog() -> dict:
    cfg = load_config_from_settings()
    handlers_cfg = cfg.get("handlers", {}) or {}
    out: dict[str, list[HandlerInstanceData]] = {}

    for _, block in handlers_cfg.items():
        catalogs = block.get("catalogs", [])
        backends = block.get("backends", [])
        for cat in catalogs:
            catalog_uri = cat.get("catalog_uri")
            auto_uri = cat.get("auto_complete_field_uri")
            mapping = cat.get("attribute_mapping", {})
            if not catalog_uri or not auto_uri:
                logger.warning("Skipping catalog with missing URIs")
                continue
            # If no backends provided, we still create a handler using class defaults (id_prefix from provider)
            if not backends:
                inst = GenericDetailHandler(attribute_mapping=mapping)
                out.setdefault(catalog_uri, []).append(HandlerInstanceData(inst.id_prefix, inst, auto_uri, catalog_uri))
                continue
            # One binding per backend (id_prefix)
            for be in backends:
                inst = GenericDetailHandler(attribute_mapping=mapping, id_prefix=be.get("id_prefix"), base_url=be.get("base_url"))
                out.setdefault(catalog_uri, []).append(HandlerInstanceData(inst.id_prefix, inst, auto_uri, catalog_uri))
    return out
