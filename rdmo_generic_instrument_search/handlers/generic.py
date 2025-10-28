from __future__ import annotations

import logging
from dataclasses import dataclass

from rdmo_generic_instrument_search.handlers.base import GenericSearchHandler
from rdmo_generic_instrument_search.handlers.parser import map_jamespath_to_attribute_uri
from rdmo_generic_instrument_search.providers.factory import build_providers

logger = logging.getLogger(__name__)

_PROVIDERS = build_providers()


@dataclass
class CatalogBinding:
    catalog_uri: str
    auto_complete_field_uri: str
    attribute_mapping: dict[str, str]


class GenericDetailHandler(GenericSearchHandler):
    """One handler to rule them all: delegates to providers selected by id_prefix."""

    def __init__(self, *, attribute_mapping: dict, id_prefix: str | None = None, base_url: str | None = None):
        super().__init__(attribute_mapping=attribute_mapping, id_prefix=id_prefix, base_url=base_url)

    def handle(self, id_: str) -> dict:
        provider = _PROVIDERS.get(self.id_prefix)
        if not provider:
            logger.warning("Unknown provider for id_prefix=%s", self.id_prefix)
            return {}
        doc = provider.detail(id_)
        if not doc:
            logger.debug("Empty detail document for %s:%s", self.id_prefix, id_)
            return {}
        return map_jamespath_to_attribute_uri(self.attribute_mapping, doc)
