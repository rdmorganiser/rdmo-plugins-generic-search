from __future__ import annotations

import logging
from dataclasses import dataclass

from rdmo_generic_instrument_search.handlers.parser import map_jamespath_to_attribute_uri
from rdmo_generic_instrument_search.providers.factory import build_providers

logger = logging.getLogger("rdmo.generic_search.handlers")

_PROVIDERS = build_providers()


@dataclass(slots=True)
class GenericDetailHandler:
    """Delegates detail retrieval to a provider chosen by id_prefix, then maps fields via JMESPath."""

    name: str
    catalog_uri: str
    auto_complete_field_uri: str
    attribute_mapping: dict[str, str]

    def handle(self, external_id: str) -> dict:
        try:
            provider = _PROVIDERS[self.name]
        except KeyError as e:
            logger.warning("Unknown provider for id_prefix=%s", self.name)
            raise e from e
        doc = provider.detail(external_id)
        if not doc:
            logger.debug("Empty detail document for %s:%s", self.name, external_id)
            return {}
        return map_jamespath_to_attribute_uri(self.attribute_mapping, doc)
