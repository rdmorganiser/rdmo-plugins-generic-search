from __future__ import annotations

import logging
from dataclasses import dataclass, field

from django.utils import translation

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
    available: bool = field(default=True, repr=False)

    @classmethod
    def from_dict(cls, data):
        return cls(
            name=data["name"],
            catalog_uri=data["catalog_uri"],
            auto_complete_field_uri=data["auto_complete_field_uri"],
            attribute_mapping=data["attribute_mapping"],
            available=data.get("available", True),
        )

    def handle(self, external_id: str) -> dict:
        try:
            provider = _PROVIDERS[self.name]
        except KeyError as e:
            logger.warning("Unknown provider for id_prefix=%s", self.name)
            raise e from e
        doc = provider.detail(external_id)
        if not doc:
            logger.error("Empty detail document for %s:%s", self.name, external_id)
            return {}
        elif 'errors' in doc:
            logger.error('\n'.join(map(str, doc['errors'])))
            return {}

        lang = translation.get_language() or "en"  # current request locale (thread-local)
        return map_jamespath_to_attribute_uri(self.attribute_mapping, doc, context={"lang": lang})
