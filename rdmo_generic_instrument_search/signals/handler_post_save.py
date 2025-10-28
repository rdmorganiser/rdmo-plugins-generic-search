from __future__ import annotations

import logging

from rdmo_generic_instrument_search.handlers.factory import build_handlers_by_catalog
from rdmo_generic_instrument_search.signals.value_updater import update_values_from_mapped_data

logger = logging.getLogger(__name__)
ALL = build_handlers_by_catalog()


def handle_post_save(instance):
    try:
        id_prefix, external_id = str(instance.external_id).split(":")
    except Exception:
        logger.warning("Cannot parse external_id: %r", instance.external_id)
        return

    cat_uri = instance.project.catalog.uri
    attr_uri = instance.attribute.uri
    candidates = ALL.get(cat_uri, [])
    matched = False
    for c in candidates:
        if c.id_prefix == id_prefix and c.auto_complete_field_uri == attr_uri:
            mapped = c.handler.handle(id_=external_id)  # generic handler; provider detail inside
            if mapped.get("errors"):
                logger.error("Handler %s returned errors: %s", id_prefix, mapped["errors"])
                continue
            update_values_from_mapped_data(instance, mapped)
            matched = True

    if not matched:
        logger.warning("No matching handler for id_prefix=%s attribute_uri=%s catalog=%s", id_prefix, attr_uri, cat_uri)
