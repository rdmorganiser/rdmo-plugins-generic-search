from __future__ import annotations

import logging

from rdmo_generic_instrument_search.handlers.factory import build_handlers_by_catalog
from rdmo_generic_instrument_search.signals.value_updater import update_values_from_mapped_data

logger = logging.getLogger("rdmo.generic_search.signals.handler_post_save")
ALL_HANDLERS = build_handlers_by_catalog()


def handle_post_save(instance):
    if not ALL_HANDLERS:
        logger.warning("No handlers found for %s", __name__)
        return

    if not instance.external_id:
        logger.warning("external_id is empty: %r", instance)
        return

    try:
        id_prefix, external_id = str(instance.external_id).split(":")
    except Exception as e:
        logger.warning("Cannot parse external_id: %r, %s", instance.external_id, e)
        return

    matched = False
    for handler in ALL_HANDLERS.get(instance.project.catalog.uri, []):
        if handler.name == id_prefix and handler.auto_complete_field_uri == instance.attribute.uri:
            mapped = handler.handle(external_id=external_id)  # generic handler; provider detail inside
            if mapped.get("errors"):
                logger.error("Handler %s returned errors: %s", id_prefix, mapped["errors"])
                continue
            update_values_from_mapped_data(instance, mapped)
            matched = True

    if not matched:
        logger.warning(
            "No matching handler for id_prefix=%s attribute_uri=%s catalog=%s",
            id_prefix,
            instance.attribute.uri,
            instance.project.catalog.uri,
        )
