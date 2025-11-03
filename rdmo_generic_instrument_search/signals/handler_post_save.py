# rdmo_generic_instrument_search/signals/handler_post_save.py
from __future__ import annotations

import logging

from rdmo_generic_instrument_search.handlers.factory import build_handlers_by_catalog
from rdmo_generic_instrument_search.signals.value_updater import (
    build_clear_payload,  # NEW
    update_values_from_mapped_data,
)

logger = logging.getLogger("rdmo.generic_search.signals.handler_post_save")
ALL_HANDLERS = build_handlers_by_catalog()


def handle_post_save(instance):
    if not ALL_HANDLERS:
        logger.warning("No handlers found for %s", __name__)
        return

    # ------------------------------------------------------------
    # Branch A: empty selection on autocomplete -> clear all mapped
    # ------------------------------------------------------------
    if not instance.external_id and getattr(instance, "is_empty", False):
        # find handlers for this catalog whose autocomplete field is this attribute
        handlers = [
            h for h in ALL_HANDLERS.get(instance.project.catalog.uri, []) if h.auto_complete_field_uri == instance.attribute.uri
        ]
        if not handlers:
            logger.info(
                "No matching handler for empty selection: attribute_uri=%s catalog=%s",
                instance.attribute.uri,
                instance.project.catalog.uri,
            )
            return

        for h in handlers:
            payload = build_clear_payload(h.attribute_mapping)
            update_values_from_mapped_data(instance, payload)
        return

    # ------------------------------------------------------------
    # Branch B: normal selection -> fetch and map details
    # ------------------------------------------------------------
    if not instance.external_id:
        # no external id and not explicitly empty -> nothing to do
        logger.debug("external_id is empty and not marked empty: %r", instance)
        return

    try:
        id_prefix, external_id = str(instance.external_id).split(":")
    except Exception as e:
        logger.warning("Cannot parse external_id: %r, %s", instance.external_id, e)
        return

    matched = False
    for handler in ALL_HANDLERS.get(instance.project.catalog.uri, []):
        if handler.name == id_prefix and handler.auto_complete_field_uri == instance.attribute.uri:
            mapped = handler.handle(external_id=external_id)  # provider detail inside
            if not mapped:
                logger.debug("Handler %s returned empty.", id_prefix)
                continue

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
