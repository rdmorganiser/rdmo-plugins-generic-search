from __future__ import annotations

import logging
from typing import Any

from django.db import transaction

from rdmo.domain.models import Attribute
from rdmo.projects.models import Value
from rdmo.questions.models import Question, QuestionSet

from .utils import mute_value_post_save

logger = logging.getLogger(__name__)


# ---------- small utilities --------------------------------------------------


def _is_blank_scalar(v: Any) -> bool:
    """None or empty/whitespace-only string means 'delete'."""
    if v is None:
        return True
    if isinstance(v, str) and v.strip() == "":
        return True
    return False


def _normalize_scalar(v: Any) -> Any:
    """Keep numbers/bools; stringify everything else (but leave None as None)."""
    if isinstance(v, (int, float, bool)):
        return v
    return v if v is None else str(v)


def _collection_shape(instance, attribute) -> str | None:
    """
    Identify how this attribute is collected in the current catalog context.
    Returns:
      "question"    -> Question with is_collection=True, uses collection_index
      "questionset" -> QuestionSet with is_collection=True, uses child set_index + set_prefix
      None          -> Not a collection here
    """
    q_count = Question.objects.filter(
        is_collection=True,
        attribute=attribute,
        pages__sections__catalogs__id__exact=instance.project.catalog.id,
    ).count()

    qs_count = QuestionSet.objects.filter(
        is_collection=True,
        pages__sections__catalogs__id__exact=instance.project.catalog.id,
        questions__attribute=attribute,
    ).count()

    if q_count == 1 and qs_count == 0:
        return "question"
    if qs_count > 0:
        return "questionset"
    return None


def _qs_scalar(instance, attribute):
    return Value.objects.filter(
        project=instance.project,
        attribute=attribute,
        set_index=instance.set_index,
        set_collection=False,
    )


def _qs_collection(instance, attribute, mode: str):
    if mode == "question":
        return Value.objects.filter(
            project=instance.project,
            attribute=attribute,
            set_collection=True,
            set_index=instance.set_index,
        )
    # mode == "questionset"
    return Value.objects.filter(
        project=instance.project,
        attribute=attribute,
        set_collection=True,
        set_prefix=instance.set_index,
    )


# ---------- public entry -----------------------------------------------------


def update_values_from_mapped_data(instance, data: dict[str, Any]) -> None:
    """
    Apply mapped attribute values:
      - scalar: update, or delete if blank (None / "").
      - list: upsert non-blank entries, delete blanks at their indices, and
              trim any surplus existing entries beyond the new list length.
    All changes are atomic and do not re-trigger post_save.
    """
    if not data:
        return

    with transaction.atomic(), mute_value_post_save():
        for attribute_uri, incoming in data.items():
            try:
                attribute = Attribute.objects.get(uri=attribute_uri)
            except Attribute.DoesNotExist:
                continue  # mapping points to an attribute unknown to this instance

            # ----- list handling ------------------------------------------------
            if isinstance(incoming, list):
                _apply_list(instance, attribute, incoming)
                continue

            # ----- scalar handling ----------------------------------------------
            if _is_blank_scalar(incoming):
                # delete any existing scalar value(s) for this attribute in the current set
                deleted, _ = _qs_scalar(instance, attribute).delete()
                if deleted:
                    logger.debug("deleted %d scalar value(s) for %s", deleted, attribute.uri)
                continue

            new_text = _normalize_scalar(incoming)
            current = _qs_scalar(instance, attribute).first()

            # Compare against the computed display too (may differ for booleans/datetime)
            if current and (current.text == new_text or current.value == new_text):
                continue

            Value.objects.update_or_create(
                project=instance.project,
                attribute=attribute,
                set_index=instance.set_index,
                set_collection=False,
                defaults={"text": new_text},
            )


# ---------- list logic -------------------------------------------------------


def _apply_list(instance, attribute, items: list[Any]) -> None:
    """
    Upsert list items and delete surplus/blank ones based on the collection shape.
    Empty list => delete all existing collection entries for this attribute/set.
    """
    mode = _collection_shape(instance, attribute)

    if mode is None:
        # You provided a list but the catalog doesn't declare a collection for this attribute.
        # Fail soft: clean up any stray collection entries to avoid duplicates.
        logger.warning(
            "List value found, but no matching Question/QuestionSet with is_collection=True. Attribute=%s",
            attribute.uri,
        )
        (
            Value.objects.filter(project=instance.project, attribute=attribute, set_collection=True)
            .filter(set_index=instance.set_index)  # same level
            .delete()
        )
        (
            Value.objects.filter(
                project=instance.project, attribute=attribute, set_collection=True, set_prefix=instance.set_index
            ).delete()
        )
        return

    qs_all = _qs_collection(instance, attribute, mode)

    # incoming empty list => delete all existing collection entries
    if not items:
        deleted, _ = qs_all.delete()
        if deleted:
            logger.debug("deleted %d collection value(s) for %s (empty incoming list)", deleted, attribute.uri)
        return

    # Fetch existing entries keyed by index (DO NOT reference the non-field 'value' here)
    if mode == "question":
        existing = {v.collection_index: v for v in qs_all.only("id", "collection_index", "text")}

        def upsert_at(i: int, text: Any):
            Value.objects.update_or_create(
                project=instance.project,
                attribute=attribute,
                set_collection=True,
                set_index=instance.set_index,
                collection_index=i,
                defaults={"text": text},
            )

        def delete_index(i: int):
            qs_all.filter(collection_index=i).delete()

        def delete_from(start: int):
            qs_all.filter(collection_index__gte=start).delete()
    else:  # "questionset"
        existing = {v.set_index: v for v in qs_all.only("id", "set_index", "text")}

        def upsert_at(i: int, text: Any):
            Value.objects.update_or_create(
                project=instance.project,
                attribute=attribute,
                set_collection=True,
                set_prefix=instance.set_index,
                set_index=i,
                defaults={"text": text},
            )

        def delete_index(i: int):
            qs_all.filter(set_index=i, set_prefix=instance.set_index).delete()

        def delete_from(start: int):
            qs_all.filter(set_index__gte=start).delete()

    # Upsert non-blank entries; delete at specific index for blanks
    last_nonblank_index = -1
    for i, raw in enumerate(items):
        if _is_blank_scalar(raw):
            # delete this position if it exists
            delete_index(i)
            continue

        text = _normalize_scalar(raw)
        cur = existing.get(i)
        # Compare against stored text OR computed display (through .value property)
        if not cur or (cur.text != text and getattr(cur, "value", None) != text):
            upsert_at(i, text)
        last_nonblank_index = max(last_nonblank_index, i)

    # Trim surplus: anything beyond the last non-blank incoming index must go
    delete_from(last_nonblank_index + 1)
