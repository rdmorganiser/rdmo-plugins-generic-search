from __future__ import annotations

from typing import Any


def _first_str(*values: Any) -> str | None:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, list):
            for item in value:
                if isinstance(item, str) and item.strip():
                    return item.strip()
                if isinstance(item, dict):
                    # common datacite title shape {"title": "..."}
                    title_val = item.get("title") if isinstance(item, dict) else None
                    if isinstance(title_val, str) and title_val.strip():
                        return title_val.strip()
    return None


def normalize_b2inst_record(doc: dict[str, Any]) -> dict[str, Any]:
    """Normalise a B2INST record into the shape our handler expects.

    The B2INST HTTP API is InvenioRDM-based. This helper extracts the handle PID,
    user-friendly title, landing page, and nested metadata regardless of whether
    they are exposed under ``metadata`` or top-level fields.
    """

    metadata = doc.get("metadata") or {}

    # Metadata blocks we care about
    datacite = metadata.get("datacite_attributes") or metadata.get("datacite_attribute") or {}
    b2inst_attrs = (
        metadata.get("b2inst_attributes")
        or metadata.get("b2inst_attribute")
        or metadata.get("b2inst")
        or {}
    )

    # PID / handle
    pids = metadata.get("pids") or doc.get("pids") or {}
    pid = (
        (pids.get("handle") or {}).get("identifier")
        or metadata.get("pid")
        or doc.get("pid")
        or (b2inst_attrs.get("Identifier") or {}).get("identifierValue")
        or doc.get("id")
    )

    # Human-readable label
    name = _first_str(
        metadata.get("Name"),
        b2inst_attrs.get("Name"),
        metadata.get("title"),
        metadata.get("titles"),
        datacite.get("titles"),
    )

    # Landing page / documentation URL
    links = metadata.get("links") or doc.get("links") or {}
    landing_page = (
        links.get("self_html")
        or links.get("self")
        or metadata.get("landing_page")
        or doc.get("landing_page")
        or metadata.get("LandingPage")
        or b2inst_attrs.get("LandingPage")
    )

    if isinstance(pid, str):
        pid = pid.strip()
    if isinstance(landing_page, str):
        landing_page = landing_page.strip()

    if name:
        doc["name"] = name
    if pid:
        doc["pid"] = pid

    doc.setdefault("datacite_attributes", datacite)
    doc.setdefault("b2inst_attributes", b2inst_attrs)
    if landing_page:
        doc["landing_page"] = landing_page

    return doc
