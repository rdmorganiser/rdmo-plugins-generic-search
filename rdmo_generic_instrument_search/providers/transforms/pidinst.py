from __future__ import annotations

from typing import Any


def normalize_pidinst_record(doc: dict[str, Any]) -> dict[str, Any]:
    """
    Normalise a PIDINST instrument record from search.pidinst.org:
      - strip leading/trailing whitespace on common string fields
      - ensure "name" is populated (fall back to first DataCite title or pid)
    """
    for key in (
        "pid",
        "name",
        "owner",
        "owner_identifier",
        "manufacturer",
        "manufacturer_identifier",
        "instrument_type",
        "instrument_type_identifier",
        "model",
        "measured_variable",
        "technical_info",
        "landing_page",
    ):
        val = doc.get(key)
        if isinstance(val, str):
            doc[key] = val.strip()

    # Fill name from datacite_attributes.titles[0].title if needed
    if not doc.get("name"):
        d = doc.get("datacite_attributes") or {}
        titles = d.get("titles") or []
        if titles and isinstance(titles, list):
            title = (titles[0] or {}).get("title")
            if isinstance(title, str) and title.strip():
                doc["name"] = title.strip()

    # Ensure nested dicts exist (quality-of-life for JMESPath mappings)
    doc.setdefault("datacite_attributes", {})
    doc.setdefault("b2inst_attributes", {})

    return doc
