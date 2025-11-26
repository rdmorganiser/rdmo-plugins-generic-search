from __future__ import annotations


def add_contacts_from_o2a(doc: dict, contacts_path: str = "records") -> dict:
    # contacts endpoint returns {records: [{contact: {...} | ref}, ...]}
    contacts = []
    for rec in (doc.get("_contacts") or {}).get(contacts_path, []):  # expects prior step assigned to _contacts
        c = rec.get("contact")
        if isinstance(c, dict):
            slim = {k: c[k] for k in ("firstName", "lastName", "email") if k in c}
            contacts.append(slim)
    doc["contacts"] = contacts
    return doc


def add_parameters_with_units_from_o2a(doc: dict) -> dict:
    # expects prior steps saved at _parameters.records and _units.records
    params = (doc.get("_parameters") or {}).get("records", [])
    units = (doc.get("_units") or {}).get("records", [])
    unit_lookup = {u.get("@uuid"): u.get("code") for u in units if "@uuid" in u}

    out = []
    for p in params:
        name = p.get("name", "")
        udata = p.get("unit")
        if isinstance(udata, dict):
            unit = udata.get("code", "")
        else:
            unit = unit_lookup.get(udata, "")
        out.append({"name": name, "unit": unit})
    doc["parameters"] = out
    return doc
