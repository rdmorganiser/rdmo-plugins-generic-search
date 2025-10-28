def wikidata_flatten(doc: dict, prefer_langs: list[str] | None = None) -> dict:
    # wbgetentities → { "entities": { "Qxxx": { "labels": {...}, "descriptions": {...}, "aliases": {...}, "claims": {...} } } }
    entities = (doc or {}).get("entities", {})
    if not entities:
        return {}
    item = next(iter(entities.values()))
    langs = prefer_langs or ["en", "de"]

    def pick_lang(bag):
        for lg in langs:
            v = bag.get(lg)
            if v and (txt := v.get("value")):
                return txt
        return next(iter(bag.values()), {}).get("value")

    out = {
        "qid": item.get("id"),
        "label": pick_lang(item.get("labels", {})),
        "description": pick_lang(item.get("descriptions", {})),
        "aliases": [a.get("value") for lg in langs for a in item.get("aliases", {}).get(lg, [])],
        "claims": item.get("claims", {}),
    }

    # Lift common claims (image P18 → Commons filename)
    try:
        p18 = item["claims"].get("P18", [])
        if p18:
            out["image_filename"] = p18[0]["mainsnak"]["datavalue"]["value"]
            # optional: prebuild a fetchable URL
            out["image_url"] = f"https://commons.wikimedia.org/wiki/Special:FilePath/{out['image_filename']}?width=800"
    except Exception:
        pass

    return out
