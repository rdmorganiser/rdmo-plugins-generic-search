from __future__ import annotations

from collections.abc import Iterable

from rdmo_generic_instrument_search.client import fetch_json

DEFAULT_BASE = "https://www.wikidata.org"
DEFAULT_LANGS = ("en", "de")
Q_MEASURING_INSTRUMENT = "Q2041172"  # measuring instrument


def qid_from_iri(value: str) -> str:
    if "/entity/" in value:
        return value.rsplit("/", 1)[-1]
    return value


def wbsearchentities(query: str, *, base_url: str = DEFAULT_BASE, lang: str = "en", limit: int = 30) -> list[str]:
    url = (
        f"{base_url}/w/api.php?"
        f"action=wbsearchentities&format=json"
        f"&language={lang}&uselang={lang}&type=item&limit={int(limit)}"
        f"&search={query}"
    )
    doc = fetch_json(url) or {}
    return [hit.get("id") for hit in (doc.get("search") or []) if hit.get("id")]


def wbgetentities(qids: Iterable[str], *, base_url: str = DEFAULT_BASE, lang: str = "en") -> dict[str, dict]:
    ids = "|".join(qids)
    if not ids:
        return {}
    url = (
        f"{base_url}/w/api.php?"
        f"action=wbgetentities&format=json"
        f"&languages={lang}|en|de&props=labels|descriptions|aliases|claims"
        f"&ids={ids}"
    )
    doc = fetch_json(url) or {}
    return doc.get("entities") or {}


def _claims_values(entity: dict, pid: str) -> list[str]:
    try:
        claims = entity.get("claims", {}).get(pid, [])
        out: list[str] = []
        for c in claims:
            mainsnak = c.get("mainsnak") or {}
            dv = (mainsnak.get("datavalue") or {}).get("value")
            if isinstance(dv, dict) and dv.get("id"):
                out.append(dv["id"])
        return out
    except Exception:
        return []


def pick_label(entity: dict, langs: list[str] = DEFAULT_LANGS) -> str | None:
    labels = entity.get("labels", {})
    for lg in langs:
        if (v := labels.get(lg)) and (t := v.get("value")):
            return t
    try:
        return next(iter(labels.values()), {}).get("value")
    except Exception:
        return None


# ---------------- transitive subclass-of (P279*) with memo ------------------


def _parents_p279(entity: dict) -> list[str]:
    """Direct parents via P279 (subclass of)."""
    return _claims_values(entity, "P279")


def _ensure_entities(qids: Iterable[str], *, base_url: str, lang: str, cache: dict[str, dict]) -> None:
    """Fetch any missing entities into the cache."""
    missing = [q for q in qids if q not in cache]
    if not missing:
        return
    cache.update(wbgetentities(missing, base_url=base_url, lang=lang))


def _any_reaches_root_via_p279(
    start_qids: Iterable[str], *, root_qid: str, base_url: str, lang: str, cache: dict[str, dict], max_depth: int = 5
) -> bool:
    """
    Bounded BFS up the subclass-of tree:
      return True if any start_qid has P279* → root_qid within max_depth.
    """
    frontier: set[str] = set(start_qids)
    visited: set[str] = set()

    depth = 0
    while frontier and depth <= max_depth:
        _ensure_entities(frontier, base_url=base_url, lang=lang, cache=cache)

        if root_qid in frontier:
            return True

        next_frontier: set[str] = set()
        for q in frontier:
            if q in visited:
                continue
            visited.add(q)
            ent = cache.get(q) or {}
            parents = _parents_p279(ent)
            if not parents:
                continue
            if root_qid in parents:
                return True
            for parent in parents:
                if parent not in visited:
                    next_frontier.add(parent)

        frontier = next_frontier
        depth += 1

    return False


def is_instrument(
    entity: dict,
    *,
    base_url: str = DEFAULT_BASE,
    lang: str = "en",
    root_qid: str = Q_MEASURING_INSTRUMENT,
    max_depth: int = 5,
    _cache: dict[str, dict] | None = None,
) -> bool:
    """
    Accept if:
      - item itself is a (direct or transitive) subclass of measuring instrument; OR
      - item is an instance of some class that is (transitively) a subclass of measuring instrument.
    """
    if not entity:
        return False

    cache: dict[str, dict] = _cache or {}

    # 1) subclass-of chain from the item (covers type pages)
    item_qid = entity.get("id")
    if item_qid:
        if _any_reaches_root_via_p279(
            [item_qid], root_qid=root_qid, base_url=base_url, lang=lang, cache=cache, max_depth=max_depth
        ):
            return True

    # 2) instance-of classes → subclass-of chain
    inst_classes = _claims_values(entity, "P31")
    if not inst_classes:
        return False

    if _any_reaches_root_via_p279(
        inst_classes, root_qid=root_qid, base_url=base_url, lang=lang, cache=cache, max_depth=max_depth
    ):
        return True

    return False


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
