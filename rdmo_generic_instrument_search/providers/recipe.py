from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import quote

from django.conf import settings

from rdmo_generic_instrument_search.client import _sparql_post_json, fetch_json

from .base import BaseInstrumentProvider  # jmespath helper via self._jp
from .transforms.wikidata import is_instrument, pick_label, wbgetentities, wbsearchentities

logger = logging.getLogger("rdmo.generic_search.providers.recipe")


# -------------------------------
# Dataclasses for typed config
# -------------------------------


@dataclass(slots=True)
class SearchConfig:
    # modes: "server" | "client_filter" | "sparql" | "wikidata_action"
    mode: str

    # common (server/client_filter)
    url: str | None = None
    items_path: str | None = None
    id_path: str | None = None
    label_path: str | None = None
    label_template: str | None = None
    filter_any_paths: list[str] = field(default_factory=list)  # client_filter

    # SPARQL extras
    endpoint: str | None = None
    query: str | None = None
    lang: str | None = None
    root_qid: str | None = None

    @classmethod
    def from_dict(cls, data: dict | None) -> SearchConfig:
        if not data:
            raise ValueError("SearchConfig.from_dict requires mapping")
        return cls(
            mode=data["mode"],
            url=data.get("url"),
            items_path=data.get("items_path"),
            id_path=data.get("id_path"),
            label_path=data.get("label_path"),
            label_template=data.get("label_template"),
            filter_any_paths=data.get("filter_any_paths"),
            endpoint=data.get("endpoint"),
            query=data.get("query"),
            lang=data.get("lang"),
            root_qid=data.get("root_qid"),
        )


@dataclass(slots=True)
class FetchStep:
    url: str
    merge_included: bool = False
    assign: str | None = None

    @classmethod
    def from_dict(cls, data: dict | None) -> FetchStep:
        if not data:
            raise ValueError("FetchStep.from_dict requires mapping")
        return cls(
            url=data["url"],
            merge_included=bool(data.get("merge_included")),
            assign=data.get("assign"),
        )


@dataclass(slots=True)
class Transform:
    dotted: str
    kwargs: dict[str, Any] | None = None

    @classmethod
    def from_dict(cls, data: dict | None) -> Transform:
        if not data:
            raise ValueError("Transform.from_dict requires mapping")
        return cls(dotted=data["dotted"], kwargs=data.get("kwargs"))


# -------------------------------
# Provider
# -------------------------------


@dataclass(kw_only=True, slots=True)
class InstrumentSearchProvider(BaseInstrumentProvider):
    search_config: SearchConfig | None = field(default=None, repr=False)
    fetch_steps: list[FetchStep] | None = field(default=None, repr=False)
    transforms: list[Transform] | None = field(default=None, repr=False)

    @classmethod
    def from_dict(cls, data: dict) -> InstrumentSearchProvider:
        id_prefix = data["id_prefix"]
        search = SearchConfig.from_dict(data["search"])
        detail = data["detail"]
        fetch_steps = [FetchStep.from_dict(s) for s in (detail.get("steps") or [])]
        if not search.mode == "client_filter" and not fetch_steps:
            raise ValueError(f"{id_prefix} detail.steps must be a non-empty array")
        transforms = [Transform.from_dict(t) for t in (detail.get("transforms") or [])]

        return cls(
            id_prefix=data["id_prefix"],
            base_url=data["base_url"],
            text_prefix=data["text_prefix"],
            max_hits=int(data.get("max_hits") or 10),
            available=data.get("available", True),
            search_config=search,
            fetch_steps=fetch_steps,
            transforms=transforms or None,
        )

    # -------------------------------
    # SEARCH
    # -------------------------------
    def search(self, query: str) -> list[dict]:
        if not query or not self.search_config:
            return []

        if self.search_config.url is None:
            url = self.base_url
        else:
            url = self.search_config.url.format(base_url=self.base_url, query=quote(query))

        mode = self.search_config.mode.lower()
        if mode == "server":
            return self._search_server(url, self.search_config)
        if mode == "client_filter":
            return self._search_client_filter(url, query, self.search_config)
        if mode == "sparql":
            return self._search_sparql(query, self.search_config)
        if mode == "wikidata_action":
            return self._search_wikidata_action(query, self.search_config)

        logger.warning("Unknown search mode %r for provider %s", mode, self.id_prefix)
        return []

    def fetch_and_search(self, url: str, items_path: str):
        doc = fetch_json(url) or {}
        items = self._jp(items_path, doc) or []
        return doc, items

    def _search_server(self, url: str, spec: SearchConfig) -> list[dict]:
        _doc, items = self.fetch_and_search(url, spec.items_path)
        return self._items_to_options(items, spec)

    def _search_client_filter(self, url: str, query: str, spec: SearchConfig) -> list[dict]:
        _doc, items = self.fetch_and_search(url, spec.items_path)
        filtered: list[Any] = []
        for item in items:
            if any(
                    self._contains(item, filter_path,  query.lower())
                    for filter_path in spec.filter_any_paths
            ):
                filtered.append(item)
                if len(filtered) >= self.max_hits:
                    break
        return self._items_to_options(filtered, spec)

    def _search_sparql(self, query: str, spec: SearchConfig) -> list[dict]:
        if not spec.endpoint or not spec.query:
            return []
        lang = spec.lang or getattr(settings, "LANGUAGE_CODE", "en") or "en"
        root_qid = spec.root_qid or ""
        sparql = spec.query.replace("{query}", query).replace("{lang}", lang).replace("{root_qid}", root_qid)
        rows = _sparql_post_json(spec.endpoint, sparql) or {}
        items = self._jp(spec.items_path or "results.bindings", rows) or []
        return self._items_to_options(items, spec, normalize_wikidata_ids=True)

    def _search_wikidata_action(self, query: str, spec: SearchConfig) -> list[dict]:
        lang = spec.lang or getattr(settings, "LANGUAGE_CODE", "en") or "en"
        base = self.base_url or "https://www.wikidata.org"
        root = spec.root_qid or "Q2041172"

        qids = wbsearchentities(query, base_url=base, lang=lang, limit=self.max_hits * 3)  # small overfetch
        if not qids:
            return []

        entities = wbgetentities(qids, base_url=base, lang=lang)

        out: list[dict] = []
        cache: dict[str, dict] = dict(entities)  # seed cache with what we already have
        for qid, ent in entities.items():
            if not ent:
                continue
            if not is_instrument(ent, base_url=base, lang=lang, root_qid=root, max_depth=5, _cache=cache):
                continue
            label = pick_label(ent, [lang, "en", "de"]) or qid
            text = f"{(self.text_prefix or '').strip()} {label}".strip()
            out.append({"id": f"{self.id_prefix}:{qid}", "text": text})
            if len(out) >= self.max_hits:
                break
        return out

    # -------------------------------
    # DETAIL
    # -------------------------------
    # in recipe.py, inside InstrumentSearchProvider
    def detail(self, remote_id: str) -> dict:
        """
        Fetch a detail document.

        - Normal case: follow HTTP steps and merge JSON.
        - Special case (PIDINST, or any static client_filter provider with no steps):
          look up the record directly in the same JSON index used for search.
        """
        doc: dict[str, Any] = {}

        # --- Special case: static client_filter with no explicit detail steps ---
        if (not self.fetch_steps) and self.search_config and self.search_config.mode == "client_filter":

            # Re-use the search source; for pidinst this is the static file
            if self.search_config.url:
                url = self.search_config.url
            else:
                # fallback if someone uses base_url-only static config later
                url = self.base_url

            _index, items = self.fetch_and_search(url, self.search_config.items_path or "@")

            # Normalise single-record vs list
            if isinstance(items, dict):
                items = [items]

            match: dict[str, Any] | None = None
            for item in items:
                rid = self._jp(self.search_config.id_path, item) if self.search_config.id_path else None
                if rid is None:
                    continue
                rid_str = str(rid)
                if rid_str == str(remote_id):
                    match = item
                    break

            if not match:
                logger.warning("No PIDINST record found for pid=%s", remote_id)
                return {}

            doc = match

        # --- Normal HTTP multi-step case (unchanged) ---
        else:
            for step in self.fetch_steps or []:
                url = step.url.format(base_url=self.base_url, id=remote_id)
                part = fetch_json(url) or {}

                if step.assign:
                    doc[step.assign] = part
                    continue

                if step.merge_included:
                    doc["included"] = [*doc.get("included", []), *part.get("included", [])]

                for k, v in part.items():
                    if k == "included" and not step.merge_included:
                        doc["included"] = [*doc.get("included", []), *v]
                    else:
                        doc[k] = v

        # --- Transforms (PIDINST normalize happens here) ---
        for t in self.transforms or []:
            fn = self._import_callable(t.dotted)
            try:
                doc = fn(doc, **(t.kwargs or {})) or doc
            except Exception as e:
                logger.warning("Transform %s failed: %s", t.dotted, e)

        return doc

    # -------------------------------
    # Helpers
    # -------------------------------
    def _items_to_options(self, items: list, spec: SearchConfig, *, normalize_wikidata_ids: bool = False) -> list[dict]:
        out: list[dict] = []
        id_path = spec.id_path
        label_path = spec.label_path
        label_tpl = (spec.label_template or "{label}").strip()

        if not id_path:
            return out

        for it in items[: self.max_hits]:
            rid = self._jp(id_path, it)
            lbl = self._jp(label_path, it) if label_path else None
            if rid is None:
                continue
            if isinstance(rid, (int, float)):
                rid = str(rid)
            if normalize_wikidata_ids and isinstance(rid, str):
                rid = self._qid_from_iri(rid)

            prefix = (self.text_prefix or "").strip()
            text = label_tpl.format(
                prefix=prefix,
                label=lbl or "",
                code=self._jp("Instrument.code", it) or "",
            ).strip()

            out.append({"id": f"{self.id_prefix}:{rid}", "text": text or f"{self.id_prefix}:{rid}"})
        return out

    def _contains(self, item: dict, path: str, q: str) -> bool:
        val = self._jp(path, item)
        if val is None:
            return False
        return q in str(val).lower()

    @staticmethod
    def _qid_from_iri(value: str) -> str:
        if "/entity/" in value:
            return value.rsplit("/", 1)[-1]
        return value

    @staticmethod
    def _import_callable(dotted: str):
        from importlib import import_module

        if ":" in dotted:
            mod, fn = dotted.split(":")
        else:
            parts = dotted.split(".")
            mod, fn = ".".join(parts[:-1]), parts[-1]
        return getattr(import_module(mod), fn)
