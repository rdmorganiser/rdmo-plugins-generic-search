from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import quote

from django.conf import settings

from rdmo_generic_instrument_search.client import _sparql_post_json, fetch_json

from .base import BaseInstrumentProvider  # jmespath helper via self._jp

logger = logging.getLogger("rdmo.generic_search.providers.recipe")


# -------------------------------
# Dataclasses for typed config
# -------------------------------


@dataclass(slots=True)
class SearchConfig:
    # modes: "server" | "client_filter" | "sparql"
    mode: str

    # common (server/client_filter)
    url: str | None = None
    items_path: str | None = None
    id_path: str | None = None
    label_path: str | None = None
    label_template: str | None = None
    filter_any_paths: list[str] | None = None  # client_filter

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
        search = SearchConfig.from_dict(data["search"])
        detail = data["detail"]
        steps = [FetchStep.from_dict(s) for s in (detail.get("steps") or [])]
        if not steps:
            raise ValueError("detail.steps must be a non-empty array")
        transforms = [Transform.from_dict(t) for t in (detail.get("transforms") or [])]

        return cls(
            id_prefix=data["id_prefix"],
            base_url=data.get("base_url", ""),
            text_prefix=data.get("text_prefix"),
            max_hits=int(data.get("max_hits") or 10),
            search_config=search,
            fetch_steps=steps,
            transforms=transforms or None,
        )

    # -------------------------------
    # SEARCH
    # -------------------------------
    def search(self, query: str) -> list[dict]:
        spec = self.search_config
        if not query or not spec:
            return []

        mode = spec.mode.lower()
        if mode == "server":
            return self._search_server(query, spec)
        if mode == "client_filter":
            return self._search_client_filter(query, spec)
        if mode == "sparql":
            return self._search_sparql(query, spec)

        logger.warning("Unknown search mode %r for provider %s", mode, self.id_prefix)
        return []

    def _search_server(self, query: str, spec: SearchConfig) -> list[dict]:
        url = spec.url.format(base_url=self.base_url, query=quote(query))
        doc = fetch_json(url) or {}
        items = self._jp(spec.items_path, doc) or []
        return self._items_to_options(items, spec)

    def _search_client_filter(self, query: str, spec: SearchConfig) -> list[dict]:
        url = spec.url.format(base_url=self.base_url, query=quote(query))
        doc = fetch_json(url) or {}
        items = self._jp(spec.items_path, doc) or []
        q = query.lower()
        fpaths = spec.filter_any_paths or []
        filtered: list[Any] = []
        for it in items:
            if any(self._contains(it, p, q) for p in fpaths):
                filtered.append(it)
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

    # -------------------------------
    # DETAIL
    # -------------------------------
    def detail(self, remote_id: str) -> dict:
        doc: dict[str, Any] = {}
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
