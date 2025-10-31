from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import quote

from django.conf import settings

from rdmo_generic_instrument_search.client import _sparql_post_json, fetch_json

from .base import BaseInstrumentProvider  # jmespath helper via self._jp

logger = logging.getLogger("rdmo.generic_search.providers")


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
    def from_dict(cls, data) -> SearchConfig:
        return cls(
            mode=data["mode"],
            url=data.get("url", ""),
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
            raise ValueError("FetchStep.from_dict requires a mapping")
        return cls(
            url=data["url"],
            merge_included=bool(data.get("merge_included")),
            assign=data.get("assign"),
        )


@dataclass(slots=True)
class Transform:
    dotted: str  # "pkg.mod:func" or "pkg.mod.func"
    kwargs: dict[str, Any] | None = None

    @classmethod
    def from_dict(cls, data: dict | None) -> Transform:
        if not data:
            raise ValueError("Transform.from_dict requires a mapping")
        return cls(
            dotted=data["dotted"],
            kwargs=data.get("kwargs"),
        )


# -------------------------------
# Provider
# -------------------------------


@dataclass(slots=True)
class InstrumentSearchProvider(BaseInstrumentProvider):
    # These can be provided as dicts (legacy) or as dataclasses (preferred).
    search_config: SearchConfig | dict | None = field(default=None, repr=False)
    fetch_steps: list[FetchStep] | list[dict] | None = field(default=None, repr=False)
    transforms: list[Transform] | list[dict] | None = field(default=None, repr=False)

    @classmethod
    def from_dict(cls, data) -> InstrumentSearchProvider:
        search = data.get("search")
        steps = data.get("steps") or []
        transforms = data.get("transforms") or []

        return cls(
            id_prefix=data.get("id_prefix"),
            base_url=data.get("base_url"),
            text_prefix=data.get("text_prefix"),
            max_hits=data.get("max_hits") or 10,
            search_url=data.get("search_url"),
            search_items_path=data.get("search_items_path"),
            search_id_path=data.get("search_id_path"),
            search_label_path=data.get("search_label_path"),
            search_config=SearchConfig.from_dict(search) or search,
            fetch_steps=[FetchStep.from_dict(step) for step in steps] or None,
            transforms=[Transform.from_dict(t) for t in transforms] or None,
        )

    # -------------------------------
    # SEARCH
    # -------------------------------
    def search(self, query: str) -> list[dict]:
        spec = self._ensure_search_spec(self.search_config)
        if not query or not spec:
            return []

        mode = (spec.mode or "server").lower()

        try:
            if mode == "server":
                return self._search_server(query, spec)

            if mode == "client_filter":
                return self._search_client_filter(query, spec)

            if mode == "sparql":
                return self._search_sparql(query, spec)
        except (KeyError, TypeError) as e:
            raise e from e

        logger.warning("Unknown search mode %r for provider %s", mode, self.id_prefix)
        return []

    def _search_server(self, query: str, spec: SearchConfig) -> list[dict]:
        if not spec.url:
            return []
        url = spec.url.format(base_url=self.base_url, query=quote(query))
        doc = fetch_json(url)
        items = self._jp(spec.items_path, doc) or []
        return self._map_items_to_options(items, spec)

    def _search_client_filter(self, query: str, spec: SearchConfig) -> list[dict]:
        if not spec.url:
            return []
        url = spec.url.format(base_url=self.base_url, query=quote(query))
        doc = fetch_json(url)
        items = self._jp(spec.items_path, doc) or []
        q = query.lower()
        fpaths = spec.filter_any_paths or []
        filtered = []
        for it in items:
            if any(self._matches_query_at_path(it, p, q) for p in fpaths):
                filtered.append(it)
            if len(filtered) >= self.max_hits:
                break
        return self._map_items_to_options(filtered, spec)

    def _search_sparql(self, query: str, spec: SearchConfig) -> list[dict]:
        """
        Execute a SPARQL query (e.g. Wikidata).
        Required: spec.endpoint, spec.query
        Optional: spec.lang, spec.root_qid, items_path/id_path/label_path/label_template
        """
        if not spec.endpoint or not spec.query:
            logger.warning("SPARQL search requires endpoint and query in SearchConfig")
            return []

        lang = spec.lang or getattr(settings, "LANGUAGE_CODE", "en") or "en"
        root_qid = spec.root_qid or ""

        sparql = spec.query.replace("{query}", query).replace("{lang}", lang).replace("{root_qid}", root_qid)

        rows = _sparql_post_json(spec.endpoint, sparql)
        items = self._jp(spec.items_path or "results.bindings", rows) or []
        return self._map_items_to_options(items, spec, normalize_wikidata_ids=True)

    # -------------------------------
    # DETAIL
    # -------------------------------
    def detail(self, remote_id: str) -> dict:
        steps = self._ensure_detail_steps(self.fetch_steps)
        transforms = self._ensure_transforms(self.transforms)

        doc: dict[str, Any] = {}

        for step in steps:
            url = step.url.format(base_url=self.base_url, id=remote_id)
            part = fetch_json(url) or {}

            if step.merge_included:
                doc["included"] = [*doc.get("included", []), *part.get("included", [])]

            # assign branch: stash under a temporary key (e.g. "_contacts")
            if step.assign:
                doc[step.assign] = part
                continue

            # default: shallow merge into root
            for k, v in part.items():
                if k == "included" and not step.merge_included:
                    doc["included"] = [*doc.get("included", []), *v]
                else:
                    doc[k] = v

        for t in transforms:
            fn = self._import_callable(t.dotted)
            try:
                doc = fn(doc, **(t.kwargs or {})) or doc
            except Exception as e:
                logger.warning("Transform %s failed: %s", t.dotted, e)
        return doc

    # -------------------------------
    # Helpers
    # -------------------------------
    def _map_items_to_options(self, items: list, spec: SearchConfig, *, normalize_wikidata_ids: bool = False) -> list[dict]:
        out: list[dict] = []
        id_path = spec.id_path
        label_path = spec.label_path
        label_tpl = spec.label_template or "{label}"

        if not id_path:
            return out

        for it in items[: self.max_hits]:
            rid = self._jp(id_path, it)
            lbl = self._jp(label_path, it) if label_path else None
            if rid is None:
                continue

            # keep ids printable & stable
            if isinstance(rid, (int, float)):
                rid = str(rid)

            if normalize_wikidata_ids and isinstance(rid, str):
                rid = self._qid_from_entity_iri(rid)

            prefix = (self.text_prefix or "").strip()
            # template stays flexible; missing tokens become empty string
            text = label_tpl.format(
                prefix=prefix,
                label=lbl or "",
                code=self._jp("Instrument.code", it) or "",
            ).strip()

            out.append({"id": f"{self.id_prefix}:{rid}", "text": text or f"{self.id_prefix}:{rid}"})
        return out

    def _matches_query_at_path(self, item: dict, path: str, q: str) -> bool:
        val = self._jp(path, item)
        if val is None:
            return False
        return q in str(val).lower()

    @staticmethod
    def _qid_from_entity_iri(value: str) -> str:
        # e.g. "http://www.wikidata.org/entity/Q123" → "Q123"
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

    # ----- coercion helpers (accept dicts or dataclasses) -----

    @staticmethod
    def _ensure_search_spec(spec: SearchConfig | dict | None) -> SearchConfig | None:
        if spec is None:
            return None
        if isinstance(spec, SearchConfig):
            return spec
        if isinstance(spec, dict):
            return SearchConfig.from_dict(spec)
        raise TypeError(f"Unsupported search_spec type: {type(spec)!r}")

    @staticmethod
    def _ensure_detail_steps(steps: list[FetchStep] | list[dict] | None) -> list[FetchStep]:
        if not steps:
            return []
        if isinstance(steps, list) and all(isinstance(s, FetchStep) for s in steps):
            return steps  # already typed
        out: list[FetchStep] = []
        for step in steps:  # type: ignore[assignment]
            out.append(FetchStep.from_dict(step))
        return out

    @staticmethod
    def _ensure_transforms(transforms: list[Transform] | list[dict] | None) -> list[Transform]:
        if not transforms:
            return []
        if isinstance(transforms, list) and all(isinstance(t, Transform) for t in transforms):
            return transforms
        out: list[Transform] = []
        for transform in transforms:  # type: ignore[assignment]
            out.append(Transform.from_dict(transform))
        return out
