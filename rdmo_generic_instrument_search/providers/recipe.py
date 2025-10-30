from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import quote

from django.conf import settings

from rdmo_generic_instrument_search.client import _sparql_post_json, fetch_json

from .base import BaseInstrumentProvider  # jmespath helper via self._jp

logger = logging.getLogger(__name__)


# -------------------------------
# Dataclasses for typed config
# -------------------------------


@dataclass
class SearchSpec:
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
    def from_dict(cls, data) -> SearchSpec:
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


@dataclass
class DetailStep:
    url: str
    merge_included: bool = False
    assign: str | None = None


@dataclass
class TransformSpec:
    dotted: str  # "pkg.mod:func" or "pkg.mod.func"
    kwargs: dict[str, Any] | None = None


# -------------------------------
# Provider
# -------------------------------


class RecipeInstrumentProvider(BaseInstrumentProvider):
    # These can be provided as dicts (legacy) or as dataclasses (preferred).
    search_spec: SearchSpec | dict | None = field(default=None, repr=False)
    detail_steps: list[DetailStep] | list[dict] | None = field(default=None, repr=False)
    transforms: list[TransformSpec] | list[dict] | None = field(default=None, repr=False)

    # -------------------------------
    # SEARCH
    # -------------------------------
    def search(self, query: str) -> list[dict]:
        spec = self._ensure_search_spec(self.search_spec)
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

    def _search_server(self, query: str, spec: SearchSpec) -> list[dict]:
        if not spec.url:
            return []
        url = spec.url.format(base_url=self.base_url, query=quote(query))
        doc = fetch_json(url)
        items = self._jp(spec.items_path, doc) or []
        return self._items_to_options(items, spec)

    def _search_client_filter(self, query: str, spec: SearchSpec) -> list[dict]:
        if not spec.url:
            return []
        url = spec.url.format(base_url=self.base_url, query=quote(query))
        doc = fetch_json(url)
        items = self._jp(spec.items_path, doc) or []
        q = query.lower()
        fpaths = spec.filter_any_paths or []
        filtered = []
        for it in items:
            if any(self._contains(it, p, q) for p in fpaths):
                filtered.append(it)
            if len(filtered) >= self.max_hits:
                break
        return self._items_to_options(filtered, spec)

    def _search_sparql(self, query: str, spec: SearchSpec) -> list[dict]:
        """
        Execute a SPARQL query (e.g. Wikidata).
        Required: spec.endpoint, spec.query
        Optional: spec.lang, spec.root_qid, items_path/id_path/label_path/label_template
        """
        if not spec.endpoint or not spec.query:
            logger.warning("SPARQL search requires endpoint and query in SearchSpec")
            return []

        lang = spec.lang or getattr(settings, "LANGUAGE_CODE", "en") or "en"
        root_qid = spec.root_qid or ""

        sparql = spec.query.replace("{query}", query).replace("{lang}", lang).replace("{root_qid}", root_qid)

        rows = _sparql_post_json(spec.endpoint, sparql)
        items = self._jp(spec.items_path or "results.bindings", rows) or []
        return self._items_to_options(items, spec, normalize_wikidata_ids=True)

    # -------------------------------
    # DETAIL
    # -------------------------------
    def detail(self, remote_id: str) -> dict:
        steps = self._ensure_detail_steps(self.detail_steps)
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
    def _items_to_options(self, items: list, spec: SearchSpec, *, normalize_wikidata_ids: bool = False) -> list[dict]:
        out: list[dict] = []
        id_path = spec.id_path
        label_path = spec.label_path
        label_tpl = spec.label_template or "{label}"

        for it in items[: self.max_hits]:
            rid = self._jp(id_path, it)
            lbl = self._jp(label_path, it)

            if not rid or (lbl is None and not label_tpl):
                continue

            if normalize_wikidata_ids and isinstance(rid, str):
                rid = self._qid_from_iri(rid)

            prefix = (self.text_prefix or "").strip()
            text = label_tpl.format(prefix=prefix, label=lbl or "", code=self._jp("Instrument.code", it) or "").strip()
            out.append({"id": f"{self.id_prefix}:{rid}", "text": text})
        return out

    def _contains(self, item: dict, path: str, q: str) -> bool:
        val = self._jp(path, item)
        if val is None:
            return False
        return q in str(val).lower()

    @staticmethod
    def _qid_from_iri(value: str) -> str:
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
    def _ensure_search_spec(spec: SearchSpec | dict | None) -> SearchSpec | None:
        if spec is None:
            return None
        if isinstance(spec, SearchSpec):
            return spec
        if isinstance(spec, dict):
            return SearchSpec(
                mode=spec.get("mode", "server"),
                url=spec.get("url"),
                items_path=spec.get("items_path"),
                id_path=spec.get("id_path"),
                label_path=spec.get("label_path"),
                label_template=spec.get("label_template"),
                filter_any_paths=spec.get("filter_any_paths"),
                endpoint=spec.get("endpoint"),
                query=spec.get("query"),
                lang=spec.get("lang"),
                root_qid=spec.get("root_qid"),
            )
        raise TypeError(f"Unsupported search_spec type: {type(spec)!r}")

    @staticmethod
    def _ensure_detail_steps(steps: list[DetailStep] | list[dict] | None) -> list[DetailStep]:
        if not steps:
            return []
        if isinstance(steps, list) and all(isinstance(s, DetailStep) for s in steps):
            return steps  # already typed
        # assume list[dict]
        out: list[DetailStep] = []
        for s in steps:  # type: ignore[assignment]
            out.append(
                DetailStep(
                    url=s["url"],
                    merge_included=bool(s.get("merge_included")),
                    assign=s.get("assign"),
                )
            )
        return out

    @staticmethod
    def _ensure_transforms(trs: list[TransformSpec] | list[dict] | None) -> list[TransformSpec]:
        if not trs:
            return []
        if isinstance(trs, list) and all(isinstance(t, TransformSpec) for t in trs):
            return trs
        out: list[TransformSpec] = []
        for t in trs:  # type: ignore[assignment]
            out.append(TransformSpec(dotted=t["dotted"], kwargs=t.get("kwargs")))
        return out
