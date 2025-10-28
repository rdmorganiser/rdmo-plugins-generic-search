# rdmo_generic_instrument_search/providers/recipe.py
from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

from rdmo_generic_instrument_search.client import fetch_json

from .base import BaseInstrumentProvider  # your thin base with jmespath helper

logger = logging.getLogger(__name__)


@dataclass
class SearchSpec:
    mode: str  # "server" | "client_filter"
    url: str  # e.g. "{base_url}/devices?q={query}"
    items_path: str | None = None  # jmespath to list (both modes)
    id_path: str | None = None  # jmespath per item
    label_path: str | None = None  # jmespath per item
    label_template: str | None = None  # f"{prefix} {code}" (optional)
    # client_filter only:
    filter_any_paths: list[str] | None = None  # list of jmespaths inside item to check .lower().contains(query)


@dataclass
class DetailStep:
    url: str
    merge_included: bool = False  # append step['included'] into doc['included']
    assign: str | None = None  # jmespath assign target ("$" for root). Here we use simple root merges.


@dataclass
class TransformSpec:
    dotted: str  # dotted callable "pkg.mod:func" or "pkg.mod.func"
    kwargs: dict[str, Any] | None = None


class RecipeInstrumentProvider(BaseInstrumentProvider):
    # injected from config
    search_spec: SearchSpec | None = None
    detail_steps: list[DetailStep] | None = None
    transforms: list[TransformSpec] | None = None

    def search(self, query: str) -> list[dict]:
        if not self.search_spec or not query:
            return []

        spec = self.search_spec
        url = spec.url.format(base_url=self.base_url, query=quote(query))

        if spec.mode == "server":
            doc = fetch_json(url)
            items = self._jp(spec.items_path, doc) or []
            return self._items_to_options(items, spec)
        elif spec.mode == "client_filter":
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
        else:
            logger.warning("Unknown search mode: %s", spec.mode)
            return []

    def detail(self, remote_id: str) -> dict:
        doc: dict[str, Any] = {}
        for step in self.detail_steps or []:
            part = fetch_json(step.url.format(base_url=self.base_url, id=remote_id)) or {}
            if step.merge_included:
                doc["included"] = [*doc.get("included", []), *part.get("included", [])]
            # naive merge at root (good enough for our 3 cases)
            for k, v in part.items():
                if k == "included" and not step.merge_included:
                    doc["included"] = [*doc.get("included", []), *v]
                else:
                    doc[k] = v

        # optional post-processing (e.g., O2A parameter→unit join)
        for t in self.transforms or []:
            fn = self._import_callable(t.dotted)
            try:
                doc = fn(doc, **(t.kwargs or {})) or doc
            except Exception as e:
                logger.warning("Transform %s failed: %s", t.dotted, e)
        return doc

    # ---- helpers ----
    def _items_to_options(self, items: list, spec: SearchSpec) -> list[dict]:
        out: list[dict] = []
        for it in items[: self.max_hits]:
            rid = self._jp(spec.id_path, it)
            lbl = self._jp(spec.label_path, it)
            if not rid or (not lbl and not spec.label_template):
                continue
            text = (
                (spec.label_template or "{label}")
                .format(
                    prefix=(self.text_prefix or "").strip(),
                    label=lbl,
                    code=self._jp("Instrument.code", it) or "",  # convenience for GFZ
                )
                .strip()
            )
            out.append({"id": f"{self.id_prefix}:{rid}", "text": text})
        return out

    def _contains(self, item: dict, path: str, q: str) -> bool:
        val = self._jp(path, item)
        if val is None:
            return False
        return q in str(val).lower()

    @staticmethod
    def _import_callable(dotted: str) -> Callable:
        if ":" in dotted:
            mod, fn = dotted.split(":")
        else:
            parts = dotted.split(".")
            mod, fn = ".".join(parts[:-1]), parts[-1]
        from importlib import import_module

        return getattr(import_module(mod), fn)
