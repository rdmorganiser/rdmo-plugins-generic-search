# rdmo_generic_instrument_search/providers/local_index.py
from __future__ import annotations

import json
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .base import BaseInstrumentProvider

logger = logging.getLogger("rdmo.generic_search.providers.local_index")


@dataclass(kw_only=True, slots=True)
class LocalIndexProvider(BaseInstrumentProvider):
    """
    Generic provider that searches a local JSON index and returns detail
    documents directly from that index.

    Expected config (TOML):

      [[generic_search.providers]]
      id_prefix   = "pidinst"
      engine      = "rdmo_generic_instrument_search.providers.local_index:LocalIndexProvider"
      text_prefix = "PIDINST:"
      base_url    = ""  # unused

      [generic_search.providers.search]
      path             = "/srv/rdmo/pidinst/results.json"
      items_path       = "@"
      id_path          = "pid"
      label_path       = "name"
      label_template   = "{prefix} {label}"
      filter_any_paths = ["pid", "name", "owner", "manufacturer"]

      [generic_search.providers.detail]
      transforms = [
        { dotted = "rdmo_generic_instrument_search.providers.transforms.pidinst:normalize_pidinst_record" }
      ]

    It follows the same general shape as the "recipe" providers:
      - search.* defines how to turn raw items -> {id,text}
      - detail.transforms is a list of callables(doc) -> doc.
    """

    # search config
    path: str
    items_path: str = "@"
    id_path: str = "id"
    label_path: str = "label"
    label_template: str = "{prefix} {label}"
    filter_any_paths: list[str] = field(default_factory=list)

    # detail config
    transforms: list[Callable[[dict], dict]] = field(default_factory=list, repr=False)

    # internals
    _items: list[dict[str, Any]] | None = field(default=None, init=False, repr=False)
    _by_id: dict[str, dict[str, Any]] = field(default_factory=dict, init=False, repr=False)

    # ------------------------------------------------------------------ #
    # Construction from config
    # ------------------------------------------------------------------ #
    @classmethod
    def from_dict(cls, cfg: dict[str, Any]) -> LocalIndexProvider:
        """
        Build from one [[generic_search.providers]] entry.
        Mirrors what RecipeInstrumentProvider.from_dict does, but without HTTP.
        """
        search = cfg.get("search") or {}
        detail_cfg = cfg.get("detail") or {}

        path = search.get("path") or search.get("url")
        if not path:
            raise ValueError(f"[{cfg.get('id_prefix')}] search.path (or url) is required for local index provider")

        # transforms: [{ dotted = "module:func", kwargs = {...}? }]
        transforms: list[Callable[[dict], dict]] = []
        for t in (detail_cfg.get("transforms") or []):
            dotted = t.get("dotted")
            if not dotted:
                continue
            fn = cls._import_callable(dotted)
            transforms.append(fn)

        return cls(
            id_prefix=cfg["id_prefix"],
            base_url=cfg.get("base_url", ""),
            text_prefix=cfg.get("text_prefix", ""),
            max_hits=int(cfg.get("max_hits") or cfg.get("search", {}).get("max_hits", 10)),
            path=path,
            items_path=search.get("items_path", "@"),
            id_path=search["id_path"],
            label_path=search.get("label_path") or search["id_path"],
            label_template=search.get("label_template", "{prefix} {label}"),
            filter_any_paths=list(search.get("filter_any_paths") or []),
            transforms=transforms,
        )

    # ------------------------------------------------------------------ #
    # Public API used by GenericSearchProvider + GenericDetailHandler
    # ------------------------------------------------------------------ #
    def search(self, query: str) -> list[dict]:
        q = (query or "").strip().lower()
        if not q:
            return []

        items = self._load_index()
        out: list[dict[str, str]] = []

        for item in items:
            if not self._matches_item(item, q):
                continue

            remote_id = self._extract_id(item)
            if not remote_id:
                continue

            label = self._extract_label(item, remote_id)
            text = self._render_option_text(label, remote_id)

            out.append({"id": f"{self.id_prefix}:{remote_id}", "text": text})
            if len(out) >= self.max_hits:
                break

        return out

    def detail(self, remote_id: str) -> dict:
        """Return the full document (optionally transformed) for the given id (without prefix)."""
        if not remote_id:
            return {}

        items = self._load_index()
        # lazy index by id
        if not self._by_id and items:
            for item in items:
                rid = self._extract_id(item)
                if rid:
                    self._by_id[rid] = item

        doc = self._by_id.get(remote_id)
        if not doc:
            logger.debug("local-index: no document found for %s:%s", self.id_prefix, remote_id)
            return {}

        # copy shallowly so transforms can mutate
        out: dict[str, Any] = dict(doc)
        for fn in self.transforms:
            try:
                out = fn(out) or out
            except Exception as exc:
                logger.warning("local-index: transform %r failed: %s", fn, exc)

        return out

    # ------------------------------------------------------------------ #
    # Internals
    # ------------------------------------------------------------------ #
    def _load_index(self) -> list[dict[str, Any]]:
        if self._items is not None:
            return self._items

        path = Path(self.path)
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except OSError as exc:
            logger.error("local-index: cannot read %s: %s", path, exc)
            self._items = []
            return self._items
        except json.JSONDecodeError as exc:
            logger.error("local-index: invalid JSON in %s: %s", path, exc)
            self._items = []
            return self._items

        # items_path is a JMESPath expression over the whole JSON structure
        items = self._jp(self.items_path, raw) if self.items_path else raw
        if items is None:
            items = []
        if not isinstance(items, list):
            logger.warning("local-index: items_path %r did not yield a list, got %r", self.items_path, type(items))
            items = []

        self._items = items
        logger.info("local-index: loaded %d item(s) from %s", len(self._items), path)
        return self._items

    def _extract_id(self, item: dict[str, Any]) -> str | None:
        val = self._jp(self.id_path, item)
        if val is None:
            return None
        return str(val).strip()

    def _extract_label(self, item: dict[str, Any], fallback: str) -> str:
        val = self._jp(self.label_path, item)
        if val is None or val == "":
            return fallback
        return str(val).strip()

    def _render_option_text(self, label: str, remote_id: str) -> str:
        tpl = self.label_template or "{prefix} {label}"
        data = {"prefix": (self.text_prefix or "").strip(), "label": label, "id": remote_id}
        try:
            txt = tpl.format(**data)
        except Exception:
            txt = f"{self.text_prefix} {label}"
        return txt.strip()

    def _matches_item(self, item: dict[str, Any], q: str) -> bool:
        """
        Very simple substring OR search over the configured filter_any_paths.
        If none are configured, fall back to id_path + label_path.
        """
        paths = self.filter_any_paths or [self.id_path, self.label_path]
        for path in paths:
            val = self._jp(path, item)
            if isinstance(val, list):
                values = val
            else:
                values = [val]
            for v in values:
                if v is None:
                    continue
                if q in str(v).lower():
                    return True
        return False

    @staticmethod
    def _import_callable(dotted: str) -> Callable:
        if ":" in dotted:
            mod, fn = dotted.split(":", 1)
        else:
            parts = dotted.split(".")
            mod, fn = ".".join(parts[:-1]), parts[-1]
        from importlib import import_module

        return getattr(import_module(mod), fn)
