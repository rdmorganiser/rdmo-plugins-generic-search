from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any

from rdmo.options.providers import Provider

from rdmo_generic_instrument_search.config_utils import load_config_from_settings
from rdmo_generic_instrument_search.providers.factory import build_providers

logger = logging.getLogger("rdmo.generic_search.meta")

CONFIG_ROOT = "generic_search"


@dataclass(slots=True)
class SearchContext:
    max_workers: int
    max_total_hits: int | None
    sort_key: str | None
    min_search_len: int


class GenericSearchProvider(Provider):
    """
    RDMO Provider: fanned-out search across config-defined providers.
    """

    search = True
    refresh = True

    def __init__(self, key: str, label: str, class_name: str) -> None:
        super().__init__(key, label, class_name)

        config = load_config_from_settings().get(CONFIG_ROOT, {}) or {}

        self._providers_by_prefix = build_providers()
        self._providers = list(self._providers_by_prefix.values())

        self.search_context = SearchContext(
            max_workers=int(config.get("max_workers") or min(8, len(self._providers) or 1)),
            max_total_hits=(int(config["max_total_hits"]) if "max_total_hits" in config else None),
            sort_key=(config.get("sort_key") or None),
            min_search_len=int(config.get("min_search_len") or 0),
        )

    def get_options(self, project, search: str | None = None, user=None, site=None) -> list[dict]:
        query = (search or "").strip()
        if not query or len(query) < self.search_context.min_search_len:
            return []

        if not self._providers:
            logger.info("generic-search: no providers configured; returning empty list")
            return []

        t0 = time.perf_counter()
        logger.info(
            "generic-search: start query=%r providers=%d workers=%d", query, len(self._providers), self.search_context.max_workers
        )

        results: list[dict[str, str]] = []
        seen: set[str] = set()

        with ThreadPoolExecutor(max_workers=self.search_context.max_workers) as pool:
            futures = {pool.submit(self._search_one, p, query): p for p in self._providers}

            for fut in as_completed(futures):
                prov = futures[fut]
                prefix = getattr(prov, "id_prefix", "?")
                try:
                    opts = fut.result() or []
                except Exception as exc:
                    logger.warning("generic-search: provider=%s failed: %s", prefix, exc)
                    continue

                added = 0
                for opt in opts:
                    oid = str(opt.get("id"))
                    if oid and oid not in seen:
                        seen.add(oid)
                        results.append({"id": oid, "text": str(opt.get("text", oid))})
                        added += 1
                # logger.debug("generic-search: provider=%s added=%d total=%d", prefix, added, len(results))

                if self.search_context.max_total_hits is not None and len(results) >= self.search_context.max_total_hits:
                    for f2 in futures:
                        if f2 is not fut:
                            f2.cancel()
                    break

        if self.search_context.sort_key in {"id", "text"}:
            results.sort(key=lambda r: (r.get(self.search_context.sort_key) or "").lower())

        dur_ms = int((time.perf_counter() - t0) * 1000)
        logger.info(
            "generic-search: done query=%r providers=%d results=%d duration_ms=%d",
            query,
            len(self._providers),
            len(results),
            dur_ms,
        )
        return results

    @staticmethod
    def _search_one(provider: Any, query: str) -> list[dict]:
        t0 = time.perf_counter()
        opts: list[dict] = provider.search(query) or []
        dt = int((time.perf_counter() - t0) * 1000)
        logger.debug("generic-search: provider=%s took_ms=%d hits=%d", getattr(provider, "id_prefix", "?"), dt, len(opts))
        return opts
