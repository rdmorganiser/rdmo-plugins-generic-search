from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any

from rdmo.options.providers import Provider

from rdmo_generic_instrument_search.config_utils import load_config_from_settings
from rdmo_generic_instrument_search.providers.factory import build_providers

logger = logging.getLogger("rdmo.generic_search.providers")


@dataclass(slots=True)
class SearchContext:
    max_workers: int
    max_total_hits: int | None
    sort_key: str | None


class GenericSearchProvider(Provider):
    """
    RDMO-compatible meta provider.
    Delegates the query to all configured instrument providers (from TOML),
    merges & deduplicates the results, and returns [{'id','text'}, ...].

    Requirements (from rdmo.core.plugins / rdmo.options.providers):
    - __init__(key, label, class_name)
    - get_options(project, search=None, user=None, site=None)
    - class flags: .search (supports live search), .refresh (page refresh needed)
    """

    search = True
    refresh = True

    def __init__(self, key: str, label: str, class_name: str) -> None:
        super().__init__(key, label, class_name)

        cfg = load_config_from_settings().get(self.__class__.__qualname__, {}) or {}

        # Build concrete providers once per plugin instance.
        # The factory reads the same TOML section and returns a dict[prefix -> provider].
        self._providers_by_prefix = build_providers()
        self._providers = list(self._providers_by_prefix.values())

        # Runtime knobs (safe defaults; all optional in TOML)
        self._ctx = SearchContext(
            max_workers=int(cfg.get("max_workers") or min(8, len(self._providers) or 1)),
            max_total_hits=(int(cfg["max_total_hits"]) if "max_total_hits" in cfg else None),
            sort_key=cfg.get("sort_key"),  # e.g. "text" for deterministic output
        )

    # ---- RDMO entry point ---------------------------------------------------
    def get_options(self, project, search: str | None = None, user=None, site=None) -> list[dict]:
        """
        Fan out concurrently to all configured providers, dedupe by 'id',
        optionally cap results, optionally sort for deterministic output.
        """
        query = (search or "").strip()
        if not query:
            return []

        if not self._providers:
            logger.info("generic-search: no providers configured; returning empty list")
            return []

        start_t = time.perf_counter()
        logger.info("generic-search: start query=%r providers=%d workers=%d", query, len(self._providers), self._ctx.max_workers)

        results: list[dict[str, str]] = []
        seen_ids: set[str] = set()

        # modest thread pool; these are network-bound calls
        with ThreadPoolExecutor(max_workers=self._ctx.max_workers) as pool:
            futures = {pool.submit(self._search_one, p, query): p for p in self._providers}

            for fut in as_completed(futures):
                provider = futures[fut]
                prefix = getattr(provider, "id_prefix", "?")
                try:
                    options = fut.result() or []
                except Exception as exc:
                    logger.warning("generic-search: provider=%s failed: %s", prefix, exc)
                    continue

                added = 0
                for opt in options:
                    oid = str(opt.get("id"))
                    if oid and oid not in seen_ids:
                        seen_ids.add(oid)
                        results.append({"id": oid, "text": str(opt.get("text", oid))})
                        added += 1
                logger.debug("generic-search: provider=%s added=%d total=%d", prefix, added, len(results))

                # Optional early stop when a global cap is configured
                if self._ctx.max_total_hits is not None and len(results) >= self._ctx.max_total_hits:
                    # Best effort: cancel remaining tasks
                    for f2 in futures:
                        if f2 is not fut:
                            f2.cancel()
                    break

        # Optional deterministic sort (handy for tests/snapshots)
        if self._ctx.sort_key in {"id", "text"}:
            results.sort(key=lambda r: (r.get(self._ctx.sort_key) or "").lower())

        dur_ms = int((time.perf_counter() - start_t) * 1000)
        logger.info(
            "generic-search: done query=%r providers=%d results=%d duration_ms=%d",
            query,
            len(self._providers),
            len(results),
            dur_ms,
        )
        return results

    # ---- internals ----------------------------------------------------------
    @staticmethod
    def _search_one(provider: Any, query: str) -> list[dict]:
        """Execute a single provider call with timing + concise DEBUG logging."""
        t0 = time.perf_counter()
        opts: list[dict] = provider.search(query) or []
        dt = int((time.perf_counter() - t0) * 1000)
        logger.debug(
            "generic-search: provider=%s took_ms=%d hits=%d",
            getattr(provider, "id_prefix", "?"),
            dt,
            len(opts),
        )
        return opts
