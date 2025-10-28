from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from rdmo.options.providers import Provider

from rdmo_generic_instrument_search.config_utils import load_config_from_settings
from rdmo_generic_instrument_search.providers.factory import build_providers

logger = logging.getLogger(__name__)

CONFIG_KEY = "InstrumentsOptionSetProvider"

_PROVIDERS_BY_PREFIX = build_providers()
_ALL_PROVIDERS = list(_PROVIDERS_BY_PREFIX.values())


class InstrumentOptionsetProvider(Provider):
    """RDMO optionset provider that fans out search to all configured backends."""

    search = True
    refresh = True

    def get_options(self, project, search: str | None = None, user=None, site=None) -> list[dict]:
        cfg = load_config_from_settings().get(CONFIG_KEY, {}) or {}
        min_len = int(cfg.get("min_search_len", 3))
        if not search or len(search) < min_len:
            return []

        results: list[dict] = []
        seen_ids: set[str] = set()

        # modest thread pool; these are network-bound calls
        max_workers = min(6, max(1, len(_ALL_PROVIDERS)))
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            fut = {ex.submit(p.search, search): p for p in _ALL_PROVIDERS}
            for f in as_completed(fut):
                prov = fut[f]
                try:
                    opts = f.result() or []
                except Exception as exc:
                    logger.warning("Provider %s.search failed: %s", prov.__class__.__name__, exc)
                    continue
                for opt in opts:
                    oid = str(opt.get("id"))
                    if oid and oid not in seen_ids:
                        seen_ids.add(oid)
                        results.append({"id": oid, "text": str(opt.get("text", oid))})
        return results
