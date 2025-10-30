from __future__ import annotations

import logging
from dataclasses import dataclass, field
from urllib.parse import quote

from rdmo_generic_instrument_search.client import fetch_json

logger = logging.getLogger(__name__)


@dataclass(kw_only=True, slots=True)
class BaseInstrumentProvider:
    # required
    id_prefix: str
    base_url: str

    # optional
    text_prefix: str | None = field(default=None, repr=False)
    max_hits: int = field(default=10, repr=False)

    # Template hooks (stay optional; not required in __init__)
    search_url: str | None = field(default=None, repr=False)  # e.g. "{base_url}/devices?q={query}"
    search_items_path: str | None = field(default=None, repr=False)  # e.g. "data"
    search_id_path: str | None = field(default=None, repr=False)  # e.g. "id"
    search_label_path: str | None = field(default=None, repr=False)  # e.g. "attributes.long_name"

    def search(self, query: str) -> list[dict]:
        if not self.search_url:
            return []
        url = self.search_url.format(base_url=self.base_url, query=quote(query))
        doc = fetch_json(url)
        items = self._jp(self.search_items_path, doc) or []
        out: list[dict] = []
        for it in items[: self.max_hits]:
            rid = self._jp(self.search_id_path, it)
            label = self._jp(self.search_label_path, it)
            if not rid or not label:
                continue
            prefix = (self.text_prefix or "").strip()
            text = f"{prefix} {label}".strip() if prefix else str(label)
            out.append({"id": f"{self.id_prefix}:{rid}", "text": text})
        return out

    def detail(self, remote_id: str) -> dict:
        # subclasses or the recipe provider override this
        return {}

    @staticmethod
    def _jp(path: str | None, data: dict | list | None):
        if not path or data is None:
            return None
        # lazy import to avoid hard dep if path unused
        import jmespath

        return jmespath.search(path, data)
