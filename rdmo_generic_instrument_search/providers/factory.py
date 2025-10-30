from __future__ import annotations

from django.utils.module_loading import import_string

from rdmo_generic_instrument_search.config_utils import load_config_from_settings

from .recipe import DetailStep, RecipeInstrumentProvider, SearchSpec, TransformSpec

PROVIDER_REGISTRY = {
    "GenericRecipeProvider": RecipeInstrumentProvider,
}
CONFIG_KEY = "InstrumentsOptionSetProvider"


def _resolve(name: str, dotted: str | None):
    return import_string(dotted) if dotted else PROVIDER_REGISTRY.get(name, RecipeInstrumentProvider)


def build_providers() -> dict[str, object]:
    cfg = load_config_from_settings().get("InstrumentsOptionSetProvider", {}) or {}
    result: dict[str, object] = {}
    for class_name, entries in (cfg.get("providers") or {}).items():
        for raw in entries:
            data = dict(raw)
            dotted = data.pop("class", None)
            cls = _resolve(class_name, dotted)
            prov_kwargs = {k: v for k, v in data.items() if k not in ("search", "detail", "transforms")}
            prov = cls(**prov_kwargs)
            # recipe wiring
            if isinstance(prov, RecipeInstrumentProvider):
                search = data.get("search")
                if search:
                    prov.search_spec = SearchSpec.from_dict(search)
                detail = data.get("detail", {})
                prov.detail_steps = [
                    DetailStep(url=st["url"], merge_included=bool(st.get("merge_included")), assign=st.get("assign"))
                    for st in detail.get("steps", [])
                ]
                prov.transforms = [
                    TransformSpec(dotted=t["dotted"], kwargs=t.get("kwargs")) for t in (detail.get("transforms") or [])
                ]
            result[prov.id_prefix] = prov
    return result
