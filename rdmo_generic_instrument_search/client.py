import json
import logging
from pathlib import Path
from urllib.parse import urlparse

from django.apps import apps
from django.conf import settings
from django.contrib.staticfiles import finders

import requests

from rdmo import __version__

logger = logging.getLogger(__name__)


def fetch_json(url: str) -> dict | list:
    """
    Fetch JSON from either:
      * HTTP/HTTPS URLs (via requests)
      * Django staticfiles when using 'static://relative/path.json'
      * Local filesystem when using:
          - 'file:///abs/path/to/file.json'
          - plain paths like '/path/to/file.json' or 'data/file.json'
    """
    if not url:
        return {}

    # --- detect scheme once ---
    parsed = urlparse(url)

    # 1) static:// paths -> Django staticfiles
    if parsed.scheme == "static":
        rel_path = (parsed.netloc + parsed.path).lstrip("/")
        try:
            abs_path = finders.find(rel_path) or rel_path
        except Exception as e:  # pragma: no cover - defensive
            logger.error("Error resolving static path %s: %s", rel_path, e)
            return {"errors": [str(e)]}

        return _load_local_json(abs_path, source=f"static://{rel_path}")

    # 2) file:// paths -> filesystem
    if parsed.scheme == "file":
        # urlparse for file:///tmp/x.json -> path='/tmp/x.json'
        path = parsed.path or ""
        return _load_local_json(path, source=url)

    # 3) bare paths without scheme -> treat as local file
    if not parsed.scheme:
        return _load_local_json(url, source=url)

    # 4) everything else -> HTTP(S) via requests (old behaviour)
    logger.info("Fetching json from %s", url)
    try:
        response = requests.get(url, headers={"User-Agent": get_user_agent()})
        response.raise_for_status()
        logger.debug("Fetched data from %s: %s", url, response)
        json_data = response.json()
        if not json_data:
            logger.debug("Fetched data is empty %s: %s", url, response)
        return json_data
    except requests.exceptions.RequestException as e:
        logger.error("Request failed for %s: %s", url, e)
        return {"errors": [str(e)]}


def _load_local_json(path: str | Path, *, source: str) -> dict | list:
    """Helper: read JSON from local filesystem path."""
    path = Path(path)
    logger.info("Loading local JSON from %s (resolved=%s)", source, path)
    try:
        with path.open(encoding="utf-8") as fh:
            data = json.load(fh)
        if not data:
            logger.debug("Local JSON is empty from %s", path)
    except FileNotFoundError as e:
        logger.error("Local JSON not found: %s (%s)", path, e)
        return {"errors": [f"file not found: {path}"]}
    except json.JSONDecodeError as e:
        logger.error("Invalid JSON in %s: %s", path, e)
        return {"errors": [f"invalid json: {path}: {e}"]}
    except OSError as e:  # pragma: no cover - defensive
        logger.error("Error reading %s: %s", path, e)
        return {"errors": [str(e)]}
    else:
        return data

def _sparql_post_json(endpoint: str, query: str, *, timeout: float = 15.0) -> dict:
    """
    POST a SPARQL query; return JSON result.
    Uses a polite User-Agent via get_user_agent().
    """
    try:
        headers = {
            "User-Agent": get_user_agent(),
            "Accept": "application/sparql-results+json",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        }
        resp = requests.post(
            endpoint,
            data={"query": query},
            headers=headers,
            params={"format": "json"},
            timeout=timeout,
        )
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException as e:
        logger.warning("SPARQL error at %s: %s", endpoint, e)
        return {}


def get_user_agent(app_config: dict | object | None = None) -> str:
    """
    Build a standardized User-Agent. Sources (in priority):
      1) rdmo_generic_instrument_search AppConfig attributes
      2) plugin config dict (if provided)
      3) Django settings (DEFAULT_FROM_EMAIL)
    """
    base = f"rdmo/{__version__} Instrument Search Plugin"

    domain = None
    contact = None

    # 1) Try the AppConfig first
    try:
        cfg = apps.get_app_config("rdmo_generic_instrument_search")
        # allow both attributes on AppConfig or keys in its .config dict
        domain = getattr(cfg, "USER_AGENT_DOMAIN", None) or (getattr(cfg, "config", {}) or {}).get("USER_AGENT_DOMAIN")
        contact = getattr(cfg, "USER_AGENT_CONTACT", None) or (getattr(cfg, "config", {}) or {}).get("USER_AGENT_CONTACT")
    except LookupError:
        pass

    # 2) If an app_config was explicitly provided, let it override
    if app_config:
        if isinstance(app_config, dict):
            domain = app_config.get("USER_AGENT_DOMAIN", domain)
            contact = app_config.get("USER_AGENT_CONTACT", contact)
        else:
            domain = getattr(app_config, "USER_AGENT_DOMAIN", domain)
            contact = getattr(app_config, "USER_AGENT_CONTACT", contact)

    # 3) Fallback to settings for contact
    contact = contact or getattr(settings, "DEFAULT_FROM_EMAIL", None)

    meta = []
    if domain:
        meta.append(f"https://{domain}")
    if contact:
        meta.append(contact)

    return base + (f" (+{'; '.join(meta)})" if meta else "")
