import logging

from django.apps import apps
from django.conf import settings

import requests

from rdmo import __version__

logger = logging.getLogger(__name__)


def fetch_json(url: str) -> dict | list:
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
