import logging
from functools import cache

from django.conf import settings

import requests

from rdmo import __version__

logger = logging.getLogger(__name__)


def fetch_json(url: str) -> dict | list:
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


@cache
def get_user_agent(app_config) -> str:
    """
    Constructs a standardized user agent string for backend HTTP requests.
    Example:
        rdmo/1.2 Instrument Search Plugin (+https://rdmo.example.com; support@example.com)
    """
    base = f"rdmo/{__version__} Instrument Search Plugin"
    domain = getattr(app_config, "USER_AGENT_DOMAIN", None)
    contact = getattr(app_config, "USER_AGENT_CONTACT", getattr(settings, "DEFAULT_FROM_EMAIL", None))

    # Construct metadata in RFC 7231 style
    meta = []
    if domain:
        meta.append(f"https://{domain}")
    if contact:
        meta.append(contact)

    meta_str = f" (+{'; '.join(meta)})" if meta else ""
    return base + meta_str
