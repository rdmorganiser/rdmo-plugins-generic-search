import logging
import os
import sys
from functools import lru_cache
from pathlib import Path
from typing import Any

from django.conf import settings

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

logger = logging.getLogger(__name__)


def _get_config_path() -> Path:
    try:
        config_file_path = settings.INSTRUMENT_SEARCH_PROVIDER_CONFIG_FILE_PATH
    except AttributeError:
        config_file_path = None

    config_file_path = os.getenv("INSTRUMENT_SEARCH_PROVIDER_CONFIG_FILE_PATH", config_file_path)

    if config_file_path is None or not Path(config_file_path).is_file():
        config_file_name = getattr(settings, "INSTRUMENT_SEARCH_PROVIDER_CONFIG_FILE_NAME", "config.toml")
        config_file_name = os.getenv("INSTRUMENT_SEARCH_PROVIDER_CONFIG_FILE_NAME", config_file_name)
        config_file_path = Path(__file__).parent / config_file_name

    return Path(config_file_path).resolve()


@lru_cache(maxsize=1)
def _load_config_with_mtime(path: Path, mtime: float) -> dict[str, Any]:
    """Load and parse the config file. Cached per (path, mtime)."""
    logger.debug("Loading configuration from %s", path)
    try:
        with open(path, "rb") as config_file:
            return tomllib.load(config_file)
    except (FileNotFoundError, PermissionError) as e:
        logger.error("Cannot open configuration file: %s", path)
        raise e from e
    except tomllib.TOMLDecodeError as e:
        logger.error("Failed to decode configuration file: %s", path)
        raise e from e


def load_config_from_settings() -> dict[str, Any]:
    """
    Load configuration file and cache it.
    Automatically reload if file modification time changes.
    """
    path = _get_config_path()
    try:
        mtime = path.stat().st_mtime
    except FileNotFoundError:
        mtime = 0.0  # force reload if file missing

    return _load_config_with_mtime(path, mtime)
