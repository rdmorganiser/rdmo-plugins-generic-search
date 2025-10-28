from django.apps import AppConfig

from .client import get_user_agent
from .config_utils import load_config_from_settings


class InstrumentSearchConfig(AppConfig):
    name = "rdmo_generic_instrument_search"
    label = "rdmo_generic_instrument_search"
    verbose_name = "Instrument Search OptionSet Provider Plugin"
    config = None

    def ready(self):
        self.config = load_config_from_settings()
        self.user_agent = get_user_agent(self.config)

        from .signals import signal_handlers  # noqa: F401
