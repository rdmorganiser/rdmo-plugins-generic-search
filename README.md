# RDMO Instrument search optionset provider

This optionset provider allows you to query several instrument databases at the
same time. Additional questions can be filled in automatically with information
from the instrument database entries. To use this feature an attribute mapping must be
configured.

The following instrument databases are currently implemented:
    -...
For every integration it is possible to define multiple instances in the
configuration. This is especially necessary for the Sensor Management System
(SMS), since there are four productive instances.

This plugin is based on the [RDMO Sensor AWI option set plugin](https://github.com/hafu/rdmo-sensor-awi)
with a complete refactoring, to allow configuration and easy extension with
more registries if needed.

## Setup

Install the plugin in your RDMO virtual environment using pip (directly from
GitHub):

```bash
pip install git+https://github.com/rdmorganiser/rdmo-plugins-generic-search
```

Or when editing the code you can put the code a folder beneath your RDMO
installation and install it with:

```bash
pip install -e ../rdmo-plugins-generic-search
```

Add the plugin to the `OPTIONSET_PROVIDERS` in `config/settings/local.py`:

```python
OPTIONSET_PROVIDERS = [
    ('instrument_search', _('Instrument search'), 'rdmo_generic_instrument_search.providers.GenericSearchProvider'),
]
```

Add the plugin to the `INSTALLED_APPS` in `config/settings/local.py`:

```python
INSTALLED_APPS = ['rdmo_generic_instrument_search'] + INSTALLED_APPS
```

After restarting RDMO, the `Instrument search` should be selectable as a provider
option for optionsets.

## Configuration

`config.toml` declares both providers (which perform the searches) and
handlers (which map detail documents back into RDMO attributes). The
`GenericSearchProvider` aggregates the configured providers and dispatches the
correct handler based on the `id_prefix` stored in `external_id`.

The configuration file defaults to `rdmo_generic_instrument_search/config.toml`.
Override the location with `INSTRUMENT_SEARCH_PROVIDER_CONFIG_FILE_PATH`
in `config/settings/local.py` or via an environment variable with the same
name. A custom file name can be set with
`INSTRUMENT_SEARCH_PROVIDER_CONFIG_FILE_NAME`.

```python
INSTRUMENT_SEARCH_PROVIDER_CONFIG_FILE_PATH = BASE_DIR / 'plugins' / 'instrument_search_config.toml'
```

### Configuration structure

The new schema is rooted at `[generic_search]` and is split into
configuration for search behaviour, handlers, and providers:

```toml
[generic_search]
min_search_len = 3
max_total_hits = 50

[generic_search.defaults.recipe]
max_hits = 10
lang = "en"

[generic_search.handlers.example]
catalog_uri = "http://example.com/terms/questions/test-instrument-search"
auto_complete_field_uri = "http://example.com/terms/domain/search/instrument/example"
[generic_search.handlers.example.attribute_mapping]
"longName"     = "http://example.com/terms/domain/search/instrument/example/long-name"
"serialNumber" = "http://example.com/terms/domain/search/instrument/example/serial"

[[generic_search.providers]]
id_prefix   = "example"
engine      = "recipe"
text_prefix = "Example:"
base_url    = "https://example.com/api/v1/instruments"

[generic_search.providers.search]
mode       = "server"
url        = "{base_url}/search?q={query}"
items_path = "data"
id_path    = "id"
label_path = "attributes.long_name || attributes.short_name"

[generic_search.providers.detail]
steps = [
  { url = "{base_url}/items/{id}" }
]

[generic_search.providers.handler]
ref = "example"
```

- `[generic_search]` – global settings, such as search length thresholds and
  the optional `defaults.recipe` block that applies to every recipe-based
  provider.
- `[generic_search.handlers.<id_prefix>]` – handler definitions keyed by the
  same `id_prefix` the provider uses. Each handler maps detail responses (via
  [JMESPath](https://jmespath.org/)) to attribute URIs within a catalog.
- `[[generic_search.providers]]` – a list of providers. Each provider must
  define an `id_prefix`, `engine` (currently only `"recipe"`), and how to
  perform searches and fetch detail documents. The optional
  `[generic_search.providers.handler]` block documents which handler should be
  used for the provider's results.

Providers support multiple search modes (`server`, `client_filter`, `sparql`,
or `wikidata_action`) and a `detail` pipeline composed of one or more HTTP
steps. See `rdmo_generic_instrument_search/config.toml` for more elaborate
examples covering Wikidata, NOMAD, PIDINST, and additional registries.
