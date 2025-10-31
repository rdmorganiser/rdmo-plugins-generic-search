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

Install the plugins in your RDMO virtual environment using pip (directly from
GitHub):

```bash
pip install git+https://github.com/rdmorganiser/rdmo-plugins-sensorsearch
```

Or when editing the code you can put the code a folder beneath your RDMO
installation and install it with:

```bash
pip install -e ../rdmo-plugins-sensorsearch
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

With `config.toml` the providers which should be used can be configured. The
`GenericSearchProvider` aggregates the results of the configured providers.

To automatically fill out questions with results of the matching sensor,
attribute mapping for the specific catalog(s) must be configured in the
configuration file.

The configuration file default location is inside the directory of the plugin.
The location can be overwritten with `INSTRUMENT_SEARCH_PROVIDER_CONFIG_FILE_PATH`
in the in `config/settings/local.py` or as environment variable with the same
name.

```python
INSTRUMENT_SEARCH_PROVIDER_CONFIG_FILE_PATH = BASE_DIR / 'plugins' / 'instrument_search_config.toml'
```

### Configuration: Providers

```toml
[GenericSearchProvider]
min_search_len = 3


[[GenericSearchProvider.providers.ExampleInstrumentProvider]]
id_prefix = "example"
text_prefix = "Example: "
base_url = "https://example.com/api/v1/instruments"
```

A `base_url` for every instance must be set. In addition, the
`text_prefix` and `id_prefix` is configured. The `text_prefix` is displayed
as a prefix to the search result, so that the user can identify the correct source.
The `id_prefix` is used internally, to prefix the id which is saved
along the value in `external_id`. This is used by the handler to query the
correct registry when filling out questions with attribute mapping
automatically.

In conclusion, every provider has the following options:
- `id_prefix` to identify the instance internally and used by the handler
- `text_prefix` is displayed next to the queried result to identify the used
  registry
- `max_hits` defaults to `10` and limits the results to display
- `base_url` the API URL of the used instance, must be set for the
  `GenericSearchProvider`

### Configuration: Handlers

Handlers, or signal handlers, can be used to fill out questions with the search result
automatically via a pre-configured attribute mapping.
For every provider a handler is implemented,
which can request additional information from the database to answer questions.

The catalog must be configured to be able to use the handlers and each provider must also
configure an attribute mapping.

```toml
[handlers.ExampleInstrumentSearchHandler]
[[handlers.ExampleInstrumentSearchHandler.backends]]
id_prefix = "example"
base_url = "https://example.com/api/v1/instruments"
[[handlers.ExampleInstrumentSearchHandler.catalogs]]
catalog_uri = "http://example.com/terms/questions/test-instrument-search"
auto_complete_field_uri = "http://example.com/terms/domain/search/instrument/example"
[handlers.ExampleInstrumentSearchHandler.catalogs.attribute_mapping]
"longName" = "http://example.com/terms/domain/search/instrument/example/long-name"
"shortName" =  "http://example.com/terms/domain/search/instrument/example/short-name"
"serialNumber" =  "http://example.com/terms/domain/search/instrument/example/serial"
```

A `backends` configuration must be defined in the case of
`ExampleInstrumentSearchHandler` or if more than one instance of one provider is
used. Here the `id_prefix` and the `base_url` is critical and must be the same
as in the `providers` configuration, so that additional requests can be made
to the correct endpoint.

The `catalogs` configuration is used to identify the catalog(s) where the
attribute mapping should be used to map values from the API response to
attributes of the catalog. It is possible to configure more than one catalog.
- `catalog_uri` is the uri of the catalog where the handler should map values
  to attributes
- `auto_complete_field_uri` is the uri of the question with the option set
  provider used in the catalog

With `catalogs.attribute_mapping` the mapping from the APIs JSON response is
mapped to attributes of the specified catalog. On the left a
[JMESPath](https://jmespath.org/) for the value from the API and on the right
the uri to the attribute in the catalog.
