import os
from pathlib import Path
from unittest.mock import patch

from wacli.plugin_manager import PluginManager


def get_plugin_config(endpoint, path):
    return {
        "test_catalog": [
            {
                "module": "wacli_plugins.catalog.graph",
                "endpoint": endpoint,
                "storage_backend": "test_catalog_backend",
            }
        ],
        "test_catalog_backend": [
            {
                "module": "wacli_plugins.storage.file",
                "path": path,
            }
        ],
    }


test_directory = Path(os.path.dirname(__file__))


def test_graph_catalog(tmp_path):
    # prepare
    p = tmp_path / "graph_file.ttl"

    plugin_manager = PluginManager()
    plugin_manager.register_plugins(
        get_plugin_config("http://no-endpoint.example.org/", p)
    )

    with (
        open(test_directory / "assets/test_websites.ttl", "r") as input,
        open(p, "w") as output,
    ):
        output.write(input.read())

    test_catalog = plugin_manager.get("test_catalog")

    idn_list = test_catalog.list()

    assert "1234567890" in idn_list
    # assert len(list(tmp_path.iterdir())) == 1


def test_graph_catalog_initialize(tmp_path):
    # prepare
    p = tmp_path / "graph_file.ttl"

    mocked_endpoint = "http://anymocked-sparql-endpoint"

    plugin_manager = PluginManager()
    plugin_manager.register_plugins(get_plugin_config(mocked_endpoint, p))

    test_catalog = plugin_manager.get("test_catalog")

    with (
        patch("rdflib.plugins.stores.sparqlconnector.urlopen") as mock_request,
        open(test_directory / "assets/test_websites.ttl", "rb") as input,
    ):
        mock_request.return_value.headers = {"Content-Type": "text/turtle"}
        mock_request.return_value.read = input.read

        test_catalog.initialize()

    idn_list = test_catalog.list()

    assert "1234567890" in idn_list
