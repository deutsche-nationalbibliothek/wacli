import os
from pathlib import Path
from unittest.mock import patch

from wacli.plugin_manager import PluginManager

from .utils import copy_file


def get_plugin_config(endpoint, graph_path, query_path=None):
    config = {
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
                "path": graph_path,
            }
        ],
    }

    if query_path:
        config["test_catalog"][0]["query_collection_backend"] = (
            "query_collection_backend"
        )
        config["query_collection_backend"] = [
            {
                "module": "wacli_plugins.storage.directory",
                "path": query_path,
            }
        ]
    return config


test_directory = Path(os.path.dirname(__file__))


def test_graph_catalog(tmp_path):
    # prepare
    p = tmp_path / "graph_file.ttl"
    q = tmp_path / "queries"

    remote_query = q / "remote_query.rq"
    local_query = q / "local_query.rq"

    # provide queries
    copy_file(test_directory / "assets/queries/remote_query.rq", remote_query)
    copy_file(test_directory / "assets/queries/local_query.rq", local_query)

    # Inject pre-initialized graph to skip test_catalog.initialize()
    copy_file(test_directory / "assets/test_websites.ttl", p)

    plugin_manager = PluginManager()
    plugin_manager.register_plugins(
        get_plugin_config("http://no-endpoint.example.org/", p, q)
    )

    test_catalog = plugin_manager.get("test_catalog")
    idn_list = test_catalog.list()

    assert "1234567890" in idn_list
    # assert len(list(tmp_path.iterdir())) == 1


def test_graph_catalog_initialize(tmp_path):
    # prepare
    p = tmp_path / "graph_file.ttl"
    q = tmp_path / "queries"

    remote_query = q / "remote_query.rq"
    local_query = q / "local_query.rq"

    # provide queries
    copy_file(test_directory / "assets/queries/remote_query.rq", remote_query)
    copy_file(test_directory / "assets/queries/local_query.rq", local_query)

    mocked_endpoint = "http://anymocked-sparql-endpoint"

    plugin_manager = PluginManager()
    plugin_manager.register_plugins(get_plugin_config(mocked_endpoint, p, q))

    test_catalog = plugin_manager.get("test_catalog")

    # mock the sparql response
    with (
        patch("rdflib.plugins.stores.sparqlconnector.urlopen") as mock_request,
        open(test_directory / "assets/test_websites.ttl", "rb") as input,
    ):
        mock_request.return_value.headers = {"Content-Type": "text/turtle"}
        mock_request.return_value.read = input.read

        test_catalog.initialize()

    idn_list = test_catalog.list()

    assert "1234567890" in idn_list


def test_graph_catalog_initialize_without_query_collection_backend(tmp_path):
    # prepare
    p = tmp_path / "graph_file.ttl"
    q = tmp_path / "queries"

    remote_query = q / "remote_query.rq"
    local_query = q / "local_query.rq"

    # provide queries
    copy_file(test_directory / "assets/queries/remote_query.rq", remote_query)
    copy_file(test_directory / "assets/queries/local_query.rq", local_query)

    mocked_endpoint = "http://anymocked-sparql-endpoint"

    plugin_manager = PluginManager()
    plugin_manager.register_plugins(get_plugin_config(mocked_endpoint, p))

    test_catalog = plugin_manager.get("test_catalog")

    # mock the sparql response
    with (
        patch("rdflib.plugins.stores.sparqlconnector.urlopen") as mock_request,
        open(test_directory / "assets/test_websites.ttl", "rb") as input,
    ):
        mock_request.return_value.headers = {"Content-Type": "text/turtle"}
        mock_request.return_value.read = input.read

        test_catalog.initialize()

    idn_list = test_catalog.list()

    assert "1234567890" in idn_list
