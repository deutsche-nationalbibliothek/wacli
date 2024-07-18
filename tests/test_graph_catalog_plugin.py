import os
from pathlib import Path

from loguru import logger

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
                "module": "wacli_plugins.storage.directory",
                "path": path,
            }
        ],
    }


def test_graph_catalog(tmp_path):
    plugin_manager = PluginManager()
    plugin_manager.register_plugins(get_plugin_config(None, tmp_path))

    # prepare
    graph_file = "graph_file.ttl"
    p = tmp_path / graph_file
    test_directory = Path(os.path.dirname(__file__))
    logger.debug(Path(__file__))
    with (
        open(test_directory / "assets/test_websites.ttl", "r") as input,
        open(p, "w") as output,
    ):
        output.write(input.read())

    test_catalog = plugin_manager.get("test_catalog")

    # test_catalog.initialize(example_file, content)
    idn_list = test_catalog.list()

    assert "1234567890" in idn_list
    # assert len(list(tmp_path.iterdir())) == 1
