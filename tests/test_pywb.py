import os
from pathlib import Path

from loguru import logger

from wacli.plugin_manager import PluginManager


def get_plugin_config(warc_path, pywb_path):
    return {
        "test_indexer": [
            {
                "module": "wacli_plugins.indexer.pywb",
                "collection": "example_test",
                "warc_path": warc_path,
                "pywb_path": pywb_path,
            }
        ],
    }


def test_index_warcs(tmp_path):
    warc_path = tmp_path / "warcs"
    pywb_path = tmp_path / "pywb"

    warc_path.mkdir()
    pywb_path.mkdir()

    warc_file = warc_path / "example.warc.gz"

    test_directory = Path(os.path.dirname(__file__))
    logger.debug(Path(__file__))
    with (
        open(test_directory / "assets/https_example_org.warc.gz", "rb") as input,
        open(warc_file, "wb") as output,
    ):
        output.write(input.read())

    warc_list = [str(warc_file)]

    plugin_manager = PluginManager()
    plugin_manager.register_plugins(get_plugin_config(str(warc_path), str(pywb_path)))
    test_indexer = plugin_manager.get("test_indexer")

    logger.debug(warc_list)
    test_indexer.index(warc_list)


def test_pywb_rebase():
    # prepare

    plugin_manager = PluginManager()
    plugin_manager.register_plugins(get_plugin_config(None, None))

    test_indexer = plugin_manager.get("test_indexer")

    p = Path("/tmp/bla") / "example.warc.gz"
    new_path = test_indexer.rebase(p, Path("/tmp/bla"), Path("/srv/blub"))

    assert str(new_path) == "/srv/blub/example.warc.gz"
