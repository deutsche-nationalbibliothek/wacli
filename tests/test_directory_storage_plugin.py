from io import BytesIO, StringIO

from loguru import logger

from wacli.plugin_manager import PluginManager


def get_plugin_config(path):
    return {
        "test_storage": [
            {
                "module": "wacli_plugins.storage.directory",
                "path": path,
            }
        ]
    }


def test_store_string(tmp_path):
    plugin_manager = PluginManager()
    plugin_manager.register_plugins(get_plugin_config(tmp_path))

    test_storage = plugin_manager.get("test_storage")
    example_file = "example_file.txt"
    content = "Hallo"

    test_storage.store(example_file, content)

    p = tmp_path / example_file
    assert p.read_text() == content
    assert len(list(tmp_path.iterdir())) == 1


def test_store_bytes(tmp_path):
    plugin_manager = PluginManager()
    plugin_manager.register_plugins(get_plugin_config(tmp_path))

    test_storage = plugin_manager.get("test_storage")
    example_file = "example_file.txt"

    content = "Hallo binary"
    binary_content = content.encode("utf-8")
    logger.debug(binary_content)

    test_storage.store(example_file, binary_content)

    p = tmp_path / example_file
    assert p.read_text() == content
    assert len(list(tmp_path.iterdir())) == 1


def test_store_text_io(tmp_path):
    plugin_manager = PluginManager()
    plugin_manager.register_plugins(get_plugin_config(tmp_path))

    test_storage = plugin_manager.get("test_storage")
    example_file = "example_file.txt"
    content = "some initial text data"
    stream = StringIO(content)

    test_storage.store(example_file, stream)

    p = tmp_path / example_file
    assert p.read_text() == content
    assert len(list(tmp_path.iterdir())) == 1


def test_store_binary_io(tmp_path):
    plugin_manager = PluginManager()
    plugin_manager.register_plugins(get_plugin_config(tmp_path))

    test_storage = plugin_manager.get("test_storage")
    example_file = "example_file.txt"
    content = b"some initial binary text data"
    stream = BytesIO(content)

    test_storage.store(example_file, stream)

    p = tmp_path / example_file
    assert p.read_text() == content
    assert len(list(tmp_path.iterdir())) == 1
