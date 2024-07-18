from io import BytesIO, StringIO

from loguru import logger

from wacli.plugin_manager import PluginManager


def get_plugin_config(path):
    return {
        "test_storage": [
            {
                "module": "wacli_plugins.storage.file",
                "path": path,
            }
        ]
    }


def test_store_string(tmp_path):
    plugin_manager = PluginManager()
    p = tmp_path / "example_file.txt"
    plugin_manager.register_plugins(get_plugin_config(p))

    test_storage = plugin_manager.get("test_storage")
    content = "Hallo"

    test_storage.store("", content)

    assert p.read_text() == content
    assert len(list(tmp_path.iterdir())) == 1


def test_store_bytes(tmp_path):
    plugin_manager = PluginManager()
    p = tmp_path / "example_file.txt"
    plugin_manager.register_plugins(get_plugin_config(p))

    test_storage = plugin_manager.get("test_storage")

    content = "Hallo binary"
    binary_content = content.encode("utf-8")
    logger.debug(binary_content)

    test_storage.store("", binary_content)

    assert p.read_text() == content
    assert len(list(tmp_path.iterdir())) == 1


def test_store_text_io(tmp_path):
    plugin_manager = PluginManager()
    p = tmp_path / "example_file.txt"
    plugin_manager.register_plugins(get_plugin_config(p))

    test_storage = plugin_manager.get("test_storage")
    content = "some initial text data"
    stream = StringIO(content)

    test_storage.store("", stream)

    assert p.read_text() == content
    assert len(list(tmp_path.iterdir())) == 1


def test_store_binary_io(tmp_path):
    plugin_manager = PluginManager()
    p = tmp_path / "example_file.txt"
    plugin_manager.register_plugins(get_plugin_config(p))

    test_storage = plugin_manager.get("test_storage")
    content = "some initial binary text data"
    binary_content = content.encode("utf-8")
    stream = BytesIO(binary_content)

    test_storage.store("", stream)

    assert p.read_text() == content
    assert len(list(tmp_path.iterdir())) == 1


def test_get_stream_text_io(tmp_path):
    plugin_manager = PluginManager()
    p = tmp_path / "example_file.txt"
    plugin_manager.register_plugins(get_plugin_config(p))

    test_storage = plugin_manager.get("test_storage")
    content = "Hallo"

    with test_storage.get_stream("") as stream:
        stream.write(content)

    assert p.read_text() == content
    assert len(list(tmp_path.iterdir())) == 1


def test_get_stream_binary_io(tmp_path):
    plugin_manager = PluginManager()
    p = tmp_path / "example_file.txt"
    plugin_manager.register_plugins(get_plugin_config(p))

    test_storage = plugin_manager.get("test_storage")

    content = "Hallo binary"
    binary_content = content.encode("utf-8")
    logger.debug(binary_content)

    with test_storage.get_stream("", mode="wb") as stream:
        stream.write(binary_content)

    assert p.read_text() == content
    assert len(list(tmp_path.iterdir())) == 1


def test_retrieve_str(tmp_path):
    plugin_manager = PluginManager()
    p = tmp_path / "example_file.txt"
    plugin_manager.register_plugins(get_plugin_config(p))

    test_storage = plugin_manager.get("test_storage")
    content = "some initial text data"

    with open(p, "w") as fp:
        fp.write(content)

    result_content = test_storage.retrieve("")

    assert result_content == content
