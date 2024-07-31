from io import BytesIO, StringIO, TextIOWrapper

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


def test_store_text_io(tmp_path):
    plugin_manager = PluginManager()
    plugin_manager.register_plugins(get_plugin_config(tmp_path))

    test_storage = plugin_manager.get("test_storage")
    example_file = "example_file.txt"
    content = "some initial text data"
    stream = StringIO(content)

    test_storage.store(example_file, lambda: stream)

    p = tmp_path / example_file
    assert p.read_text() == content
    assert len(list(tmp_path.iterdir())) == 1


def test_store_binary_io_in_text_io_wrapper(tmp_path):
    plugin_manager = PluginManager()
    plugin_manager.register_plugins(get_plugin_config(tmp_path))

    test_storage = plugin_manager.get("test_storage")
    example_file = "example_file.txt"
    content = "some initial binary text data"
    binary_content = content.encode("utf-8")
    stream = TextIOWrapper(BytesIO(binary_content))

    test_storage.store(example_file, lambda: stream)

    p = tmp_path / example_file
    assert p.read_text() == content
    assert len(list(tmp_path.iterdir())) == 1


def test_store_binary_io(tmp_path):
    plugin_manager = PluginManager()
    plugin_manager.register_plugins(get_plugin_config(tmp_path))

    test_storage = plugin_manager.get("test_storage")
    example_file = "example_file.txt"
    content = "some initial binary text data"
    binary_content = content.encode("utf-8")
    stream = BytesIO(binary_content)

    test_storage.store(example_file, lambda: stream)

    p = tmp_path / example_file
    assert p.read_text() == content
    assert len(list(tmp_path.iterdir())) == 1


def test_get_stream_text_io(tmp_path):
    plugin_manager = PluginManager()
    plugin_manager.register_plugins(get_plugin_config(tmp_path))

    test_storage = plugin_manager.get("test_storage")
    example_file = "example_file.txt"
    content = "Hallo"

    with test_storage.retrieve(example_file, mode="w")[0]() as stream:
        stream.write(content)

    p = tmp_path / example_file
    assert p.read_text() == content
    assert len(list(tmp_path.iterdir())) == 1


def test_get_stream_binary_io(tmp_path):
    plugin_manager = PluginManager()
    plugin_manager.register_plugins(get_plugin_config(tmp_path))

    test_storage = plugin_manager.get("test_storage")
    example_file = "example_file.txt"

    content = "Hallo binary"
    binary_content = content.encode("utf-8")
    logger.debug(binary_content)

    with test_storage.retrieve(example_file, mode="wb")[0]() as stream:
        stream.write(binary_content)

    p = tmp_path / example_file
    assert p.read_text() == content
    assert len(list(tmp_path.iterdir())) == 1


def test_retrieve_str(tmp_path):
    plugin_manager = PluginManager()
    plugin_manager.register_plugins(get_plugin_config(tmp_path))

    test_storage = plugin_manager.get("test_storage")
    example_file = "example_file.txt"
    p = tmp_path / example_file
    content = "some initial text data"

    with open(p, "w") as fp:
        fp.write(content)

    result_content, _ = test_storage.retrieve(example_file)

    with result_content() as result_data:
        assert result_data.read() == content
