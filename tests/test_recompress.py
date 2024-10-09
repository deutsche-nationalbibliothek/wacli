import os
from pathlib import Path
from shutil import copyfile

from loguru import logger

from wacli.plugin_manager import PluginManager


def get_plugin_config(input_path, output_path):
    return {
        "debug_in": [{"module": "wacli_plugins.operations.debug", "prefix": "in: "}],
        "debug_out": [{"module": "wacli_plugins.operations.debug", "prefix": "out: "}],
        "input_storage": [
            {
                "module": "wacli_plugins.storage.directory",
                "path": str(input_path),
            }
        ],
        "output_storage": [
            {
                "module": "wacli_plugins.storage.directory",
                "path": str(output_path),
            }
        ],
        "test_recompressor": [
            {"module": "wacli_plugins.operations.recompress", "verbose": True}
        ],
    }


def test_recompress_warcs(tmp_path):
    input_path = tmp_path / "input"
    output_path = tmp_path / "output"

    input_path.mkdir()
    output_path.mkdir()

    test_directory = Path(os.path.dirname(__file__))
    logger.debug(Path(__file__))

    warc_list = []
    for file in [
        "https_example_org.warc.gz",
        "warcio_example-bad-non-chunked.warc.gz",
        "warcio_example.warc.gz",
    ]:
        warc_file = input_path / file
        copyfile(test_directory / "assets" / file, warc_file)
        warc_list.append(str(warc_file))

    plugin_manager = PluginManager()
    plugin_manager.register_plugins(get_plugin_config(input_path, output_path))
    test_recompressor = plugin_manager.get("test_recompressor")
    input_storage = plugin_manager.get("input_storage")
    output_storage = plugin_manager.get("output_storage")
    debug_in = plugin_manager.get("debug_in")
    debug_out = plugin_manager.get("debug_out")

    logger.debug(warc_list)

    output_storage.store_stream(
        debug_out.run(
            test_recompressor.run(
                debug_in.run(input_storage.retrieve_stream(warc_list, mode="rb"))
            )
        )
    )

    assert "The target file is currently emtpy" == ""
    assert False

from io import BytesIO, BufferedReader, RawIOBase

def test_bytes_io():

    input = BytesIO()
    input.write(b"Hi ")
    input.write(b"Who ")
    input.write(b"are ")
    input.write(b"yOu?")
    input.seek(0)

    buff = BytesIO()
    while chunk := input.read():
        buff.write(chunk.decode("utf-8").lower().encode("utf-8"))

    buff.seek(0)

    reader = BufferedReader(buff)

    print(reader.read())

    assert False

def test_bytes_io_stream():

    class LazyLower(RawIOBase):
        def __init__(self, input):
            self.input = input

        def readable(self):
            return True

        def readinto(self, b):
            chunk = self.input.read().decode("utf-8").lower().encode("utf-8")
            b[:len(chunk)] = chunk
            return len(chunk)

    input = BytesIO()
    input.write(b"Hi ")
    input.write(b"Who ")
    input.write(b"are ")
    input.write(b"yOu?")
    input.seek(0)

    buff = LazyLower(input)
    reader = BufferedReader(buff)

    assert reader.read() == "hi who are you?"
