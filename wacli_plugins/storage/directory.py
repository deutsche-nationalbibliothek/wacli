"""This is the directory storage module."""

from contextlib import contextmanager
from io import BytesIO, StringIO
from pathlib import Path
from typing import BinaryIO, TextIO, Union

from loguru import logger

from wacli.plugin_types import StoragePlugin


class DirectoryStorage(StoragePlugin):
    """This is the directory storage plugin."""

    def configure(self, configuration):
        self.path = Path(configuration.get("path"))

    @contextmanager
    def get_stream(
        self,
        id: str,
        mode: str = "w",
    ):
        """Create a file with the given id as name in the directory."""

        if mode not in ["r", "w", "rb", "wb"]:
            raise Exception("Only 'r', 'w', 'rb', and 'wb' modes are supported.")

        try:
            target = open(self.path / id, mode)
            logger.debug("yield target")
            yield target
        finally:
            target.close()

    def store(
        self,
        id: str,
        data: Union[TextIO, BinaryIO, str, bytes, None] = None,
        mode: str = "w",
    ):
        """Create a file with the given id as name in the directory."""

        if mode not in ["w", "wb"]:
            raise Exception("Only 'w' and 'wb' modes are supported.")

        if isinstance(data, BytesIO) or isinstance(data, bytes):
            if len(mode) == 1:
                mode += "b"

        with self.get_stream(id, mode) as target:
            if isinstance(data, StringIO) or isinstance(data, BytesIO):
                logger.debug("Write from stream")
                target.write(data.read())
            else:
                logger.debug("Write data")
                target.write(data)

    def retrieve(
        self,
        id,
        mode: str = "r",
    ):
        if mode not in ["r", "rb"]:
            raise Exception("Only 'r' and 'rb' modes are supported.")

        with self.get_stream(id, mode) as source:
            return source.read()


export = DirectoryStorage
