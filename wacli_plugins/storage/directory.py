"""This is the directory storage module."""

from contextlib import contextmanager
from io import BytesIO, StringIO
from pathlib import Path
from typing import BinaryIO, TextIO, Union

from loguru import logger

from wacli.plugin_manager import StoragePlugin


class DirectoryStorage(StoragePlugin):
    """This is the directory storage plugin."""

    def __init__(self):
        super(DirectoryStorage, self).__init__()

    def configure(self, configuration):
        self.path = Path(configuration.get("path"))

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

        with open(self.path / id, mode) as target:
            if isinstance(data, StringIO) or isinstance(data, BytesIO):
                logger.debug("Write from stream")
                target.write(data.getvalue())
            else:
                logger.debug("Write data")
                target.write(data)

    @contextmanager
    def get_stream(
        self,
        id: str,
        mode: str = "w",
    ):
        """Create a file with the given id as name in the directory."""

        if mode not in ["w", "wb"]:
            raise Exception("Only 'w' and 'wb' modes are supported.")

        try:
            target = open(self.path / id, mode)
            logger.debug("yield target")
            yield target
        finally:
            target.close()

    def retrieve(self, id):
        pass


export = DirectoryStorage
