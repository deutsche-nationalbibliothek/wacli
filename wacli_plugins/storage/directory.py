"""This is the directory storage module."""

from loguru import logger
from contextlib import contextmanager
from io import BytesIO, StringIO
from pathlib import Path
from typing import BinaryIO, TextIO, Union

from wacli.plugin_manager import StoragePlugin


class DirectoryStorage(StoragePlugin):
    """This is the directory storage plugin."""

    def __init__(self):
        super(DirectoryStorage, self).__init__()

    def configure(self, configuration):
        self.basedirectory = Path(configuration.get("path"))

    @contextmanager
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

        try:
            target = open(self.basedirectory / id, mode)
            if data is None:
                logger.debug("yield target")
                yield target
            if isinstance(data, StringIO) or isinstance(data, BytesIO):
                logger.debug("Write Stream")
                target.write(data.getvalue())
            else:
                logger.debug("Write data")
                target.write(data)
        finally:
            target.close()

    def retrieve(self, id):
        pass


export = DirectoryStorage
