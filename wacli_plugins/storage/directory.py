"""This is the directory storage module."""

from io import BytesIO, StringIO
from os import listdir
from os.path import isdir, isfile
from pathlib import Path
from typing import BinaryIO, TextIO, Union

from loguru import logger

from wacli.plugin_types import StoragePlugin


class DirectoryStorage(StoragePlugin):
    """This is the directory storage plugin."""

    def configure(self, configuration):
        self.path = Path(configuration.get("path"))

    def get_stream(
        self,
        selector,
        mode: str = "w",
    ):
        """Create a file with the given id as name in the directory."""

        if mode not in ["r", "w", "rb", "wb"]:
            raise Exception("Only 'r', 'w', 'rb', and 'wb' modes are supported.")

        if selector is None:
            raise Exception("DirectoryStorage needs a list of explicite IDs")

        for id in selector:
            yield id, self.retrieve(id, mode)

    def store(
        self,
        id: str,
        data: Union[TextIO, BinaryIO, None] = None,
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

        return self._retrieve(self.path, id, mode)

    def list(self) -> list:
        return [d for d in listdir(self.path) if isdir(self.path / d)]

    def _retrieve(
        self,
        path,
        id,
        mode: str = "r",
    ):
        if mode not in ["r", "rb"]:
            raise Exception("Only 'r' and 'rb' modes are supported.")

        if isfile(path / id):
            yield id, lambda: open(path / id, mode)
        else:
            for name in listdir(path / id):
                yield name, self._retrieve(path / id, name, mode)

    def list(self) -> list:
        return [d for d in listdir(self.path) if isdir(self.path / d)]


export = DirectoryStorage
