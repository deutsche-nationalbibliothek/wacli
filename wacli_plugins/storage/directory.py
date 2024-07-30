"""This is the directory storage module."""

from collections.abc import Callable
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

    def store(
        self,
        id: str,
        data: Union[TextIO, BinaryIO, Callable],
        metadata: {},
    ):
        """Create a file with the given id as name in the directory."""

        mode = "w"
        if isinstance(data, BytesIO):
            mode = "wb"

        with self.retrieve(id, mode) as target:
            if isinstance(data, StringIO) or isinstance(data, BytesIO):
                logger.debug("Write from stream")
                target.write(data.read())

    def store_stream(
        self,
        stream: list,
    ):
        """Write the stream to the directory."""
        for id, data, metadata in stream:
            self.store()

    def retrieve(
        self,
        id: str,
        mode: str = "r",
    ) -> list[tuple[str, Union[TextIO, BinaryIO, Callable], dict]]:
        if mode not in ["r", "w", "rb", "wb"]:
            raise Exception("Only 'r', 'w', 'rb', and 'wb' modes are supported.")

        return self._retrieve(self.path, [id], mode)

    def retrieve_stream(
        self,
        selector,
        mode: str = "r",
    ) -> list[tuple[str, Union[TextIO, BinaryIO, Callable], dict]]:
        """Create a file with the given id as name in the directory."""

        if mode not in ["r", "w", "rb", "wb"]:
            raise Exception("Only 'r', 'w', 'rb', and 'wb' modes are supported.")

        if not selector:
            raise Exception("DirectoryStorage needs a list of explicite IDs")

        return self._retrieve(self.path, selector, mode)

    def list(self) -> list:
        return [d for d in listdir(self.path) if isdir(self.path / d)]

    def _retrieve(
        self,
        path,
        selector,
        mode: str = "r",
    ):
        #  -> list[tuple[str, Union[TextIO, BinaryIO, Callable], dict]]
        if mode not in ["r", "w", "rb", "wb"]:
            raise Exception("Only 'r', 'w', 'rb', and 'wb' modes are supported.")

        for id in selector:
            if isfile(path / id):
                yield id, lambda: open(path / id, mode), {}
            else:
                yield id, self._retrieve(path / id, listdir(path / id), mode), {}

    def list(self) -> list:
        return [d for d in listdir(self.path) if isdir(self.path / d)]


export = DirectoryStorage
