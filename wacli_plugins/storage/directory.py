"""This is the directory storage module."""

from collections.abc import Callable
from io import DEFAULT_BUFFER_SIZE, TextIOBase
from os import listdir
from os.path import exists, isdir, isfile
from pathlib import Path
from typing import IO, Union

from loguru import logger

from wacli.plugin_types import StoragePlugin


class DirectoryStorage(StoragePlugin):
    """This is the directory storage plugin."""

    def configure(self, configuration):
        self.path = Path(configuration.get("path"))

    def store(
        self,
        id: str,
        data: Callable[[], IO],
        metadata: dict = {},
    ):
        """Create a file with the given id as name in the directory."""
        self._store_data(self.path / id, data, metadata)

    def _store_data(self, path, data, metadata):
        with data() as source_io:
            if isinstance(source_io, TextIOBase):
                mode = "w"
            else:
                mode = getattr(source_io, "mode", "wb")

            target, target_metadata = self._retrieve(path, mode)
            with target() as target_io:
                source_io.wacli_read = source_io.read
                while chunk := source_io.wacli_read(DEFAULT_BUFFER_SIZE):
                    try:
                        target_io.write(chunk)
                    except TypeError:
                        logger.debug("overwrite source_io.wacli_read")
                        target_io.write(chunk.encode("utf-8"))
                        source_io.wacli_read = lambda buffer_size: source_io.read(
                            buffer_size
                        ).encode("utf-8")

    def store_stream(
        self,
        stream: list[tuple[str, Union[list[tuple], Callable[[], IO]], dict]],
    ):
        """Write the stream to the directory."""
        self._store_stream(self.path, stream)

    def _store_stream(self, path, stream):
        for id, data, metadata in stream:
            if isinstance(data, Callable):
                self._store_data(path / id, data, metadata)
            else:
                self._store_stream(path / id, data)

    def retrieve(
        self,
        id: str,
        mode: str = "r",
    ) -> tuple[Callable[[], IO], dict]:
        if mode not in ["r", "w", "rb", "wb"]:
            raise Exception("Only 'r', 'w', 'rb', and 'wb' modes are supported.")
        return self._retrieve(self.path / id, mode)

    def _retrieve(self, path, mode):
        if "w" in mode:
            path.parent.mkdir(parents=True, exist_ok=True)
        return lambda: open(path, mode), {}

    def retrieve_stream(
        self,
        selector,
        mode: str = "r",
    ) -> list[tuple[str, Union[list[tuple], Callable[[], IO]], dict]]:
        """Create a file with the given id as name in the directory."""

        if mode not in ["r", "w", "rb", "wb"]:
            raise Exception("Only 'r', 'w', 'rb', and 'wb' modes are supported.")

        if not selector:
            raise Exception("DirectoryStorage needs a list of explicite IDs")

        return self._retrieve_stream(self.path, selector, mode)

    def list(self) -> list:
        return [d for d in listdir(self.path) if isdir(self.path / d)]

    def _retrieve_stream(
        self,
        path,
        selector,
        mode: str = "r",
    ):
        #  -> list[tuple[str, Union[TextIO, BinaryIO, Callable], dict]]
        if mode not in ["r", "w", "rb", "wb"]:
            raise Exception("Only 'r', 'w', 'rb', and 'wb' modes are supported.")

        for id in selector:
            if not exists(path / id) or isfile(path / id):
                yield id, self._retrieve(path / id, mode), {}
            else:
                yield id, self._retrieve_stream(path / id, listdir(path / id), mode), {}

    def list(self) -> list:
        return [d for d in listdir(self.path) if isdir(self.path / d)]


export = DirectoryStorage
