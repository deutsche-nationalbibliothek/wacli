"""This is the directory storage module."""

from collections.abc import Callable
from io import DEFAULT_BUFFER_SIZE, BytesIO
from os import listdir
from os.path import exists, isdir, isfile
from pathlib import Path
from typing import BinaryIO, TextIO, Union

from wacli.plugin_types import StoragePlugin


class DirectoryStorage(StoragePlugin):
    """This is the directory storage plugin."""

    def configure(self, configuration):
        self.path = Path(configuration.get("path"))

    def store(
        self,
        id: str,
        data: Callable[[], Union[TextIO, BinaryIO]],
        metadata: dict = {},
    ):
        """Create a file with the given id as name in the directory."""
        self._store_data(self.path / id, data, metadata)

    def _store_data(self, path, data, metadata):
        with data() as source_io:
            mode = "w"
            if isinstance(source_io, BytesIO):
                mode = "wb"

            target, target_metadata = self._retrieve(path, mode)
            with target() as target_io:
                while True:
                    chunk = source_io.read(DEFAULT_BUFFER_SIZE)
                    if chunk:
                        target_io.write(chunk)
                    else:
                        break

    def store_stream(
        self,
        stream: list,
    ):
        """Write the stream to the directory."""
        self._store_stream(self.path, stream)

    def _store_stream(self, path, stream):
        for id, data, metadata in stream:
            if isinstance(data, list):
                self._store_stream(path / id, data)
            else:
                self._store_data(path / id, data, metadata)

    def retrieve(
        self,
        id: str,
        mode: str = "r",
    ) -> tuple[Callable[[], Union[TextIO, BinaryIO]], dict]:
        if mode not in ["r", "w", "rb", "wb"]:
            raise Exception("Only 'r', 'w', 'rb', and 'wb' modes are supported.")
        return self._retrieve(self.path / id, mode)

    def _retrieve(self, path, mode):
        return lambda: open(path, mode), {}

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
