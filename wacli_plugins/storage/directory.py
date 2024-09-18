"""This is the directory storage module."""

from collections.abc import Callable
from io import DEFAULT_BUFFER_SIZE, TextIOBase
from os import listdir, walk
from os.path import exists, isdir, isfile
from pathlib import Path
from typing import IO

from loguru import logger

from wacli.plugin_types import StoragePlugin, StorageStream, StoreItem


class DirectoryStorage(StoragePlugin):
    """This is the directory storage plugin."""

    def configure(self, configuration):
        self.path = Path(configuration.get("path"))

    def store(
        self,
        id: str,
        data: Callable[[], IO],
        metadata: dict = {},
        callback: Callable = None,
    ):
        """Create a file with the given id as name in the directory."""
        self._store_data(self.path / id, data, metadata, callback)

    def _store_data(self, path, data, metadata, callback=None):
        callbacks = []
        logger.debug(f"source metadata: {metadata}")
        if source_callback := metadata.get("callback", False):
            logger.debug("register source_callback")
            callbacks.append(source_callback)

        with data() as source_io:
            if isinstance(source_io, TextIOBase):
                mode = "w"
            else:
                mode = getattr(source_io, "mode", "wb")

            target, target_metadata = self._retrieve(path, mode, callback)
            if target_callback := target_metadata.get("callback", False):
                callbacks.append(target_callback)
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
                    for callback in callbacks:
                        callback(
                            advance=DEFAULT_BUFFER_SIZE,
                            total=metadata["size"],
                            name=path,
                        )

    def store_stream(
        self,
        stream: StorageStream,
        callback: Callable = None,
    ):
        """Write the stream to the directory."""
        self._store_stream(self.path, stream, callback)

    def _store_stream(self, path, stream, callback: Callable):
        for id, data, metadata in stream:
            if isinstance(data, Callable):
                self._store_data(path / id, data, metadata, callback)
            else:
                self._store_stream(path / id, data, callback)

    def retrieve(
        self, id: str, mode: str = "r", callback: Callable = None
    ) -> StoreItem:
        if mode not in ["r", "w", "rb", "wb"]:
            raise Exception("Only 'r', 'w', 'rb', and 'wb' modes are supported.")
        return self._retrieve(self.path / id, mode, callback)

    def _retrieve(self, path, mode, callback):
        if "w" in mode:
            path.parent.mkdir(parents=True, exist_ok=True)
        return lambda: open(path, mode), {"callback": callback}

    def retrieve_stream(
        self, selector, mode: str = "r", callback: Callable = None
    ) -> StorageStream:
        """Create a file with the given id as name in the directory."""

        if mode not in ["r", "w", "rb", "wb"]:
            raise Exception("Only 'r', 'w', 'rb', and 'wb' modes are supported.")

        if not selector:
            raise Exception("DirectoryStorage needs a list of explicite IDs")

        return self._retrieve_stream(self.path, selector, mode, callback)

    def _retrieve_stream(
        self, path, selector, mode: str = "r", callback: Callable = None
    ) -> StorageStream:
        if mode not in ["r", "w", "rb", "wb"]:
            raise Exception("Only 'r', 'w', 'rb', and 'wb' modes are supported.")

        for id in selector:
            if not exists(path / id) or isfile(path / id):
                yield id, self._retrieve(path / id, mode), {"callback": callback}
            else:
                yield (
                    id,
                    self._retrieve_stream(
                        path / id, listdir(path / id), mode, callback
                    ),
                    {},
                )

    def list(self) -> list:
        return [d for d in listdir(self.path) if isdir(self.path / d)]

    def list_files(self, filter_fn=None) -> list:
        for subdir, dirs, files in walk(self.path):
            subdir_p = Path(subdir)
            files = filter(filter_fn, files) if filter_fn else files
            for file in files:
                yield subdir_p / file


export = DirectoryStorage
