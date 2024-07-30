"""This is the directory storage module."""

from os.path import basename, dirname
from pathlib import Path
from typing import BinaryIO, TextIO, Union

from .directory import DirectoryStorage


class FileStorage(DirectoryStorage):
    """This is the file storage plugin."""

    def configure(self, configuration):
        path = configuration.get("path")
        self.path = Path(dirname(path))
        self.id = Path(basename(path))

    def store(
        self,
        id: str,
        data: Union[TextIO, BinaryIO],
        metadata={},
    ):
        """Create a file with the given id as name in the directory."""

        return super(FileStorage, self).store(id=self.id, data=data, metadata=metadata)

    def store_stream(self, stream: list):
        super(FileStorage, self).store_stream(
            [(self.id, data, metadata) for _, data, metadata in stream]
        )

    def retrieve(
        self,
        id: str,
        mode: str = "r",
    ):
        return super(FileStorage, self).retrieve(id=self.id, mode=mode)

    def retrieve_stream(
        self,
        selector: list,
        mode: str = "w",
    ):
        return super(FileStorage, self).retrieve_stream(selector=[self.id], mode=mode)


export = FileStorage
