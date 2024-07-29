"""This is the directory storage module."""

from os.path import basename, dirname
from pathlib import Path
from typing import BinaryIO, TextIO, Union

from wacli_plugins.plugin_types.directory import DirectoryStorage


class FileStorage(DirectoryStorage):
    """This is the file storage plugin."""

    def configure(self, configuration):
        path = configuration.get("path")
        self.path = Path(dirname(path))
        self.id = Path(basename(path))

    def get_stream(
        self,
        selector: list,
        mode: str = "w",
    ):
        with super(FileStorage, self).get_stream(
            selector=[self.id], mode=mode
        ) as parent:
            return parent

    def store(
        self,
        id: str,
        data: Union[TextIO, BinaryIO, None] = None,
        mode: str = "w",
    ):
        """Create a file with the given id as name in the directory."""

        return super(FileStorage, self).store(id=self.id, data=data, mode=mode)

    def retrieve(
        self,
        id: str,
        mode: str = "r",
    ):
        return super(FileStorage, self).retrieve(id=self.id, mode=mode)


export = FileStorage
