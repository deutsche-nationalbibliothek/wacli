"""This is the directory storage module."""

from contextlib import contextmanager
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

    @contextmanager
    def get_stream(
        self,
        id: str,
        mode: str = "w",
    ):
        with super(FileStorage, self).get_stream(id=self.id, mode=mode) as parent:
            yield parent

    def store(
        self,
        id: str,
        data: Union[TextIO, BinaryIO, str, bytes, None] = None,
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
