from abc import abstractmethod
from collections.abc import Callable
from typing import IO, Union

from .plugin_manager import Plugin


class CatalogPlugin(Plugin):
    """Access metadata for web archive files.

    Accessing the metadata should allow to get a complte list of all web archive files.
    """

    @abstractmethod
    def initialize(self):
        pass

    @abstractmethod
    def list(self):
        pass


class StoragePlugin(Plugin):
    """Implement the storage and retrieval of WARC files."""

    @abstractmethod
    def store(
        self,
        id: str,
        data: Callable[[], IO],
        metadata: dict = {},
        callback: Callable = None,
    ):
        """Store the data at the given id in the storage.
        If data is None, a writable IO-like object is returned.
        """
        pass

    @abstractmethod
    def store_stream(
        self,
        stream: list[tuple[str, Union[list[tuple], Callable[[], IO]], dict]],
        callback: Callable = None,
    ):
        """Store the data at the given id in the storage.
        If data is None, a writable IO-like object is returned.
        """
        pass

    @abstractmethod
    def retrieve(
        self, id: str, mode: str = "r", callback: Callable = None
    ) -> tuple[Callable[[], IO], dict]:
        pass

    @abstractmethod
    def retrieve_stream(
        self, selector: list, mode: str = "r", callback: Callable = None
    ) -> list[tuple[str, Union[list[tuple], Callable[[], IO]], dict]]:
        pass


class IndexerPlugin(Plugin):
    """Implement to trigger the indexing of the WARC files for a replay engine."""

    @abstractmethod
    def index(self, warc):
        pass
