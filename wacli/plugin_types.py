from abc import abstractmethod
from collections.abc import Callable
from typing import IO, TypeAlias, Union

from .plugin_manager import Plugin

StorageStream: TypeAlias = list[
    tuple[str, Union["StorageStream", Callable[[], IO]], dict]
]
StoreItem: TypeAlias = tuple[Callable[[], IO], dict]
MetadataList: TypeAlias = list[tuple[object, object]]


class CatalogPlugin(Plugin):
    """Access and store metadata for web archive files.

    Get a complete list of all web archive files, store metadata along with the web archive files.
    """

    @abstractmethod
    def initialize(self):
        pass

    @abstractmethod
    def list(self):
        """List the web archive files in the catalog."""
        pass

    @abstractmethod
    def annotate(self, id: str, metadata: StoreItem = []):
        """Add the metadata to the provided resource."""
        pass

    @abstractmethod
    def report(self, id: str, report: StoreItem = []):
        """Create a report for the respective resource."""
        pass


class StoragePlugin(Plugin):
    """Implement the storage and retrieval of WARC files.
    The storage steam is:
    storage_stream: list[tuple[str, Union[storage_stream, Callable[[], IO]], dict]]
    """

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
        stream: StorageStream,
        callback: Callable = None,
    ):
        """Store the data at the given id in the storage.
        If data is None, a writable IO-like object is returned.
        """
        pass

    @abstractmethod
    def retrieve(
        self, id: str, mode: str = "r", callback: Callable = None
    ) -> StoreItem:
        pass

    @abstractmethod
    def retrieve_stream(
        self, selector: list, mode: str = "r", callback: Callable = None
    ) -> StorageStream:
        pass


class IndexerPlugin(Plugin):
    """Implement to trigger the indexing of the WARC files for a replay engine."""

    @abstractmethod
    def index(self, warcs: list):
        """Takes a list of paths to WARC files."""
        pass


class OperationPlugin(Plugin):
    """Implement to trigger some operation on the WARC files."""

    @abstractmethod
    def run(self, stream: StorageStream) -> StorageStream:
        pass
