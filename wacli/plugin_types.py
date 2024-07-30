from abc import abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import BinaryIO, Self, TextIO, Union

from uuid6 import uuid7

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
        data: Callable[[], Union[TextIO, BinaryIO]],
        metadata: dict = {},
    ):
        """Store the data at the given id in the storage.
        If data is None, a writable IO-like object is returned.
        """
        pass

    @abstractmethod
    def store_stream(
        self,
        stream: list[tuple[str, Callable[[], Union[TextIO, BinaryIO]], dict]],
    ):
        """Store the data at the given id in the storage.
        If data is None, a writable IO-like object is returned.
        """
        pass

    @abstractmethod
    def retrieve(
        self, id: str, mode: str = "r"
    ) -> tuple[Callable[[], Union[TextIO, BinaryIO]], dict]:
        pass

    @abstractmethod
    def retrieve_stream(
        self,
        selector: list,
        mode: str = "r",
    ) -> list[tuple[str, Callable[[], Union[TextIO, BinaryIO]], dict]]:
        pass


class IndexerPlugin(Plugin):
    """Implement to trigger the indexing of the WARC files for a replay engine."""

    @abstractmethod
    def index(self, warc):
        pass


@dataclass
class StorageStream:
    root: dict[str, Union[Self, TextIO, BinaryIO]] = field(
        default_factory=dict[str, Union[Self, TextIO, BinaryIO]]
    )

    def __iter__(self):
        return iter(self.root.values())

    def __setitem__(self, key: str, item: Union[Self, TextIO, BinaryIO]):
        self.root[key] = item

    def __getitem__(self, key: str) -> Union[Self, TextIO, BinaryIO]:
        return self.root[key]

    def append(self, item: Union[Self, TextIO, BinaryIO]) -> str:
        key = uuid7()
        self.root[key] = item
        return key

    def keys(self) -> list[str]:
        return self.root.keys()

    def values(self) -> list[Union[Self, TextIO, BinaryIO]]:
        return self.root.values()

    def items(self) -> tuple[str, Union[Self, TextIO, BinaryIO]]:
        return self.root.items()
