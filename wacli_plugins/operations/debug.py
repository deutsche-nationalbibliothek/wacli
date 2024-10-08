from collections.abc import Callable

from loguru import logger

from wacli.plugin_types import OperationPlugin, StorageStream


class DebugPlugin(OperationPlugin):
    """Debug a WARC stream.

    This operator should be plugged in like any other operator, but it should not alter
    the stream, but allow introspection and print some information about the stream as
    it is processed."""

    def configure(self, configuration):
        self.log_level = configuration.get("log_level", "DEBUG")
        self.prefix = configuration.get("prefix")

    def run(self, storage_stream: StorageStream) -> StorageStream:
        return self._iterate_stream(storage_stream)

    def _iterate_stream(self, storage_stream: StorageStream) -> StorageStream:
        logger.debug(f"→ {self.prefix}{storage_stream}")
        for id, data, metadata in storage_stream:
            logger.debug(f"{self.prefix}{id}, {self._iterate_stream(data)}, {metadata}")
            if isinstance(data, Callable):
                logger.debug(f"{self.prefix}🍃 leaf")
                yield id, data, metadata
            else:
                logger.debug(f"{self.prefix}↳ descend")
                yield id, self._iterate_stream(data), metadata


export = DebugPlugin
