from collections.abc import Callable

from loguru import logger
from warcio.archiveiterator import ArchiveIterator

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
        def leaf_callback_print(data):
            logger.debug(f"{self.prefix}🍃 leaf ({data})")

        return self._iterate_stream(storage_stream, leaf_callback=leaf_callback_print)

    def iterate_warcs(self, storage_stream: StorageStream) -> StorageStream:
        def leaf_callback_iterate_warc(data):
            with data() as stream_in:
                for record in ArchiveIterator(stream_in, no_record_parse=False, arc2warc=True, verify_http=False):
                    logger.debug(record.http_headers)

        for id, data, metadata in self._iterate_stream(storage_stream, leaf_callback=leaf_callback_iterate_warc):
            pass

    def _iterate_stream(self, storage_stream: StorageStream, leaf_callback: Callable) -> StorageStream:
        logger.debug(f"→ {self.prefix}{storage_stream}")
        for id, data, metadata in storage_stream:
            logger.debug(f"{self.prefix}{id}, {self._iterate_stream(data, leaf_callback=leaf_callback)}, {metadata}")
            if isinstance(data, Callable):
                leaf_callback(data)
                yield id, data, metadata
            else:
                logger.debug(f"{self.prefix}↳ descend ({data})")
                yield id, self._iterate_stream(data, leaf_callback=leaf_callback), metadata


export = DebugPlugin
