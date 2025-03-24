import gzip
from collections.abc import Callable
from contextlib import contextmanager, nullcontext

from loguru import logger
from warcio.recompressor import RecompressorStream

from wacli.plugin_types import OperationPlugin, StorageStream
from wacli_plugins.catalog.graph import RDF, WASE


class RecompressPlugin(OperationPlugin):
    """Recompress WARC files to the canonical format."""

    def configure(self, configuration):
        self.verbose = configuration.get("verbose", False)
        self.catalog = configuration.get("catalog", None)

    def run(self, storage_stream: StorageStream) -> StorageStream:
        return self._iterate_stream(storage_stream)

    def _iterate_stream(self, storage_stream: StorageStream) -> StorageStream:
        for id, data, metadata in storage_stream:
            try:
                if isinstance(data, Callable):
                    yield self._recompress(id, data, metadata)
                else:
                    yield id, self._iterate_stream(data), metadata
            except Exception as e:
                logger.debug(f"Report Exception for id={id}: {e}")
                if self.catalog:
                    self.catalog.report(id,
                        [
                            (RDF.type, WASE.Report),
                            (RDF.type, WASE["Exception"]),
                            (RDFS.comment, f"{e}"),
                        ]
                    )

    def _recompress(self, id, data, metadata):
        @contextmanager
        def data_callback():
            with data() as source_io:
                with (
                    gzip.open(source_io, "rb")
                    if self._is_compressed(id, metadata)
                    else nullcontext(source_io) as source_stream
                ):
                    logger.debug(f"stream is: {source_io}")
                    yield RecompressorStream(source_stream, self.verbose)

        return id, data_callback, {**metadata, "compression": "application/gzip"}

    def _is_compressed(self, id, metadata):
        return True
        if "compression" in metadata:
            return True
        return id[:-3] == ".gz"


export = RecompressPlugin
