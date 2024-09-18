from collections.abc import Callable
from io import BytesIO

from warcio.recompressor import StreamRecompressor

from wacli.plugin_types import OperationPlugin, StorageStream


class RecompressPlugin(OperationPlugin):
    """Recompress WARC files to the canonical format."""

    def configure(self, configuration):
        self.collection = configuration.get("collection")
        self.pywb_path = configuration.get("pywb_path")
        self.warc_path = configuration.get("warc_path")

    def run(self, storage_stream: StorageStream) -> StorageStream:
        return self._iterate_stream(storage_stream)

    def _iterate_stream(self, storage_stream: StorageStream) -> StorageStream:
        for id, data, metadata in storage_stream:
            if isinstance(data, Callable):
                yield self._recompress(id, data, metadata)
            else:
                yield id, self._iterate_stream(data), metadata

    def _recompress(self, id, data, metadata):
        def data_callback():
            with data() as source_io:
                target_io = BytesIO()
                _recompressor = StreamRecompressor(source_io, target_io, True)
                if self._is_compressed(id, metadata):
                    _recompressor.decompress_recompress(data)
                else:
                    _recompressor.recompress(data)

        return id, data_callback, {**metadata, "compression": "application/gzip"}

    def _is_compressed(self, id, metadata):
        if "compression" in metadata:
            return True
        return id[:-3] == ".gz"


export = RecompressPlugin
