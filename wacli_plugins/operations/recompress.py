from pathlib import Path

from docker.client import from_env as docker_from_env
from docker.types import Mount
from loguru import logger
from warcio.recompressor import Recompressor

from wacli.plugin_types import OperationPlugin, StorageStream


class RecompressPlugin(OperationPlugin):
    """Recompress WARC files to the canonical format."""

    def configure(self, configuration):
        self.collection = configuration.get("collection")
        self.pywb_path = configuration.get("pywb_path")
        self.warc_path = configuration.get("warc_path")

    def run(self, storage_stream: StorageStream) -> StorageStream:
        containers = []
        for warc in storage_stream:
            warc = self.rebase(
                Path(warc),
                base=Path(self.warc_path),
                to=Path(self.container_path_source),
            )
            proc = self.index_warc(warc)
            containers.append(proc)
            # ============================================================================

    def recompress(cmd):
        _recompressor = Recompressor(cmd.filename, cmd.output, cmd.verbose)
        _recompressor.recompress()

    def index_warc(self, warc):
        env = {"INIT_COLLECTION": self.collection}
        args = ["wb-manager", "add", self.collection, str(warc)]
        mounts = [
            Mount(
                target=self.container_path_source, source=self.warc_path, type="bind"
            ),
            Mount(
                target=self.container_path_webarchive,
                source=self.pywb_path,
                type="bind",
            ),
        ]

        logger.debug(f"command={args}, environment={env}, mounts={mounts},")

        client = docker_from_env()

        container = client.containers.run(
            image="docker.io/webrecorder/pywb",
            command=args,
            environment=env,
            mounts=mounts,
            detach=True,
            # auto_remove=True,
        )
        logger.debug(container)
        logger.debug(container.logs())
        return container

    def rebase(self, warc, base, to):
        return to / warc.relative_to(base)


export = PyWbPlugin
