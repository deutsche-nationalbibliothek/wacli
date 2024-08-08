from pathlib import Path

from docker.client import from_env as docker_from_env
from docker.types import Mount
from loguru import logger

from wacli.plugin_types import IndexerPlugin


class PyWbPlugin(IndexerPlugin):
    """Indexing WARC files to be used with pywb."""

    def configure(self, configuration):
        self.container_path_source = "/source"
        self.container_path_webarchive = "/webarchive"
        self.collection = configuration.get("collection")
        self.pywb_path = configuration.get("pywb_path")
        self.warc_path = configuration.get("warc_path")

    def index(self, warc_list):
        for warc in warc_list:
            warc = self.rebase(
                Path(warc),
                base=Path(self.warc_path),
                to=Path(self.container_path_source),
            )
            self.index_warc(warc)

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
            image="webrecorder/pywb",
            command=args,
            environment=env,
            mounts=mounts,
            detach=True,
            auto_remove=True,
        )
        logger.debug(container)

    def rebase(self, warc, base, to):
        return to / warc.relative_to(base)


export = PyWbPlugin
