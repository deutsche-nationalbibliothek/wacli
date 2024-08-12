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

    def index(self, warc_list, clean: bool = True, block: bool = False):
        """Add the warc files to the pywb index.
        Clean tells, if the containers should be removed, when they are finished.
        Block tells if the index command will wait for all containers to finish befor
        returning. It will also output the logs of the containers.
        """
        containers = []
        for warc in warc_list:
            warc = self.rebase(
                Path(warc),
                base=Path(self.warc_path),
                to=Path(self.container_path_source),
            )
            proc = self.index_warc(warc, clean and not block)
            containers.append(proc)
        if block:
            for proc in containers:
                while proc.status in ["created", "running"]:
                    proc.reload()
                    logger.debug(proc)
                    logger.debug(proc.status)
                    continue
            for proc in containers:
                logger.debug(proc)
                logger.debug(proc.logs())
                if clean:
                    proc.remove()

    def index_warc(self, warc, clean: bool = True):
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
            auto_remove=clean,
        )
        logger.debug(container)
        return container

    def rebase(self, warc, base, to):
        return to / warc.relative_to(base)


export = PyWbPlugin
