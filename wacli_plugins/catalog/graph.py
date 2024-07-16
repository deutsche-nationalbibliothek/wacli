"""This is the directory storage module."""

from loguru import logger

from wacli.plugin_manager import CatalogPlugin


class GraphCatalog(CatalogPlugin):
    """This is the directory storage plugin."""

    def __init__(self):
        super(GraphCatalog, self).__init__()

    def configure(self, configuration):
        self.endpoint = configuration.get("endpoint")

    def initialize(self):
        """Load the graph"""
        logger.debug("Initiation sequence")

    def list(self):
        logger.debug("This is the list")


export = GraphCatalog
