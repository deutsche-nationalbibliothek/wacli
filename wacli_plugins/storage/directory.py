"""This is the directory storage module."""

from wacli.plugin_manager import StoragePlugin


class DirectoryStorage(StoragePlugin):
    """This is the directory storage plugin."""

    def __init__(self):
        super(DirectoryStorage, self).__init__()

    def configure(self, configuration):
        self.basedirectory = configuration.get("basedirectory")

    def store(self, warc):
        self.basedirectory
        pass

    def retrieve(self, id):
        pass


export = DirectoryStorage
