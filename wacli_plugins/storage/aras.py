from wacli.plugin_manager import StoragePlugin


class ArasStorage(StoragePlugin):
    def configure(self, configuration):
        self.basedirectory = configuration.get("basedirectory")

    def store(self, id, content):
        self.basedirectory
        pass

    def retrieve(self, id):
        pass


export = ArasStorage
