from aras_py.run import get_stream as aras_get_stream

from wacli.plugin_types import StoragePlugin


class ArasStorage(StoragePlugin):
    def configure(self, configuration):
        self.rest_base = configuration.get("rest_base")
        self.repository = configuration.get("repo")

    def store(self, id, data):
        raise Exception("ArasStorage is read only")

    def store_stream(self, stream):
        raise Exception("ArasStorage is read only")

    def retrieve(self, id, mode: str = "rb"):
        if mode not in ["rb"]:
            raise Exception("Only 'rb' mode is supported.")
        for name, stream, metadata in aras_get_stream(self.rest_base, self.repository, id
        ):
            yield name, stream, metadata

    def retrieve_stream(self, selector, mode = "rb"):
        if mode not in ["rb"]:
            raise Exception("Only 'rb' mode is supported.")
        if selector is None:
            raise Exception("ArasStorage needs a list of explicite IDNs")
        for idn in selector:
            yield idn, self.retrieve(idn, mode), {}


export = ArasStorage
