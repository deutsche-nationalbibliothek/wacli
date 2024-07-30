from aras_py.run import get_stream as aras_get_stream

from wacli.plugin_types import StoragePlugin


class ArasStorage(StoragePlugin):
    def configure(self, configuration):
        self.rest_base = configuration.get("rest_base")
        self.repository = configuration.get("repo")

    def store(self, id, data, metadata):
        raise Exception("ArasStorage is read only")

    def store_stream(self, stream):
        raise Exception("ArasStorage is read only")

    def retrieve(self, id, mode: str = "rb") -> list:
        raise Exception(
            "ArasStorage always organzies WARCs in folders of IDNs, "
            + "use retrieve_stream instead"
        )

    def retrieve_stream(self, selector, mode="rb") -> list:
        if mode not in ["rb"]:
            raise Exception("Only 'rb' mode is supported.")
        if selector is None:
            raise Exception("ArasStorage needs a list of explicite IDNs")
        for idn in selector:
            yield idn, aras_get_stream(self.rest_base, self.repository, idn), {}


export = ArasStorage
