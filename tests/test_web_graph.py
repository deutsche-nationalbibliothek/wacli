import pytest
from loguru import logger
from rdflib import Graph
from rdflib.plugins.stores.sparqlstore import SPARQLStore

from wacli.web_graph import WebGraph


class TestWebGraph:
    @classmethod
    def setup_class(cls):
        """setup any state specific to the execution of the given class (which
        usually contains tests).
        """
        cls.web_graph = WebGraph()
        cls.endpoint = "https://qlever.cs.uni-freiburg.de/api/dnb"

    @pytest.mark.skip(
        reason="Some code to check if rdflib and the endpoint play together"
    )
    def test_select(self):
        select_patch(self.web_graph, self.endpoint)

    @pytest.mark.skip(
        reason="Some code to check if rdflib and the endpoint play together"
    )
    def test_construct(self):
        construct_patch(self.web_graph, self.endpoint)


def select_patch(cls, endpoint):
    store = SPARQLStore(query_endpoint=endpoint)
    remote_graph = Graph(store=store)
    res = remote_graph.query("""
        select * {
        ?s ?p ?o
        } limit 10
    """)
    for row in res:
        logger.debug(f"{row['s']}, {row['p']}, {row['o']}")


def construct_patch(cls, endpoint):
    # specify format due to https://github.com/ad-freiburg/qlever/issues/1372
    store = SPARQLStore(query_endpoint=endpoint, returnFormat="turtle")
    remote_graph = Graph(store=store)
    res = remote_graph.query("""
        construct {
        ?s ?p ?o
        } where {
        ?s ?p ?o
        } limit 10
    """)
    logger.debug(res.serialize(format="turtle"))
