"""This is the directory storage module."""

from textwrap import dedent

from loguru import logger
from rdflib import Graph
from rdflib.namespace import Namespace, NamespaceManager
from rdflib.plugins.stores.sparqlstore import SPARQLStore

from wacli.plugin_manager import ConfigurationError
from wacli.plugin_types import CatalogPlugin


class GraphCatalog(CatalogPlugin):
    """This is the directory storage plugin."""

    def __init__(self):
        super(GraphCatalog, self).__init__()

    def configure(self, configuration):
        self.endpoint = configuration.get("endpoint")
        if self.endpoint is None or self.endpoint == "":
            raise ConfigurationError(
                "The SPARQL query endpoint needs to be set to a not empty value."
            )
        self.storage_backend = self.plugin_manager.get(
            configuration.get("storage_backend")
        )

        self.namespaces = NamespaceManager(Graph())
        self.namespaces.bind(
            "rdf", Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
        )
        self.namespaces.bind("rdfs", Namespace("http://www.w3.org/2000/01/rdf-schema#"))
        self.namespaces.bind("foaf", Namespace("http://xmlns.com/foaf/0.1/"))
        self.namespaces.bind("dc", Namespace("http://purl.org/dc/elements/1.1/"))
        self.namespaces.bind("dcterms", Namespace("http://purl.org/dc/terms/"))
        self.namespaces.bind("bibo", Namespace("http://purl.org/ontology/bibo/"))
        self.namespaces.bind(
            "gndo", Namespace("https://d-nb.info/standards/elementset/gnd#")
        )
        self.namespaces.bind("xsd", Namespace("http://www.w3.org/2001/XMLSchema#"))
        self.namespaces.bind("wdrs", Namespace("http://www.w3.org/2007/05/powder-s#"))
        self.namespaces.bind(
            "rdact", Namespace("http://rdaregistry.info/termList/RDACarrierType/")
        )

        self.order_offset_limit = ""

        limit = configuration.get("limit")
        if limit is not None:
            self.order_offset_limit = f"limit {limit}"

    def initialize(self):
        """Load the graph into a local storage"""
        # specify format due to https://github.com/ad-freiburg/qlever/issues/1372
        store = SPARQLStore(query_endpoint=self.endpoint, returnFormat="turtle")
        remote_graph = Graph(store=store, namespace_manager=self.namespaces)
        remote_query = (
            dedent("""
            CONSTRUCT {
              ?page foaf:isPrimaryTopicOf ?topic;
                    dc:title ?title;
                    dcterms:medium rdact:1018 .
              ?snapshot dcterms:isPartOf ?page ;
                        dc:identifier ?identifier ;
                        wdrs:describedby ?description .
              ?description dcterms:modified ?modification .
            } WHERE {
              ?page foaf:isPrimaryTopicOf ?topic;
                    dc:title ?title;
                    dcterms:medium rdact:1018 .
              ?snapshot dcterms:isPartOf ?page ;
                  dc:identifier ?identifier ;
                  wdrs:describedby ?description .
              ?description dcterms:modified ?modification .
              filter(?modification < "2020-01-01T00:00:00.000"^^xsd:dateTime)
              optional {
                ?snapshot a ?snapshot_type
              }
              filter (!bound(?snapshot_type))
            }
        """)
            + self.order_offset_limit
        )
        logger.debug(f"remote_query: {remote_query}")
        logger.debug(f"self.endpoint: {self.endpoint}")
        remote_result = remote_graph.query(remote_query)
        temporary_graph = remote_result.graph
        temporary_graph.namespace_manager = self.namespaces
        local_query = (
            dedent("""
            CONSTRUCT {
              ?page foaf:isPrimaryTopicOf ?topic;
                    dc:title ?title;
                    dcterms:medium rdact:1018 .
              ?snapshot dcterms:isPartOf ?page ;
                        dc:identifier ?identifier, ?idn ;
                        gndo:gndIdentifier ?idn ;
                        wdrs:describedby ?description .
              ?description dcterms:modified ?modification .
            } WHERE {
              ?page foaf:isPrimaryTopicOf ?topic;
                    dc:title ?title;
                    dcterms:medium rdact:1018 .
              ?snapshot dcterms:isPartOf ?page ;
                  dc:identifier ?identifier ;
                  wdrs:describedby ?description .
              ?description dcterms:modified ?modification .
              bind(SUBSTR(str(?identifier), 9) as ?idn)
            }
        """)
            + self.order_offset_limit
        )
        logger.debug(f"local_query: {local_query}")
        local_result = temporary_graph.query(local_query)
        graph_file_io, _ = self.storage_backend.retrieve("graph_file.ttl", "w")
        website_graph = local_result.graph
        website_graph.namespace_manager = self.namespaces
        website_graph.serialize(graph_file_io(), format="turtle")

    def list(self) -> list:
        """List available web archive entries by IDN."""
        website_graph = Graph()
        graph_file_io, _ = self.storage_backend.retrieve("graph_file.ttl")
        website_graph.parse(source=graph_file_io())
        website_graph.namespace_manager = self.namespaces
        idn_result = website_graph.query("""
            select distinct ?idn {
                ?snapshot dcterms:isPartOf ?page .
                ?snapshot gndo:gndIdentifier ?idn .
            }
        """)
        return [row["idn"].value for row in idn_result]


export = GraphCatalog
