"""This is the directory storage module."""

from textwrap import dedent

from loguru import logger
from rdflib import Graph, URIRef
from rdflib.namespace import (
    DC,
    DCTERMS,
    FOAF,
    RDF,
    RDFS,
    XSD,
    Namespace,
    NamespaceManager,
)
from rdflib.plugins.stores.sparqlstore import SPARQLStore
from uuid6 import uuid7

from wacli.plugin_manager import ConfigurationError
from wacli.plugin_types import CatalogPlugin, MetadataList

BIBO = Namespace("http://purl.org/ontology/bibo/")
GNDO = Namespace("https://d-nb.info/standards/elementset/gnd#")
WDRS = Namespace("http://www.w3.org/2007/05/powder-s#")
RDACT = Namespace("http://rdaregistry.info/termList/RDACarrierType/")
DNB = Namespace("https://d-nb.info/")
"""Web Archive Schema or Web Archive Standard Elementset"""
WASE = Namespace("https://d-nb.info/standards/elementset/webarchive#")


class GraphCatalog(CatalogPlugin):
    """This is the directory storage plugin."""

    def __init__(self):
        super(GraphCatalog, self).__init__()
        self.website_graph = None

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
        self.namespaces.bind("rdf", RDF)
        self.namespaces.bind("rdfs", RDFS)
        self.namespaces.bind("foaf", FOAF)
        self.namespaces.bind("dc", DC)
        self.namespaces.bind("dcterms", DCTERMS)
        self.namespaces.bind("bibo", BIBO)
        self.namespaces.bind("gndo", GNDO)
        self.namespaces.bind("xsd", XSD)
        self.namespaces.bind("wdrs", WDRS)
        self.namespaces.bind("rdact", RDACT)
        self.namespaces.bind("wase", WASE)

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
        graph_file_io, _ = self.storage_backend.retrieve("graph_file.ttl", "wb")
        website_graph = local_result.graph
        website_graph.namespace_manager = self.namespaces
        website_graph.serialize(graph_file_io(), format="turtle")

    def _get_graph(self) -> Graph:
        """Load the website graph into memory."""
        if not self.website_graph:
            self.website_graph = Graph()
            graph_file_io, _ = self.storage_backend.retrieve("graph_file.ttl")
            self.website_graph.parse(source=graph_file_io())
            self.website_graph.namespace_manager = self.namespaces
        return self.website_graph

    def list(self) -> list:
        """List available web archive entries by IDN."""
        website_graph = self._get_graph()
        idn_result = website_graph.query("""
            select distinct ?idn {
                ?snapshot dcterms:isPartOf ?page .
                ?snapshot gndo:gndIdentifier ?idn .
            }
        """)
        return [row["idn"].value for row in idn_result]

    def _get_archive_resource(self, id: URIRef | str):
        """Add the metadata to the provided resource."""
        if isinstance(id, str):
            id = DNB[id]
        return self._get_graph().resource(id)

    def annotate(self, id: URIRef | str, metadata: MetadataList = ()):
        """Add the metadata to the provided resource."""
        archive_resource = self._get_archive_resource(id)
        list(map(lambda item: archive_resource.add(*item), metadata))

    def report(self, id: URIRef | str, report: MetadataList = ()):
        """Create a report for the respective resource."""
        archive_resource = self._get_archive_resource(id)
        report_resource = self._get_graph().resource(URIRef(f"urn:uuid:{uuid7()}"))
        archive_resource.add(WASE["report"], report_resource.identifier)
        list(map(lambda item: report_resource.add(*item), report))


export = GraphCatalog
