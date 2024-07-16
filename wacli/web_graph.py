from loguru import logger
from rdflib import Graph
from rdflib.namespace import Namespace, NamespaceManager
from rdflib.plugins.stores.sparqlstore import SPARQLStore


class WebGraph:
    def __init__(self, endpoint: str):
        self.store_select = SPARQLStore(query_endpoint=endpoint)
        # specify format due to https://github.com/ad-freiburg/qlever/issues/1372
        self.store_construct = SPARQLStore(
            query_endpoint=endpoint, returnFormat="turtle"
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

    def load_graph(self, graph_file):
        remote_graph = Graph(
            store=self.store_construct, namespace_manager=self.namespaces
        )
        remote_result = remote_graph.query("""
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
        temporary_graph = remote_result.graph
        temporary_graph.namespace_manager = self.namespaces
        local_result = temporary_graph.query("""
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
        website_graph = local_result.graph
        website_graph.namespace_manager = self.namespaces
        website_graph.serialize(graph_file, format="turtle")

    def list(self, graph_file):
        website_graph = Graph()
        website_graph.parse(graph_file)
        website_graph.namespace_manager = self.namespaces
        idn_result = website_graph.query("""
            select distinct ?idn {
                ?snapshot dcterms:isPartOf ?page .
                ?snapshot gndo:gndIdentifier ?idn .
            }
        """)
        for row in idn_result:
            logger.debug(row["idn"])
