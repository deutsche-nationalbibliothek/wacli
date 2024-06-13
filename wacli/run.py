import click
from loguru import logger
from rdflib import Graph
from rdflib.namespace import Namespace, NamespaceManager
from rdflib.plugins.stores.sparqlstore import SPARQLStore

namespaces = NamespaceManager(Graph())
namespaces.bind("rdf", Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#"))
namespaces.bind("rdfs", Namespace("http://www.w3.org/2000/01/rdf-schema#"))
namespaces.bind("foaf", Namespace("http://xmlns.com/foaf/0.1/"))
namespaces.bind("dc", Namespace("http://purl.org/dc/elements/1.1/"))
namespaces.bind("dcterms", Namespace("http://purl.org/dc/terms/"))
namespaces.bind("bibo", Namespace("http://purl.org/ontology/bibo/"))
namespaces.bind("gndo", Namespace("https://d-nb.info/standards/elementset/gnd#"))
namespaces.bind("xsd", Namespace("http://www.w3.org/2001/XMLSchema#"))
namespaces.bind("wdrs", Namespace("http://www.w3.org/2007/05/powder-s#"))
namespaces.bind("rdact", Namespace("http://rdaregistry.info/termList/RDACarrierType/"))


@click.group()
@click.option("--endpoint", envvar="SPARQL_TITLE_DATA")
@click.pass_context
def cli(ctx, endpoint):
    ctx.ensure_object(dict)
    ctx.obj["endpoint"] = endpoint
    # specify format due to https://github.com/ad-freiburg/qlever/issues/1372
    ctx.obj["store_construct"] = SPARQLStore(
        query_endpoint=endpoint, returnFormat="turtle"
    )
    ctx.obj["store_select"] = SPARQLStore(query_endpoint=endpoint)


@cli.command()
@click.pass_context
def test_select(ctx):
    remote_graph = Graph(store=ctx.obj["store_select"])
    res = remote_graph.query("""
        select * {
        ?s ?p ?o
        } limit 10
    """)
    for row in res:
        logger.debug(f"{row["s"]}, {row["p"]}, {row["o"]}")


@cli.command()
@click.pass_context
def test_construct(ctx):
    remote_graph = Graph(store=ctx.obj["store_construct"])
    res = remote_graph.query("""
        construct {
        ?s ?p ?o
        } where {
        ?s ?p ?o
        } limit 10
    """)
    logger.debug(res.serialize(format="turtle"))


@cli.command()
@click.option("--graph-file", envvar="WEBSITE_GRAPH_FILE")
@click.pass_context
def load_graph(ctx, graph_file):
    remote_graph = Graph(store=ctx.obj["store_construct"], namespace_manager=namespaces)
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
    temporary_graph.namespace_manager = namespaces
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
    website_graph.namespace_manager = namespaces
    website_graph.serialize(graph_file, format="turtle")

@cli.command()
@click.option("--graph-file", envvar="WEBSITE_GRAPH_FILE")
@click.pass_context
def list(ctx, graph_file):
    website_graph = Graph()
    website_graph.parse(graph_file)
    website_graph.namespace_manager = namespaces
    idn_result = website_graph.query("""
        select distinct ?idn {
            ?snapshot dcterms:isPartOf ?page .
            ?snapshot gndo:gndIdentifier ?idn .
        }
    """)
    for row in idn_result:
        logger.debug(row["idn"])
