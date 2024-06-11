import click
from loguru import logger
from rdflib import Graph
from rdflib.namespace import Namespace, NamespaceManager
from rdflib.plugins.stores.sparqlstore import SPARQLStore


@click.group()
@click.option("--endpoint", envvar="SPARQL_TITLE_DATA")
@click.pass_context
def cli(ctx, endpoint):
    ctx.ensure_object(dict)
    ctx.obj["endpoint"] = endpoint


@cli.command()
@click.pass_context
def test_select(ctx):
    store = SPARQLStore(query_endpoint=ctx.obj["endpoint"])
    remote_graph = Graph(store=store)
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
    store = SPARQLStore(query_endpoint=ctx.obj["endpoint"])
    remote_graph = Graph(store=store)
    res = remote_graph.query("""
        construct {
        ?s ?p ?o
        } where {
        ?s ?p ?o
        } limit 10
    """)
    logger.debug(res.serialize(format="turtle"))


@cli.command()
@click.pass_context
def list(ctx):
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
    namespaces.bind(
        "rdact", Namespace("http://rdaregistry.info/termList/RDACarrierType/")
    )

    store = SPARQLStore(query_endpoint=ctx.obj["endpoint"])
    remote_graph = Graph(store=store, namespace_manager=namespaces)
    temporary_graph = remote_graph.query("""
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
        } LIMIT 1000
    """)
    logger.debug(f"store is: {temporary_graph.store}")
    websites_graph = temporary_graph.query("""
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
        } LIMIT 1000
    """)
    idn_result = websites_graph.query("""
        select distinct ?idn {
            ?snapshot dcterms:isPartOf ?page .
            ?snapshot gndo:gndIdentifier ?idn .
        }
    """)
    for row in idn_result:
        logger.debug(row["idn"])
