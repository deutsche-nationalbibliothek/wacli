import click
from rdflib.plugins.stores.sparqlstore import SPARQLStore

from .web_graph import WebGraph


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
    WebGraph(store_select=ctx.obj["store_select"],
        store_construct=ctx.obj["store_construct"]).test_select()


@cli.command()
@click.pass_context
def test_construct(ctx):
    WebGraph(store_select=ctx.obj["store_select"],
        store_construct=ctx.obj["store_construct"]).test_construct()


@cli.command()
@click.option("--graph-file", envvar="WEBSITE_GRAPH_FILE")
@click.pass_context
def load_graph(ctx, graph_file):
    WebGraph(store_select=ctx.obj["store_select"],
        store_construct=ctx.obj["store_construct"]).load_graph(graph_file)

@cli.command()
@click.option("--graph-file", envvar="WEBSITE_GRAPH_FILE")
@click.pass_context
def list(ctx, graph_file):
    WebGraph(store_select=ctx.obj["store_select"],
        store_construct=ctx.obj["store_construct"]).list(graph_file)
