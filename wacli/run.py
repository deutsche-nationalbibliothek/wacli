import click

from .web_graph import WebGraph


@click.group()
@click.option("--endpoint", envvar="SPARQL_TITLE_DATA")
@click.pass_context
def cli(ctx, endpoint):
    ctx.ensure_object(dict)
    load_plugins()
    ctx.obj["endpoint"] = endpoint


@cli.command()
@click.option("--graph-file", envvar="WEBSITE_GRAPH_FILE")
@click.pass_context
def load_graph(ctx, graph_file):
    WebGraph(endpoint=ctx.obj["endpoint"]).load_graph(graph_file)


@cli.command()
@click.option("--graph-file", envvar="WEBSITE_GRAPH_FILE")
@click.pass_context
def list(ctx, graph_file):
    WebGraph(endpoint=ctx.obj["endpoint"]).list(graph_file)
