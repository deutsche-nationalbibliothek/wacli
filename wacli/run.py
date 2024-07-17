import click

from .plugin_manager import PluginManager


@click.group()
@click.pass_context
@click.option("--endpoint", envvar="SPARQL_TITLE_DATA", default=None)
@click.option("--graph-file", envvar="WEBSITE_GRAPH_FILE", default=None)
@click.option("--aras-rest-base", envvar="ARAS_REST_BASE", default=None)
@click.option("--aras-repo", envvar="ARAS_REPO", default=None)
@click.option("--warc-dir", "--warc-directory", envvar="WARC_DIRECTORY", default=None)
def cli(ctx, endpoint, graph_file, aras_rest_base, aras_repo, warc_dir):
    ctx.ensure_object(dict)
    ctx.obj["plugin_manager"] = PluginManager()
    plugin_configuration = {
        "catalog": [
            {
                "module": "wacli_plugins.catalog.graph",
                "endpoint": endpoint,
                "storage_backend": "catalog_backend",
            }
        ],
        "catalog_backend": [
            {
                "module": "wacli_plugins.storage.directory",
                "path": graph_file,
            }
        ],
        "source_repository": [
            {
                "module": "wacli_plugins.storage.aras",
                "rest_base": aras_rest_base,
                "repo": aras_repo,
            }
        ],
        "local_repository": [
            {
                "module": "wacli_plugins.storage.directory",
                "path": warc_dir,
            }
        ],
        "indexers": [
            {"module": "wacli_plugins.indexer.pywb"},
            {"module": "wacli_plugins.indexer.solrwayback"},
        ],
    }
    ctx.obj["plugin_manager"].register_plugins(plugin_configuration)


@cli.command()
@click.pass_context
def list_plugins(ctx):
    ctx.obj["plugin_manager"].list_registered_plugins()


@cli.command()
@click.pass_context
def list_available_plugins(ctx):
    ctx.obj["plugin_manager"].list_available_plugins()


@cli.command()
@click.pass_context
def load_graph(ctx):
    # WebGraph().load_graph(endpoint=endpoint, graph_file=graph_file)
    catalog = ctx.obj["plugin_manager"].get("catalog")
    catalog.initialize()


@cli.command()
@click.pass_context
def list(ctx):
    # WebGraph().list(graph_file)
    catalog = ctx.obj["plugin_manager"].get("catalog")
    catalog.list()


@cli.command()
@click.pass_context
def load_warcs(ctx):
    catalog = ctx.obj["plugin_manager"].get("catalog")
    source_repository = ctx.obj["plugin_manager"].get("source_repository")
    local_repository = ctx.obj["plugin_manager"].get("local_repository")

    local_repository.store(source_repository.retrieve(catalog.list()))


@cli.command()
@click.pass_context
def index_warcs(ctx):
    local_repository = ctx.obj["plugin_manager"].get("local_repository")
    indexers = ctx.obj["plugin_manager"].get_all("indexers")

    warc_list = local_repository.list()
    for indexer in indexers:
        indexer.index(warc_list)
