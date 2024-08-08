import click
from loguru import logger
from rich.progress import Progress

from .plugin_manager import PluginManager


@click.group()
@click.pass_context
@click.option("--endpoint", envvar="SPARQL_TITLE_DATA", default=None)
@click.option("--graph-file", envvar="WEBSITE_GRAPH_FILE", default=None)
@click.option("--aras-rest-base", envvar="ARAS_REST_BASE", default=None)
@click.option("--aras-repo", envvar="ARAS_REPO", default=None)
@click.option("--warc-dir", "--warc-directory", envvar="WARC_DIRECTORY", default=None)
@click.option("--pywb-dir", "--pywb-directory", envvar="PYWB_DIRECTORY", default=None)
def cli(ctx, endpoint, graph_file, aras_rest_base, aras_repo, warc_dir, pywb_dir):
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
                "module": "wacli_plugins.storage.file",
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
            {
                "module": "wacli_plugins.indexer.pywb",
                "collection": "dnb",
                "pywb_path": pywb_dir,
                "warc_path": warc_dir,
            },
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
    for idn in catalog.list():
        logger.info(idn)


@cli.command()
@click.pass_context
def load_warcs(ctx):
    catalog = ctx.obj["plugin_manager"].get("catalog")
    source_repository = ctx.obj["plugin_manager"].get("source_repository")
    local_repository = ctx.obj["plugin_manager"].get("local_repository")

    with Progress() as progress:
        tasks = {}

        def get_task(name):
            if name not in tasks:
                tasks[name] = progress.add_task(
                    "[bright_black]Downloading...",
                    total=None,
                )
            return tasks.get(name)

        def callback_factory(description, id_prefix=None):
            def callback(advance, total, name):
                progress.update(
                    get_task(id_prefix + str(name)),
                    description=f"{description} {name}...",
                    advance=advance,
                    total=total,
                )

            return callback

        target_callback = callback_factory("[blue]Storing", id_prefix="target")
        source_callback = callback_factory("[blue]Downloading", id_prefix="source")

        local_repository.store_stream(
            source_repository.retrieve_stream(catalog.list(), callback=source_callback),
            callback=target_callback,
        )


@cli.command()
@click.pass_context
def index_warcs(ctx):
    local_repository = ctx.obj["plugin_manager"].get("local_repository")
    indexers = ctx.obj["plugin_manager"].get_all("indexers")

    warc_list = local_repository.list_files()
    logger.debug(warc_list)
    for indexer in indexers:
        indexer.index(warc_list)
