from itertools import tee

import click
from loguru import logger
from rich.progress import Progress

from .plugin_manager import PluginManager


@click.group()
@click.pass_context
@click.option("--endpoint", envvar="SPARQL_TITLE_DATA", default=None)
@click.option("--graph-file", envvar="WEBSITE_GRAPH_FILE", default=None)
@click.option("--graph-limit", envvar="WEBSITE_GRAPH_LIMIT", default=None)
@click.option("--aras-rest-base", envvar="ARAS_REST_BASE", default=None)
@click.option("--aras-repo", envvar="ARAS_REPO", default=None)
@click.option("--warc-dir", "--warc-directory", envvar="WARC_DIRECTORY", default=None)
@click.option(
    "--warc-dir-clean",
    "--warc-directory-clean",
    envvar="WARC_DIRECTORY_CLEAN",
    default=None,
)
@click.option("--pywb-dir", "--pywb-directory", envvar="PYWB_DIRECTORY", default=None)
def cli(
    ctx,
    endpoint,
    graph_file,
    graph_limit,
    aras_rest_base,
    aras_repo,
    warc_dir,
    warc_dir_clean,
    pywb_dir,
):
    ctx.ensure_object(dict)
    ctx.obj["plugin_manager"] = PluginManager()
    plugin_configuration = {
        "catalog": [
            {
                "module": "wacli_plugins.catalog.graph",
                "endpoint": endpoint,
                "limit": graph_limit,
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
        "local_recompressed_repository": [
            {
                "module": "wacli_plugins.storage.directory",
                "path": warc_dir_clean,
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
        "recompressor": [
            {
                "module": "wacli_plugins.operations.recompress",
                "verbose": True,
            },
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


@cli.command("list")
@click.pass_context
def list_cmd(ctx):
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

    logger.debug("Load WARCs …")
    warc_list = catalog.list()
    logger.debug(warc_list)

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
    logger.debug("DONE loading WARCs.")


@cli.command()
@click.pass_context
def index_warcs(ctx):
    local_repository = ctx.obj["plugin_manager"].get("local_repository")
    indexers = list(ctx.obj["plugin_manager"].get_all("indexers"))
    logger.debug(indexers)

    warc_list = local_repository.list_files()
    logger.debug(warc_list)
    warc_lists = iter(tee(warc_list, len(indexers)))
    for indexer in indexers:
        indexer.index(next(warc_lists))


@cli.command()
@click.pass_context
def recompress_warcs(ctx):
    """Recompress warc files"""
    local_repository = ctx.obj["plugin_manager"].get("local_repository")
    local_recompressed_repository = ctx.obj["plugin_manager"].get(
        "local_recompressed_repository"
    )
    recompressor = ctx.obj["plugin_manager"].get("recompressor")

    warc_list = local_repository.list()
    local_recompressed_repository.store_stream(
        recompressor.run(local_repository.retrieve_stream(warc_list, mode="rb"))
    )


@cli.command()
@click.pass_context
def check_warcs(ctx):
    """Check warc files if they are valid"""
    local_repository = ctx.obj["plugin_manager"].get("local_repository")
    report_storage = ctx.obj["plugin_manager"].get("report_storage")
    checkers = ctx.obj["plugin_manager"].get_all("checker")

    warc_list = local_repository.list()
    for checker in checkers:
        report_storage.store_stream(checker.run(warc_list))


if __name__ == "__main__":
    cli()
