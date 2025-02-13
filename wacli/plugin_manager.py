import importlib
import pkgutil
from abc import ABC, abstractmethod
from collections import defaultdict

from loguru import logger

import wacli_plugins.catalog
import wacli_plugins.indexer
import wacli_plugins.storage


class ConfigurationError(Exception):
    pass


class PluginManager:
    def __init__(self):
        self.registry = defaultdict(list)
        self.plugin_namespaces = [
            wacli_plugins.catalog,
            wacli_plugins.indexer,
            wacli_plugins.storage,
        ]
        self.plugin_factory = PluginFactory(self)

    def iter_namespace(self, ns_pkg):
        return pkgutil.iter_modules(ns_pkg.__path__, ns_pkg.__name__ + ".")

    def list_registered_plugins(self):
        logger.debug("loaded plugins")
        for role, plugins in self.registry.items():
            logger.debug(f"# {role}")
            for plugin in plugins:
                logger.debug(f"{plugin["class"].__name__}")
                logger.debug(f"{plugin["class"].__doc__}\n")
                logger.debug(f"{plugin}\n")

    def list_available_plugins(self):
        for namespace in self.plugin_namespaces:
            for _, name, _ in self.iter_namespace(namespace):
                module = importlib.import_module(name)
                logger.debug(f"available_plugin: {name}: {module.__doc__}")

    def register_plugins(self, plugin_configuration: dict):
        for role, plugins in plugin_configuration.items():
            for plugin in plugins:
                self.register_plugin(role, plugin)

    def register_plugin(self, role: str, plugin_configuration: dict):
        logger.debug(f"import_module: {plugin_configuration["module"]} for role {role}")
        plugin_module = importlib.import_module(plugin_configuration["module"])
        if hasattr(plugin_module, "export"):
            self.registry[role].append(
                {**plugin_configuration, "class": plugin_module.export}
            )
        else:
            logger.error(
                f"configured module: {plugin_configuration["module"]} has no export"
            )

    def get_modules(self, name: str):
        return self.registry[name]

    def get_all(self, name: str):
        for plugin in self.get_modules(name):
            if "instance" not in plugin:
                try:
                    plugin["instance"] = self.plugin_factory.get_plugin(plugin)
                except ConfigurationError as e:
                    raise ConfigurationError(f"role '{name}' {e}")
            yield plugin["instance"]

    def get(self, name: str):
        return next(self.get_all(name))


class PluginFactory:
    def __init__(self, plugin_manager: PluginManager):
        self.plugin_manager = plugin_manager

    def get_plugin(self, plugin: dict):
        instance = plugin["class"]()
        instance._plugin_manager = self.plugin_manager
        try:
            instance.configure(plugin)
        except ConfigurationError as e:
            raise ConfigurationError(f"(module: {plugin["module"]}): {e}")
        return instance


class Plugin(ABC):
    @abstractmethod
    def configure(self, configuration: dict):
        pass

    @property
    def plugin_manager(self):
        return self._plugin_manager
