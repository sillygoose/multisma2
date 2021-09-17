"""Custom YAML file loader with !secrets support."""

import logging
import os
import sys

from config.configuration import Configuration
from dateutil.parser import parse
from pathlib import Path
import yaml
from config import config_from_yaml

from collections import OrderedDict
from typing import Dict, List, TextIO, TypeVar, Union

from exceptions import FailedInitialization


CONFIG_YAML = "multisma2.yaml"
SECRET_YAML = "secrets.yaml"

JSON_TYPE = Union[List, Dict, str]  # pylint: disable=invalid-name
DICT_T = TypeVar("DICT_T", bound=Dict)  # pylint: disable=invalid-name

_LOGGER = logging.getLogger("multisma2")
__SECRET_CACHE: Dict[str, JSON_TYPE] = {}


class ConfigError(Exception):
    """General YAML configurtion file exception."""


class FullLineLoader(yaml.FullLoader):
    """Loader class that keeps track of line numbers."""

    def compose_node(self, parent: yaml.nodes.Node, index: int) -> yaml.nodes.Node:
        """Annotate a node with the first line it was seen."""
        last_line: int = self.line
        node: yaml.nodes.Node = super().compose_node(parent, index)
        node.__line__ = last_line + 1  # type: ignore
        return node


def load_yaml(fname: str) -> JSON_TYPE:
    """Load a YAML file."""
    try:
        with open(fname, encoding="utf-8") as conf_file:
            return parse_yaml(conf_file)
    except UnicodeDecodeError as exc:
        _LOGGER.error("Unable to read file %s: %s", fname, exc)
        raise ConfigError(exc) from exc


def parse_yaml(content: Union[str, TextIO]) -> JSON_TYPE:
    """Load a YAML file."""
    try:
        # If configuration file is empty YAML returns None
        # We convert that to an empty dict
        return yaml.load(content, Loader=FullLineLoader) or OrderedDict()
    except yaml.YAMLError as exc:
        _LOGGER.error(str(exc))
        raise ConfigError(exc) from exc


def _load_secret_yaml(secret_path: str) -> JSON_TYPE:
    """Load the secrets yaml from path."""
    secret_path = os.path.join(secret_path, SECRET_YAML)
    if secret_path in __SECRET_CACHE:
        return __SECRET_CACHE[secret_path]

    _LOGGER.debug("Loading %s", secret_path)
    try:
        secrets = load_yaml(secret_path)
        if not isinstance(secrets, dict):
            raise ConfigError("Secrets is not a dictionary")

    except FileNotFoundError:
        secrets = {}

    __SECRET_CACHE[secret_path] = secrets
    return secrets


def secret_yaml(loader: FullLineLoader, node: yaml.nodes.Node) -> JSON_TYPE:
    """Load secrets and embed it into the configuration YAML."""
    if os.path.basename(loader.name) == SECRET_YAML:
        _LOGGER.error("secrets.yaml: attempt to load secret from within secrets file")
        raise ConfigError("secrets.yaml: attempt to load secret from within secrets file")

    secret_path = os.path.dirname(loader.name)
    home_path = str(Path.home())
    do_walk = os.path.commonpath([secret_path, home_path]) == home_path

    while True:
        secrets = _load_secret_yaml(secret_path)
        if node.value in secrets:
            _LOGGER.debug(
                "Secret %s retrieved from secrets.yaml in folder %s",
                node.value,
                secret_path,
            )
            return secrets[node.value]

        if not do_walk or (secret_path == home_path):
            break
        secret_path = os.path.dirname(secret_path)

    raise ConfigError(f"Secret '{node.value}' not defined")


def check_key(config, required, path='') -> bool:
    """Recursively check the configuration file for required entries."""

    passed = True
    for k, v in config.items():
        currentpath = path + k if path == '' else path + '.' + k

        # look for unknown entries
        options = required.get(k, None)
        if options is None:
            _LOGGER.error(f"Unknown option '{currentpath}'")
            passed = False
            continue
        elif options is False:
            continue

        # look for missing entries
        if isinstance(v, dict):
            for key in options.keys():
                if v.get(key, None) is None:
                    _LOGGER.error(f"Missing option '{key}' in '{currentpath}'")
                    passed = False
        if isinstance(v, Configuration):
            for key in options.keys():
                v1 = dict(v)
                if v1.get(key, None) is None:
                    _LOGGER.error(f"Missing option '{key}' in '{currentpath}'")
                    passed = False

        if isinstance(v, dict):
            passed = check_key(v, options, currentpath) and passed
        elif isinstance(v, list):
            for lk in v:
                passed = check_key(lk, options, currentpath) and passed
        elif isinstance(v, Configuration):
            passed = check_key(dict(v), options, currentpath) and passed
    return passed


def check_config(config):
    """Check that the important options are present and unknown options aren't."""

    required_keys = {
        'multisma2': {
            'log': {'file': True, 'format': True, 'level': True},
            'site': {'name': True, 'region': True, 'tz': True, 'latitude': True, 'longitude': True, 'elevation': True, 'co2_avoided': True},
            'solar_properties': {'azimuth': True, 'tilt': True, 'area': True, 'efficiency': True, 'rho': True},
            'influxdb2': {'enable': True, 'org': True, 'url': True, 'bucket': True, 'token': True},
            'mqtt': {'enable': True, 'client': True, 'ip': True, 'port': True, 'username': True, 'password': True},
            'inverters': {'inverter': {'name': True, 'url': True, 'username': True, 'password': True}}
        }
    }
    result = check_key(dict(config), required_keys)
    return config if result else None


def read_config(checking=False):
    """Open the YAML configuration file and optionally check the contents"""

    try:
        yaml.FullLoader.add_constructor('!secret', secret_yaml)
        yaml_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), CONFIG_YAML)
        config = config_from_yaml(data=yaml_file, read_from_file=True)

        if config and checking:
            config = check_config(config)

        if config is None:
            raise FailedInitialization(Exception(f"One or more errors detected in the YAML configuration file"))
    except Exception as e:
        raise FailedInitialization(Exception(f"One or more errors detected in the YAML configuration file: {e}"))
    return config


if __name__ == '__main__':
    # make sure we can run
    if sys.version_info[0] >= 3 and sys.version_info[1] >= 9:
        config = read_config()
    else:
        print("python 3.9 or better required")
