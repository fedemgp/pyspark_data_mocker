import pathlib
from pathlib import Path
from typing import Union

import yaml
from dacite import from_dict

from .app_config import AppConfig
from .schema import config_schema


def get_config_from_dir(filepath: Union[str, Path]) -> AppConfig:
    """
    Reads the configuration of the yaml file located in <filepath>, validates it and return an
    AppConfig dataclass instance of the app configuration.

    :param filepath:
    :return:
    """
    with open(filepath, "r") as file:
        config = yaml.safe_load(file)

    config = config_schema.validate(config)
    return from_dict(data_class=AppConfig, data=config)


def default_config() -> AppConfig:
    """
    :return: Returns a default AppConfig class with basic configuration
    """
    default_config_path = pathlib.Path(pathlib.Path(__file__).parent, "default_config.yaml")
    return get_config_from_dir(default_config_path)
