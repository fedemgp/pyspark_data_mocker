import pathlib
from pathlib import Path
from typing import Union

from pyspark_data_mocker import config
from pyspark_data_mocker.config import AppConfig

PathLike = Union[str, Path]


def to_path(path: PathLike) -> Path:
    """
    Transforms a PathLike class (that can be a pathlib.Path or a string) into a pathlib.Path

    :param path: PathLike   the path to transform
    :return:
    """
    if isinstance(path, Path):
        return path

    return Path(path)


def default_config() -> AppConfig:
    """
    :return: Returns a default AppConfig class with basic configuration
    """
    default_config_path = pathlib.Path(pathlib.Path(__file__).parent, "config", "default_config.yaml")
    return config.get_config_from_dir(default_config_path)
