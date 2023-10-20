import pathlib
import re

import yaml
from dacite import from_dict

from pyspark_data_mocker.config.app_config import AppConfig
from pyspark_data_mocker.config.schema import validate_schema
from pyspark_data_mocker.utils import PathLike


def get_config_from_dir(filepath: PathLike) -> AppConfig:
    """
    Reads the configuration of the yaml file located in <filepath>, validates it and return an
    AppConfig dataclass instance of the app configuration.

    :param filepath:
    :return:
    """
    with open(filepath, "r") as file:
        config = yaml.safe_load(file)

    app_config = validate_schema(config)
    return from_dict(data_class=AppConfig, data=app_config)


def get_schema_configuration_from_dir(filepath: PathLike) -> dict:
    """
    Given the path to a schema_config.yaml file, loads the file, and validates that the file defines table schemas

    :param filepath:
    :return:
    """
    import schema

    with open(filepath, "r") as file:
        config = yaml.safe_load(file)

    # The schema of the file is dynamic, the only restriction is that the config file must contain a dict of dicts,
    # where each key must contain a string value (later we can add more restrictions like the latter str must be a valid
    # DDL-formatted string

    # TODO:   due to an issue with schema (https://github.com/keleshev/schema/issues/255) it is not possible to make a
    #        user friendly error message here. I will add the check with a proper error msg for the keys outside, but
    #        when that bug is fixed, it should be added here using a Regex
    #        schema.Regex(
    #            r"^[a-z]+\.[a-z]+$",
    #            flags=re.I,
    #            error="Table name must contain the form <database>.<table> name convention",
    #        )
    schema_validator = schema.Schema(
        {table_name: {name: str for name in table_config.keys()} for table_name, table_config in config.items()}
    )
    config_validated = schema_validator.validate(config)

    for table_name in config_validated.keys():
        if not re.search(r"^[\w_-]+\.[\w_-]+$", table_name, re.IGNORECASE):
            raise ValueError(
                f"Invalid parameter: Table '{table_name}' name must follow the form <database>.<table> name convention"
            )
    return config_validated


def default_config() -> AppConfig:
    """
    :return: Returns a default AppConfig class with basic configuration
    """
    default_config_path = pathlib.Path(pathlib.Path(__file__).parent, "default_config.yaml")
    return get_config_from_dir(default_config_path)
