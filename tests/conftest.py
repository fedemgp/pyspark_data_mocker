import pathlib

import pytest

from pyspark_data_mocker.config import default_config
from pyspark_data_mocker.datalake_builder import DataLakeBuilder
from pyspark_data_mocker.spark_session import SparkTestSession


@pytest.fixture(scope="session")
def data_dir() -> pathlib.Path:
    return pathlib.Path(pathlib.Path(__file__).parent, "data")


@pytest.fixture()
def data_mocker_load_from_dir():
    app_config = default_config()
    builder = DataLakeBuilder(SparkTestSession(app_config.spark_configuration), app_config=app_config)
    yield builder.load_from_dir
    builder.cleanup()


@pytest.fixture()
def data_mocker():
    app_config = default_config()
    builder = DataLakeBuilder(SparkTestSession(app_config.spark_configuration), app_config=app_config)
    yield builder
    builder.cleanup()
