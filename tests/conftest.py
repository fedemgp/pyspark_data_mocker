import pathlib

import pytest

from pyspark_data_mocker import utils
from pyspark_data_mocker.spark_session import SparkTestSession


@pytest.fixture(scope="session")
def data_dir() -> pathlib.Path:
    return pathlib.Path(pathlib.Path(__file__).parent, "data")


@pytest.fixture(scope="session")
def spark_test():
    return SparkTestSession(utils.default_config())
