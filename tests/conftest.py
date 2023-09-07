import pathlib

import pytest


@pytest.fixture(scope="session")
def data_dir() -> pathlib.Path:
    return pathlib.Path(pathlib.Path(__file__).parent, "data")