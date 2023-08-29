import os

import pytest


@pytest.fixture(scope="session")
def data_dir() -> str:
    return os.path.join(os.path.dirname(__file__), "data")
