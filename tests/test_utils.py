import pathlib
from tempfile import TemporaryDirectory

from pyspark_data_mocker import utils


def test_to_path():
    str_path = "/tmp/foo/bar"
    new_path = utils.to_path(str_path)

    assert new_path == pathlib.Path("/", "tmp", "foo", "bar")

    path = pathlib.Path("tmp", "foo")
    new_path = utils.to_path(path)
    assert path == new_path


def test_default_config():
    config = utils.default_config()
    assert config.app_name == "test"
    assert config.number_of_cores == 1

    assert config.delta_configuration is None
    assert not config.enable_hive
    assert isinstance(config.warehouse_dir, TemporaryDirectory)
