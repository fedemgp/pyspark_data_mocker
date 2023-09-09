from tempfile import TemporaryDirectory

from pyspark_data_mocker.config import default_config


def test_default_config():
    config = default_config()
    assert config.app_name == "test"
    assert config.number_of_cores == 1

    assert config.delta_configuration is None
    assert not config.enable_hive
    assert isinstance(config.warehouse_dir, TemporaryDirectory)
