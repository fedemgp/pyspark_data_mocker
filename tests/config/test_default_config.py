from tempfile import TemporaryDirectory

from pyspark_data_mocker.config import default_config


def test_default_config():
    config = default_config()
    assert not config.disable_spark_configuration

    assert config.schema.config_file == "schema_config.yaml"
    assert not config.schema.infer

    assert config.spark_configuration.app_name == "test"
    assert config.spark_configuration.number_of_cores == 1

    assert config.spark_configuration.delta_configuration is None
    assert not config.spark_configuration.enable_hive
    assert isinstance(config.spark_configuration.warehouse_dir, TemporaryDirectory)
