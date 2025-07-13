import os
import tempfile

from pyspark_data_mocker.config import AppConfig, get_config_from_dir


def test_get_config_returns_a_dataclass_fully_configured(data_dir):
    config_path = os.path.join(data_dir, "config", "example.yaml")
    config = get_config_from_dir(config_path)

    assert isinstance(config, AppConfig)
    assert isinstance(config.spark_configuration.warehouse_dir, str)
    assert config.spark_configuration.app_name == "test"
    assert config.spark_configuration.warehouse_dir == "./tmp/foo/bar"
    assert config.spark_configuration.spark_warehouse_dir_path == "tmp/foo/bar/spark_warehouse"
    assert config.spark_configuration.number_of_cores == 1
    assert config.spark_configuration.delta_configuration.scala_version == "2.12"
    assert config.spark_configuration.delta_configuration.delta_version == "2.0.2"
    assert config.spark_configuration.delta_configuration.snapshot_partitions == 2
    assert config.spark_configuration.delta_configuration.log_cache_size == 3

    assert not config.disable_spark_configuration
    assert config.schema.config_file == "schema_config.yaml"
    assert not config.schema.infer


def test_app_config_has_a_temporary_directory_by_default(data_dir):
    config_path = os.path.join(data_dir, "config", "default_warehouse_temp_dir.yaml")
    config = get_config_from_dir(config_path)

    assert isinstance(config, AppConfig)
    assert isinstance(config.spark_configuration.warehouse_dir, tempfile.TemporaryDirectory)
    assert isinstance(config.spark_configuration.spark_warehouse_dir_path, str)

    path = config.spark_configuration.spark_warehouse_dir_path
    assert "/tmp/" in path or "/var/" in path

    assert not config.disable_spark_configuration
    assert config.schema.config_file == "schema_config.yaml"
    assert not config.schema.infer


def test_app_config_when_spark_configuration_is_disabled(data_dir):
    config_path = os.path.join(data_dir, "config", "disabled_spark_configuration.yaml")
    config = get_config_from_dir(config_path)

    assert isinstance(config, AppConfig)
    assert config.disable_spark_configuration
