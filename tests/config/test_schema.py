import tempfile

import pytest
from schema import SchemaError

from pyspark_data_mocker.config import schema, validate_schema


@pytest.fixture
def mocked_temp_dir(monkeypatch):
    monkeypatch.setattr(tempfile, "TemporaryDirectory", lambda: "/tmp/temp_dir")


def test_app_config_schema_default_values():
    config = schema.app_config_schema.validate({})
    expected = {
        "schema": {"infer": False, "config_file": "schema_config.yaml"},
        "disable_spark_configuration": False,
        "spark_configuration": {},
    }
    assert config == expected


def test_spark_config_schema_default_values(mocked_temp_dir):
    config = schema.spark_conf_schema.validate(
        {
            "app_name": "foo",
            "number_of_cores": 1,
        }
    )
    expected = {"app_name": "foo", "number_of_cores": 1, "enable_hive": False, "warehouse_dir": "/tmp/temp_dir"}
    assert config == expected


def test_validate_schema_does_not_set_spark_configuration_if_disable_spark_is_true():
    config = validate_schema({"disable_spark_configuration": True})
    expected = {"schema": {"infer": False, "config_file": "schema_config.yaml"}, "disable_spark_configuration": True}
    assert config == expected


def test_validate_schema_fails_if_there_no_spark_conf_when_enabled():
    with pytest.raises(SchemaError) as e:
        validate_schema({"disable_spark_configuration": False})
    assert str(e.value) == "Missing keys: 'app_name', 'number_of_cores'"


def test_validate_schema_add_spark_config_defaults_when_enabled(mocked_temp_dir):
    config = validate_schema(
        {"disable_spark_configuration": False, "spark_configuration": {"app_name": "foo", "number_of_cores": 1}}
    )
    expected = {
        "schema": {"infer": False, "config_file": "schema_config.yaml"},
        "disable_spark_configuration": False,
        "spark_configuration": {
            "app_name": "foo",
            "number_of_cores": 1,
            "enable_hive": False,
            "warehouse_dir": "/tmp/temp_dir",
        },
    }
    assert config == expected


def test_validate_schema_returns_successful_configuration():
    expected_config = {
        "schema": {"infer": False, "config_file": "schema_config.yaml"},
        "disable_spark_configuration": False,
        "spark_configuration": {
            "app_name": "foo",
            "number_of_cores": 1,
            "enable_hive": True,
            "warehouse_dir": "/tmp/foo/bar",
            "delta_configuration": {
                "scala_version": "2.11",
                "delta_version": "2.0.2",
                "snapshot_partitions": 1,
                "log_cache_size": 3,
            },
        },
    }

    config = validate_schema(
        {
            "schema": {"config_file": "schema_config.yaml"},
            "spark_configuration": {
                "app_name": "foo",
                "number_of_cores": 1,
                "enable_hive": True,
                "warehouse_dir": "/tmp/foo/bar",
                "delta_configuration": {
                    "scala_version": "2.11",
                    "delta_version": "2.0.2",
                    "snapshot_partitions": 1,
                    "log_cache_size": 3,
                },
            },
        }
    )
    assert config == expected_config


def test_validate_schema_sets_some_default_values(mocked_temp_dir):
    expected_config = {
        "schema": {"infer": False, "config_file": "schema_config.yaml"},
        "disable_spark_configuration": False,
        "spark_configuration": {
            "app_name": "foo",
            "number_of_cores": 1,
            "enable_hive": False,  # default is False
            "warehouse_dir": "/tmp/temp_dir",
            # Delta doesn't have a default configuration
        },
    }

    config = validate_schema(
        {
            "spark_configuration": {
                "app_name": "foo",
                "number_of_cores": 1,
            }
        }
    )
    assert config == expected_config


def test_validate_schema_raises_if_number_of_cores_is_not_valid():
    for n in [0, 9]:
        with pytest.raises(SchemaError) as e_info:
            validate_schema(
                {
                    "spark_configuration": {
                        "app_name": "foo",
                        "number_of_cores": n,
                    }
                }
            )
        assert str(e_info.value) == (
            f"Key 'number_of_cores' error:\n"
            f"validate({n}) raised Exception('Number configured is not between the range [1, 8]')"
        )


def test_validate_schema_raises_if_scala_version_is_not_valid():
    with pytest.raises(SchemaError) as e_info:
        validate_schema(
            {
                "spark_configuration": {
                    "app_name": "foo",
                    "number_of_cores": 1,
                    "delta_configuration": {
                        "scala_version": "2.a",
                        "delta_version": "2.f.0",
                        "snapshot_partitions": 2,
                        "log_cache_size": 2,
                    },
                }
            }
        )
    assert str(e_info.value) == "Scala version must be a supported scala version"


def test_validate_schema_raises_if_delta_version_is_not_valid():
    with pytest.raises(SchemaError) as e_info:
        validate_schema(
            {
                "spark_configuration": {
                    "app_name": "foo",
                    "number_of_cores": 1,
                    "delta_configuration": {
                        "scala_version": "2.11",
                        "delta_version": "3.0.f",
                        "snapshot_partitions": 2,
                        "log_cache_size": 2,
                    },
                }
            }
        )
    assert str(e_info.value) == "Delta version must be a supported version"


def test_validate_schema_raises_if_schema_config_file_is_not_a_yaml_file():
    with pytest.raises(SchemaError) as e_info:
        validate_schema(
            {
                "schema": {"config_file": "schema_config.json"},
                "spark_configuration": {"app_name": "foo", "number_of_cores": 2},
            }
        )
    assert str(e_info.value) == "The config file must be a yaml file"
