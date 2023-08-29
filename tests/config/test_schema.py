import tempfile

import pytest
from schema import SchemaError

from pyspark_data_mocker.config import schema


@pytest.fixture
def mocked_temp_dir(monkeypatch):
    monkeypatch.setattr(tempfile, "TemporaryDirectory", lambda: "/tmp/temp_dir")


def test_config_schema_returns_successful_configuration():
    expected_config = {
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
    }

    config = schema.config_schema.validate(
        {
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
        }
    )
    assert config == expected_config


def test_config_schema_sets_some_default_values(mocked_temp_dir):
    expected_config = {
        "app_name": "foo",
        "number_of_cores": 1,
        "enable_hive": False,  # default is False
        "warehouse_dir": "/tmp/temp_dir",  # default will be a TemporaryDirectory that needs to be transformed to string
        # Delta doesn't have a default configuration
    }

    config = schema.config_schema.validate(
        {
            "app_name": "foo",
            "number_of_cores": 1,
        }
    )
    assert config == expected_config


def test_config_schema_raises_if_number_of_cores_is_not_valid():
    for n in [0, 9]:
        with pytest.raises(SchemaError) as e_info:
            schema.config_schema.validate(
                {
                    "app_name": "foo",
                    "number_of_cores": n,
                }
            )
        assert (
            str(e_info.value) == f"Key 'number_of_cores' error:\nvalidate({n}) raised Exception('Number configured "
            "is not between the range [1, 8]')"
        )


def test_config_schema_raises_if_scala_version_is_not_valid():
    with pytest.raises(SchemaError) as e_info:
        schema.config_schema.validate(
            {
                "app_name": "foo",
                "number_of_cores": 1,
                "delta_configuration": {
                    "scala_version": "2.10",
                    "delta_version": "2.0.0",
                    "snapshot_partitions": 2,
                    "log_cache_size": 2,
                },
            }
        )
    assert (
        str(e_info.value) == "Key 'delta_configuration' error:\nKey 'scala_version' error:\nvalidate('2.10') "
        "raised Exception(\"Version '2.10' it not in the list of supported versions (['2.11', '2.12', '2.13'])\")"
    )


def test_config_schema_raises_if_delta_version_is_not_valid():
    with pytest.raises(SchemaError) as e_info:
        schema.config_schema.validate(
            {
                "app_name": "foo",
                "number_of_cores": 1,
                "delta_configuration": {
                    "scala_version": "2.11",
                    "delta_version": "3.0.0",
                    "snapshot_partitions": 2,
                    "log_cache_size": 2,
                },
            }
        )
    assert (
        str(e_info.value) == "Key 'delta_configuration' error:\nKey 'delta_version' error:\nvalidate('3.0.0') "
        "raised Exception(\"Version '3.0.0' it not in the list of supported versions (['1.1.0', '1.2.0', '1.2.1', "
        "'2.0.0', '2.0.1', '2.0.2', '2.1.0', '2.1.1', '2.2.0', '2.3.0', '2.4.0'])\")"
    )
