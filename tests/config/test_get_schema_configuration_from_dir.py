import pathlib

import pytest
import schema

from pyspark_data_mocker.config import get_schema_configuration_from_dir


def test_function_returns_a_dictionary_when_config_file_is_correct(data_dir):
    path = pathlib.Path(data_dir, "config", "schema_config.yaml").resolve()
    config = get_schema_configuration_from_dir(path)
    assert isinstance(config, dict)

    assert "bar.courses" in config
    assert config["bar.courses"] == {"id": "int", "course_name": "string"}

    assert "bar.school_students" in config
    assert config["bar.school_students"] == {
        "id": "int",
        "first_name": "string",
        "last_name": "string",
        "email": "string",
        "gender": "string",
    }


def test_function_raises_if_table_name_does_not_follow_the_naming_convention(data_dir):
    path = pathlib.Path(data_dir, "config", "invalid_table_name.yaml").resolve()
    with pytest.raises(ValueError) as e_info:
        get_schema_configuration_from_dir(path)

    assert (
        str(e_info.value)
        == "Invalid parameter: Table 'barcourses' name must follow the form <database>.<table> name convention"
    )


def test_function_raises_if_column_schema_doesnt_follow_ddl_convention(data_dir):
    # TODO: for now, it only checks that the column config is a string. To be able to detect the DDL is well defined
    #       it is needed to instantiate the spark engine. We cannot do that at the point where we load configuration.
    #       review this to be able to check that
    path = pathlib.Path(data_dir, "config", "invalid_column_schema.yaml").resolve()
    with pytest.raises(schema.SchemaError) as e_info:
        get_schema_configuration_from_dir(path)

    assert str(e_info.value) == "Key 'bar.courses' error:\nKey 'id' error:\n1 should be instance of 'str'"
