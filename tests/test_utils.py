import pathlib

from pyspark_data_mocker import utils


def test_to_path():
    str_path = "/tmp/foo/bar"
    new_path = utils.to_path(str_path)

    assert new_path == pathlib.Path("/", "tmp", "foo", "bar")

    path = pathlib.Path("tmp", "foo")
    new_path = utils.to_path(path)
    assert path == new_path


def test_dict_to_ddl_string():
    result = utils.dict_to_ddl_string(
        {"id": "int", "start_date": "date", "metadata": "map<string, string>", "name": "string"}
    )
    assert result == "id int, start_date date, metadata map<string, string>, name string"
