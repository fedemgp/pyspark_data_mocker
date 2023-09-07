import pathlib

from pyspark_data_mocker.utils import to_path


def test_to_path():
    str_path = "/tmp/foo/bar"
    new_path = to_path(str_path)

    assert new_path == pathlib.Path("/", "tmp", "foo", "bar")

    path = pathlib.Path("tmp", "foo")
    new_path = to_path(path)
    assert path == new_path
