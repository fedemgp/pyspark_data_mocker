from pathlib import Path
from typing import Union

PathLike = Union[str, Path]


def to_path(path: PathLike) -> Path:
    """
    Transforms a PathLike class (that can be a pathlib.Path or a string) into a pathlib.Path

    :param path: PathLike   the path to transform
    :return:
    """
    if isinstance(path, Path):
        return path

    return Path(path)


def dict_to_ddl_string(table_definition: dict) -> str:
    """
    Given a table definition, make a string with the ddl for pyspark, to be able to infer the schema of the dataframe

    Example:
        Considering this table definition:
            {
                "id": "int",
                "start_date": "date",
                "metadata": "map<string, string>",
                "name": "string"
            }
        this function will return
        "id string, start_date date, metadata map<string, string>, name string"
    :param table_definition:
    :return:
    """
    rows = [f"{col_name} {col_type}" for col_name, col_type in table_definition.items()]
    return ", ".join(rows)
