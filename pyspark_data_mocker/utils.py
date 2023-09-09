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
