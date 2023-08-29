from typing import Callable


def range_between(min_value: int, max_value: int) -> Callable:
    """
    Returns a callback that checks if the given configuration number is between the configured range. Raises
    an error that will be caught later in schema module.

    :param min_value:
    :param max_value:
    :return:
    """
    if min_value > max_value:
        raise Exception(f"Bad schema configuration: range is not valid ({min_value=}, {max_value=})")

    def validate(n: int):
        if not min_value <= n <= max_value:
            raise Exception(f"Number configured is not between the range [{min_value}, {max_value}]")
        return True

    return validate


def validate_version(valid_versions: set) -> Callable:
    """
    Given a set of valid versions, return a callback that will be used later for schema validation that raises
    and error with a readable description for the user if the version configured is not valid.
    :param valid_versions:
    :return:
    """

    def validate(version: str):
        if version not in valid_versions:
            raise Exception(f"Version '{version}' it not in the list of supported versions ({sorted(valid_versions)})")
        return True

    return validate
