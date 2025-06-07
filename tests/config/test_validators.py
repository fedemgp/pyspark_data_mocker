from typing import Callable

import pytest

from pyspark_data_mocker.config import validators


def test_range_between_raises_if_range_is_bad_configured():
    with pytest.raises(Exception) as e_info:
        validators.range_between(2, 1)

    assert str(e_info.value) == "Bad schema configuration: range is not valid (min_value=2, max_value=1)"


def test_range_between_returns_callback_that_checks_if_the_given_parameter_is_between_range():
    validator = validators.range_between(2, 5)
    assert isinstance(validator, Callable)
    assert validator(3)

    # it raises if parameter is not in the range
    with pytest.raises(Exception) as e_info:
        validator(1)

    assert str(e_info.value) == "Number configured is not between the range [2, 5]"
