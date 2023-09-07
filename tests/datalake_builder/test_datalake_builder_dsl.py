from pathlib import Path

import pyspark.sql.utils
import pytest
from chispa import assert_df_equality

from pyspark_data_mocker.datalake_builder import DataLakeBuilder


def test_build_datalake_using_dsl(spark_test, data_dir):
    builder = DataLakeBuilder(spark_test)
    spark = spark_test.session
    base_path = Path(data_dir, "basic_datalake", "bar")
    builder = (
        builder.with_db("db1")
        .with_table("a_table_name", fmt="csv", path=Path(base_path, "courses.csv"), db_name="db1")
        .with_table("a_table_with_students", fmt="csv", path=Path(base_path, "students.csv"))
        .run()
    )

    df = spark.table("db1.a_table_name")
    expected = spark.createDataFrame(
        data=[("1", "Algorithms 1"), ("2", "Algorithms 2"), ("3", "Calculus 1")], schema=["id", "course_name"]
    )
    assert_df_equality(df, expected)

    df = spark.table("default.a_table_with_students")
    expected = spark.createDataFrame(
        data=[
            ("1", "Shirleen", "Dunford", "sdunford0@amazonaws.com", "Female"),
            ("2", "Niko", "Puckrin", "npuckrin1@shinystat.com", "Male"),
            ("3", "Sergei", "Barukh", "sbarukh2@bizjournals.com", "Male"),
            ("4", "Sal", "Maidens", "smaidens3@senate.gov", "Male"),
            ("5", "Cooper", "MacGuffie", "cmacguffie4@ibm.com", "Male"),
        ],
        schema=["id", "first_name", "last_name", "email", "gender"],
    )
    assert_df_equality(df, expected)


def test_creating_table_with_non_existing_db_raises(spark_test, data_dir):
    builder = DataLakeBuilder(spark_test)
    spark = spark_test.session  # noqa: F841
    base_path = Path(data_dir, "basic_datalake", "bar")
    builder = builder.with_table("a_table_name", fmt="csv", path=Path(base_path, "courses.csv"), db_name="db1")

    with pytest.raises(pyspark.sql.utils.AnalysisException) as error:
        builder.run()

    assert str(error.value) == "Database 'db1' not found"
