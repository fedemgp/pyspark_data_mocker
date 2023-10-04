from pathlib import Path

import pyspark.sql.utils
import pytest
from chispa import assert_df_equality
from pyspark.sql import SparkSession

from pyspark_data_mocker import config
from pyspark_data_mocker.datalake_builder import DataLakeBuilder
from pyspark_data_mocker.spark_session import SparkTestSession


def test_build_datalake_using_dsl(data_dir, data_mocker):
    spark = SparkSession.builder.getOrCreate()
    base_path = Path(data_dir, "basic_datalake", "bar")
    (
        data_mocker.with_db("db1")
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
            ("1", "Shirleen", "Dunford", "sdunford0@amazonaws.com", "Female", "1978-08-01"),
            ("2", "Niko", "Puckrin", "npuckrin1@shinystat.com", "Male", "2000-11-28"),
            ("3", "Sergei", "Barukh", "sbarukh2@bizjournals.com", "Male", "1992-01-20"),
            ("4", "Sal", "Maidens", "smaidens3@senate.gov", "Male", "2003-12-14"),
            ("5", "Cooper", "MacGuffie", "cmacguffie4@ibm.com", "Male", "2000-03-07"),
        ],
        schema=["id", "first_name", "last_name", "email", "gender", "birth_date"],
    )
    assert_df_equality(df, expected)
    assert True


def test_creating_table_with_non_existing_db_raises(data_dir):
    builder = DataLakeBuilder(SparkTestSession(config.default_config()))
    base_path = Path(data_dir, "basic_datalake", "bar")
    builder = builder.with_table("a_table_name", fmt="csv", path=Path(base_path, "courses.csv"), db_name="db1")

    with pytest.raises(pyspark.sql.utils.AnalysisException) as error:
        builder.run()

    assert str(error.value) == "Database 'db1' not found"
