import pathlib
from pathlib import Path

import pytest
from chispa import assert_df_equality
from pyspark.sql import SparkSession

from pyspark_data_mocker.datalake_builder import DataLakeBuilder


def test_load_from_dir_creates_a_database_per_directory(data_dir):
    # It is needed to keep a reference of the builder to avoid a clean of the GC and avoid the temporal dir to be erased
    builder = DataLakeBuilder.load_from_dir(Path(data_dir, "basic_datalake"))  # noqa: F841
    # This will use the same spark session as the one configured in the DataLakeBuilder
    spark = SparkSession.builder.getOrCreate()

    # There are three databases: bar, foo and default (this last one is for free thanks to spark)
    df = spark.sql("SHOW DATABASES")
    expected = spark.createDataFrame(
        data=[["bar"], ["default"], ["foo"]],
        schema=["namespace"],
    )
    assert_df_equality(df, expected, ignore_nullable=True)

    # There is no table in 'default' database
    assert len(spark.sql("SHOW TABLES IN default").collect()) == 0

    # bar has two tables
    df = spark.sql("SHOW TABLES IN bar")
    expected = spark.createDataFrame(
        data=[("bar", "courses", False), ("bar", "students", False)],
        schema=["namespace", "tableName", "isTemporary"],
    )
    assert_df_equality(df, expected, ignore_nullable=True)

    # foo has a single table
    df = spark.sql("SHOW TABLES IN foo")
    expected = spark.createDataFrame(
        data=[("foo", "exams", False)],
        schema=["namespace", "tableName", "isTemporary"],
    )
    assert_df_equality(df, expected, ignore_nullable=True)


def test_load_from_dir_creates_a_table_for_each_file_in_the_given_database(data_dir):
    builder = DataLakeBuilder.load_from_dir(Path(data_dir, "basic_datalake"))  # noqa: F841
    # This will
    spark = SparkSession.builder.getOrCreate()

    df = spark.table("bar.courses")
    expected = spark.createDataFrame(
        data=[("1", "Algorithms 1"), ("2", "Algorithms 2"), ("3", "Calculus 1")], schema=["id", "course_name"]
    )
    assert_df_equality(df, expected)

    df = spark.table("bar.students")
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

    df = spark.table("foo.exams")
    expected = spark.createDataFrame(
        data=[
            ("1", "1", "1", "2022-05-01", "9"),
            ("2", "2", "1", "2022-05-08", "7"),
            ("3", "3", "1", "2022-06-17", "4"),
            ("4", "1", "3", "2023-05-12", "9"),
            ("5", "2", "3", "2023-05-12", "10"),
            ("6", "3", "3", "2022-12-07", "7"),
            ("7", "4", "3", "2022-12-07", "4"),
            ("8", "5", "3", "2022-12-07", "2"),
            ("9", "1", "2", "2023-05-01", "5"),
            ("10", "2", "2", "2023-05-07", "8"),
        ],
        schema=["id", "student_id", "course_id", "date", "note"],
    )
    assert_df_equality(df, expected)


def test_load_from_dir_raises_if_path_is_not_valid():
    with pytest.raises(ValueError) as error:
        DataLakeBuilder.load_from_dir("/tmp/foo/bar")

    assert str(error.value) == "The path provided '/tmp/foo/bar' does not exists"


def test_load_from_dir_raises_if_path_is_not_a_directory(data_dir):
    with pytest.raises(ValueError) as error:
        DataLakeBuilder.load_from_dir(pathlib.Path(data_dir, "basic_datalake", "bar", "courses.csv"))

    assert (
        str(error.value)
        == f"The path '{data_dir}/basic_datalake/bar/courses.csv' is not a directory with a delta lake data"
    )


def test_load_from_dir_creates_tables_in_default_database_if_it_is_in_root_directory(data_dir):
    builder = DataLakeBuilder.load_from_dir(Path(data_dir, "datalake_with_default_tables"))  # noqa: F841
    # This will
    spark = SparkSession.builder.getOrCreate()

    df = spark.table("bar.courses")
    expected = spark.createDataFrame(
        data=[("1", "Algorithms 1"), ("2", "Algorithms 2"), ("3", "Calculus 1")], schema=["id", "course_name"]
    )
    assert_df_equality(df, expected)

    df = spark.table("bar.students")
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

    df = spark.table("exams")
    expected = spark.createDataFrame(
        data=[
            ("1", "1", "1", "2022-05-01", "9"),
            ("2", "2", "1", "2022-05-08", "7"),
            ("3", "3", "1", "2022-06-17", "4"),
            ("4", "1", "3", "2023-05-12", "9"),
            ("5", "2", "3", "2023-05-12", "10"),
            ("6", "3", "3", "2022-12-07", "7"),
            ("7", "4", "3", "2022-12-07", "4"),
            ("8", "5", "3", "2022-12-07", "2"),
            ("9", "1", "2", "2023-05-01", "5"),
            ("10", "2", "2", "2023-05-07", "8"),
        ],
        schema=["id", "student_id", "course_id", "date", "note"],
    )
    assert_df_equality(df, expected)

    # This is the same as "select * from exams"
    df = spark.table("default.exams")
    assert_df_equality(df, expected)
