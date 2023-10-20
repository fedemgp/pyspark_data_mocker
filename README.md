<!--
# To improve the naming of the datalake and avoid refactor the project, move the basic datalake temporally
$ mv tests/data/basic_datalake/bar tests/data/basic_datalake/school
$ mv tests/data/basic_datalake/foo tests/data/basic_datalake/grades
-->
# pyspark-data-mocker
`pyspark-data-mocker` is a testing tool that facilitates the burden of setting up a desired datalake, so you can test
easily the behavior of your data application. It configures also the spark session to optimize it for testing
purpose.

## Install
```
pip install pyspark-data-mocker
```

## Usage
`pyspark-data-mocker` searches the directory you provide in order to seek and load files that can be interpreted as
tables, storing them inside the datalake. That datalake will contain certain databases depending on the folders
inside the root directory. For example, let's take a look into the `basic_datalake`

```bash
$ tree tests/data/basic_datalake -n --charset=ascii  # byexample: +rm=~ +skip
tests/data/basic_datalake
|-- grades
|   `-- exams.csv
`-- school
    |-- courses.csv
    `-- students.csv
~
2 directories, 3 files
```

This file hierarchy will be respected in the further datalake when loaded:  each sub-folder will be considered as
spark database, and each file will be loaded as table, using the filename to name the table.

How can we load them using `pyspark-data-mocker`? Really simple!

```python
>>> from pyspark_data_mocker import DataLakeBuilder
>>> builder = DataLakeBuilder.load_from_dir("./tests/data/basic_datalake")  # byexample: +timeout=20 +pass
```

And that's it! you will now have in that execution context a datalake with the structure defined in the folder
`basic_datalake`. Let's take a closer look by running some queries.

```python
>>> from pyspark.sql import SparkSession
>>> spark = SparkSession.builder.getOrCreate()
>>> spark.sql("SHOW DATABASES").show()
+---------+
|namespace|
+---------+
|  default|
|   grades|
|   school|
+---------+
```

We have the `default` database (which came for free when instantiating spark), and the two folders inside
`tests/data/basic_datalake`: `school` and `grades`.


```python
>>> spark.sql("SHOW TABLES IN school").show()
+---------+---------+-----------+
|namespace|tableName|isTemporary|
+---------+---------+-----------+
|   school|  courses|      false|
|   school| students|      false|
+---------+---------+-----------+

>>> spark.sql("SELECT * FROM school.courses").show()
+---+------------+
| id| course_name|
+---+------------+
|  1|Algorithms 1|
|  2|Algorithms 2|
|  3|  Calculus 1|
+---+------------+


>>> spark.table("school.students").show()
+---+----------+---------+--------------------+------+----------+
| id|first_name|last_name|               email|gender|birth_date|
+---+----------+---------+--------------------+------+----------+
|  1|  Shirleen|  Dunford|sdunford0@amazona...|Female|1978-08-01|
|  2|      Niko|  Puckrin|npuckrin1@shinyst...|  Male|2000-11-28|
|  3|    Sergei|   Barukh|sbarukh2@bizjourn...|  Male|1992-01-20|
|  4|       Sal|  Maidens|smaidens3@senate.gov|  Male|2003-12-14|
|  5|    Cooper|MacGuffie| cmacguffie4@ibm.com|  Male|2000-03-07|
+---+----------+---------+--------------------+------+----------+

```

Note how it is already filled with the data each CSV file has! The tool supports all kind of files: `csv`, `parquet`,
`json`. The application will infer which format to use by looking the file extension.

```python
>>> spark.sql("SHOW TABLES IN grades").show()
+---------+---------+-----------+
|namespace|tableName|isTemporary|
+---------+---------+-----------+
|   grades|    exams|      false|
+---------+---------+-----------+

>>> spark.table("grades.exams").show()
+---+----------+---------+----------+----+
| id|student_id|course_id|      date|note|
+---+----------+---------+----------+----+
|  1|         1|        1|2022-05-01|   9|
|  2|         2|        1|2022-05-08|   7|
|  3|         3|        1|2022-06-17|   4|
|  4|         1|        3|2023-05-12|   9|
|  5|         2|        3|2023-05-12|  10|
|  6|         3|        3|2022-12-07|   7|
|  7|         4|        3|2022-12-07|   4|
|  8|         5|        3|2022-12-07|   2|
|  9|         1|        2|2023-05-01|   5|
| 10|         2|        2|2023-05-07|   8|
+---+----------+---------+----------+----+

```

### Cleanup
You can easily clean the datalake by using the `cleanup` function

```python
>>> builder.cleanup()
>>> spark.sql("SHOW DATABASES").show()
+---------+
|namespace|
+---------+
|  default|
+---------+
```

## Documentation
You can check the full documentation to use all features available in `pyspark-data-mocker` [here](https://fedemgp.github.io/)

<!--
# Restore the previous state
$ mv tests/data/basic_datalake/school tests/data/basic_datalake/bar
$ mv tests/data/basic_datalake/grades tests/data/basic_datalake/foo
-->
