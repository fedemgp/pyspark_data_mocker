# Basic test (default config)

## Dependency check

```bash
$ pip freeze | grep pyspark
pyspark==3.4.1
<...>
```

```bash
$ tree tests/data/basic_datalake -n --charset=ascii  # byexample: +rm=~
tests/data/basic_datalake
|-- bar
|   |-- courses.csv
|   `-- students.csv
`-- foo
    `-- exams.csv
<...>
```
## Setup
```bash
$ echo "spark_configuration:
>   app_name: test_complete
>   number_of_cores: 4
> schema:
>   infer: true
> " > /tmp/3_4_0_inferring.yaml
```

## Execution
```python
>>> from pyspark_data_mocker import DataLakeBuilder
>>> builder = DataLakeBuilder(app_config="/tmp/3_4_0_inferring.yaml").load_from_dir("./tests/data/basic_datalake")  # byexample: +timeout=20
<...>
```

```python
>>> from pyspark.sql import SparkSession
>>> spark = SparkSession.builder.getOrCreate()
>>> spark.sql("SHOW DATABASES").show()
+---------+
|namespace|
+---------+
|      bar|
|  default|
|      foo|
+---------+
```

```python
>>> spark.sql("SHOW TABLES IN bar").show()
+---------+---------+-----------+
|namespace|tableName|isTemporary|
+---------+---------+-----------+
|      bar|  courses|      false|
|      bar| students|      false|
+---------+---------+-----------+

>>> courses = spark.sql("SELECT * FROM bar.courses")
>>> courses.show()
+---+------------+
| id| course_name|
+---+------------+
|  1|Algorithms 1|
|  2|Algorithms 2|
|  3|  Calculus 1|
+---+------------+


>>> students = spark.table("bar.students")
>>> students.show()
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

```python
>>> spark.sql("SHOW TABLES IN foo").show()
+---------+---------+-----------+
|namespace|tableName|isTemporary|
+---------+---------+-----------+
|      foo|    exams|      false|
+---------+---------+-----------+

>>> exams = spark.table("foo.exams")
>>> exams.show()
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

## Show schema

```python
>>> import pyspark.sql.functions as F
>>> schema = spark.sql("DESCRIBE TABLE EXTENDED bar.courses").select("col_name", "data_type")
>>> schema.filter(F.col("col_name").isin(*courses.columns, "Created By", "Database", "Table", "Type")).show()
+-----------+-----------+
|   col_name|  data_type|
+-----------+-----------+
|         id|        int|
|course_name|     string|
|   Database|        bar|
|      Table|    courses|
| Created By|Spark 3.4.1|
|       Type|    MANAGED|
+-----------+-----------+
```

```python
>>> schema = spark.sql("DESCRIBE TABLE EXTENDED bar.students").select("col_name", "data_type")
>>> schema.filter(F.col("col_name").isin(*students.columns, "Created By", "Database", "Table", "Type")).show()
+----------+-----------+
|  col_name|  data_type|
+----------+-----------+
|        id|        int|
|first_name|     string|
| last_name|     string|
|     email|     string|
|    gender|     string|
|birth_date|       date|
|  Database|        bar|
|     Table|   students|
|Created By|Spark 3.4.1|
|      Type|    MANAGED|
+----------+-----------+
```

```python
>>> schema = spark.sql("DESCRIBE TABLE EXTENDED foo.exams").select("col_name", "data_type")
>>> schema.filter(F.col("col_name").isin(*exams.columns, "Created By", "Database", "Table", "Type")).show()
+----------+-----------+
|  col_name|  data_type|
+----------+-----------+
|        id|        int|
|student_id|        int|
| course_id|        int|
|      date|       date|
|      note|        int|
|  Database|        foo|
|     Table|      exams|
|Created By|Spark 3.4.1|
|      Type|    MANAGED|
+----------+-----------+
```

## Cleanup
```python
>>> builder.cleanup()
>>> spark.sql("SHOW DATABASES").show()
+---------+
|namespace|
+---------+
|  default|
+---------+
```

<!--
# clean previous spark configuration
>>> spark.stop()
>>> SparkSession.builder._options = {}
-->
