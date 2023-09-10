# pyspark-data-mocker
`pyspark-data-mocker` is a testing tool that facilitates the burden of set up an entire datalake, so you can test
easily the behavior of your data application. It configures also the spark session to optimize it for testing
purpose.

## Install
```
pip install pyspark-data-mocker
```

## Usage
`pyspark-data-mocker` searchs the directory you provide in order to seek and load files that can be interpreted as
tables, that will live inside the current datalake. That datalake will contain certain database depending on the folders
inside the root directory. For example, let's take a look into the `basic_datalake`

```bash
$ tree tests/data/basic_datalake -n --charset=ascii  # byexample: +rm=~
tests/data/basic_datalake
|-- bar
|   |-- courses.csv
|   `-- students.csv
`-- foo
    `-- exams.csv
~
2 directories, 3 files
```

This file hierarchy will be respected in the further datalake when loaded:  each subfolder will be considered as
spark's databases, and each file will be loaded as table, using the filename to name the table.

How can we load then using `pyspark-data-mocker`? Really simple!

```python
>>> from pyspark_data_mocker import DataLakeBuilder
>>> builder = DataLakeBuilder.load_from_dir("./tests/data/basic_datalake")  # byexample: +timeout=20 +pass
```


And that's it! you will now have in that context a datalake with the structure defined in the folder `basic_datalake`.
Let's take a closer look on the datalake by running some queries.
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

We have the `default` database (which came for free when instantiating spark), and the two folders inside
`tests/data/basic_datalake`: `bar` and `foo`.


```python
>>> spark.sql("SHOW TABLES IN bar").show()
+---------+---------+-----------+
|namespace|tableName|isTemporary|
+---------+---------+-----------+
|      bar|  courses|      false|
|      bar| students|      false|
+---------+---------+-----------+

>>> spark.sql("SELECT * FROM bar.courses").show()
+---+------------+
| id| course_name|
+---+------------+
|  1|Algorithms 1|
|  2|Algorithms 2|
|  3|  Calculus 1|
+---+------------+


>>> spark.table("bar.students").show()
+---+----------+---------+--------------------+------+
| id|first_name|last_name|               email|gender|
+---+----------+---------+--------------------+------+
|  1|  Shirleen|  Dunford|sdunford0@amazona...|Female|
|  2|      Niko|  Puckrin|npuckrin1@shinyst...|  Male|
|  3|    Sergei|   Barukh|sbarukh2@bizjourn...|  Male|
|  4|       Sal|  Maidens|smaidens3@senate.gov|  Male|
|  5|    Cooper|MacGuffie| cmacguffie4@ibm.com|  Male|
+---+----------+---------+--------------------+------+

```

```python
>>> spark.sql("SHOW TABLES IN foo").show()
+---------+---------+-----------+
|namespace|tableName|isTemporary|
+---------+---------+-----------+
|      foo|    exams|      false|
+---------+---------+-----------+

>>> spark.table("foo.exams").show()
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

## Configuration

<!--
$ cat pyspark_data_mocker/config/app_config.py  # byexample: +rm=~
<...>
@dataclasses.dataclass
class AppConfig:
    app_name: str
    number_of_cores: int
    enable_hive: bool
    warehouse_dir: Dir
    delta_configuration: Optional["DeltaConfig"]
<...>
@dataclasses.dataclass
class DeltaConfig:
    scala_version: str
    delta_version: str
    snapshot_partitions: int
    log_cache_size: int
-->

`pyspark-data-mocker` has a default spark configuration that optimize tests executions.

```python
>>> spark_conf = spark.conf
>>> spark_conf.get("spark.app.name")
'test'
>>> spark_conf.get("spark.master")
'local[1]'
>>> spark_conf.get("spark.sql.warehouse.dir")
'/tmp/tmp<...>/spark_warehouse'
>>> spark_conf.get("spark.sql.shuffle.partitions")
'1'

>>> spark_conf.get("spark.ui.showConsoleProgress")
'false'

>>> spark_conf.get("spark.ui.enabled")
'false'
>>> spark_conf.get("spark.ui.dagGraph.retainedRootRDDs")
'1'
>>> spark_conf.get("spark.ui.retainedJobs")
'1'
>>> spark_conf.get("spark.ui.retainedStages")
'1'
>>> spark_conf.get("spark.ui.retainedTasks")
'1'
>>> spark_conf.get("spark.sql.ui.retainedExecutions")
'1'
>>> spark_conf.get("spark.worker.ui.retainedExecutors")
'1'
>>> spark_conf.get("spark.worker.ui.retainedDrivers")
'1'

>>> spark_conf.get("spark.sql.catalogImplementation")
'in-memory'
```

To better understand what these configuration means and why it is configured like this, you can take a look
on Sergey Ivanychev's excellent research on ["Faster PySpark Unit Test"](https://medium.com/constructor-engineering/faster-pyspark-unit-tests-1cb7dfa6bdf6)


Some of these configurations can be overridden by providing a config yaml file. For example

<!--
$ echo "app_name: test_complete
> number_of_cores: 4
> enable_hive: True
> warehouse_dir: "/tmp/full_delta_lake"
> delta_configuration:
>     scala_version: '2.12'
>     delta_version: '2.0.2'
>     snapshot_partitions: 2
>     log_cache_size: 3
> " > /tmp/custom_config.yaml

# clean previous spark configuration
>>> spark.stop()
>>> SparkSession.builder._options = {}
-->

```bash
$ cat /tmp/custom_config.yaml
app_name: test_complete
number_of_cores: 4
enable_hive: True
warehouse_dir: /tmp/full_delta_lake
delta_configuration:
    scala_version: '2.12'
    delta_version: '2.0.2'
    snapshot_partitions: 2
    log_cache_size: 3

```

Let's digest each value and what it controls:

| config name           | type         | description                                                                                                                                                                                                                                                                  | default value                 |
|-----------------------|--------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------|
| `number_of_cores`     | INTEGER      | change the amount of CPU cores  The spark session will use                                                                                                                                                                                                                   | 1                             |
| `enable_hive`         | BOOL         | Enables the usage of [Apache Hive's catalog](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.SparkSession.builder.enableHiveSupport.html)                                                                                                           | false                         |
| `warehouse_dir`       | STRING       | If set, it will create a persistent directory where the wharehouse will live. By default `pyspark_data_mocker` uses a [TemporaryDirectory](https://docs.python.org/3/library/tempfile.html#tempfile.TemporaryDirectory) that will exists as long the builder instance exists | tempfile.TemporaryDirectory() |
| `delta_configuration` | DELTA_CONFIG | If set, it will enable [Delta Lake framework](https://delta.io/)                                                                                                                                                                                                             | None                          |

Among the things you can change when enabling Delta capabilities are:

| config name           | type     | description                                                                                                                                                                                                     |
|-----------------------|----------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `scala_version`       | STRING   | Version of Scala that the spark session will use. Thake into consideration that the scala version [MUST be compatible](https://mvnrepository.com/artifact/io.delta/delta-core) with the Delta-core version used |
| `delta_version`       | STRING   | Version of delta core used. The version used highly depends [on the pyspark version](https://docs.delta.io/latest/releases.html)                                                                                |
| `snapshot_partitions` | INTEGER  | Tells delta how should the partitions be done                                                                                                                                                                   |
| `log_cache_size`      | INTEGER  | Limits the Delta log cache                                                                                                                                                                                      |

For the delta configuration, take into consideration that ALL VALUES should be explicitly set-up, there is no default
value for each one of them.

To use a custom configuration, you can pass a `string` or `pathlib.Path` optional argument to `load_from_dir`.

```python
>>> builder = DataLakeBuilder.load_from_dir("./tests/data/basic_datalake", "/tmp/custom_config.yaml")  # byexample: +timeout=20
<...>
>>> spark_conf = SparkSession.builder.getOrCreate().conf
>>> spark_conf.get("spark.app.name")
'test_complete'
>>> spark_conf.get("spark.master")
'local[4]'
>>> spark_conf.get("spark.sql.warehouse.dir")
'/tmp/full_delta_lake/spark_warehouse'

>>> spark_conf.get("spark.jars.packages")
'io.delta:delta-core_2.12:2.0.2'
>>> spark_conf.get("spark.sql.extensions")
'io.delta.sql.DeltaSparkSessionExtension'
>>> spark_conf.get("spark.databricks.delta.snapshotPartitions")
'2'
>>> spark_conf.get("spark.sql.catalog.spark_catalog")
'org.apache.spark.sql.delta.catalog.DeltaCatalog'

>>> spark_conf.get("spark.sql.catalogImplementation")
'hive'
```
