# Configuration
## Default configuration
`pyspark-data-mocker` configures spark in a way that optimize tests executions.

``` python
>>> from pyspark_data_mocker import DataLakeBuilder
>>> builder = DataLakeBuilder.load_from_dir("./tests/data/basic_datalake")  # byexample: +timeout=20 +pass
>>> spark = builder.spark
>>> spark_conf = spark.conf
```

```python
>>> spark_conf.get("spark.app.name")
'test'
>>> spark_conf.get("spark.master")  # 1 thread for the execution
'local[1]'
>>> spark_conf.get("spark.sql.warehouse.dir")  # Temporal directory to store the data warehouse
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

## Custom configuration

Some of these configurations can be overridden by providing a config yaml file. For example lets build a custom
configuration.

```bash
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
```

<!--
# clean previous spark configuration
>>> from pyspark.sql import SparkSession
>>> spark.stop()
>>> SparkSession.builder._options = {}
-->

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

Note that now the spark session now use 4 CPU cores, the delta framework is enabled, and it uses the `hive` catalog
implementation.

## Configuration file explanation
¿But, what do those values represent? Let's take a closer look on the levers that we can control in this configuration
file 

| config name           | type         | description                                                                                                                                                                                                                                                                  | default value                 |
|-----------------------|--------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------|
| `number_of_cores`     | INTEGER      | change the amount of CPU cores  The spark session will use                                                                                                                                                                                                                   | 1                             |
| `enable_hive`         | BOOL         | Enables the usage of [Apache Hive's catalog](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.SparkSession.builder.enableHiveSupport.html)                                                                                                           | false                         |
| `warehouse_dir`       | STRING       | If set, it will create a persistent directory where the wharehouse will live. By default `pyspark_data_mocker` uses a [TemporaryDirectory](https://docs.python.org/3/library/tempfile.html#tempfile.TemporaryDirectory) that will exists as long the builder instance exists | tempfile.TemporaryDirectory() |
| `schema_config_file_name` | STRING | Yaml file to Configure the schema of each table the datalake will have. By default `pyspark_data_mocker` will search for a `schema_config.yaml` file. See [Schema infering section for further details](https://fedemgp.github.io/Documentation/schema_infering/)                                     | schema_config.yaml |
| `delta_configuration` | DELTA_CONFIG | If set, it will enable [Delta Lake framework](https://delta.io/)                                                                                                                                                                                                             | None                          |

Among the things you can change when enabling Delta capabilities are:

| config name           | type     | description                                                                                                                                                                                                    |
|-----------------------|----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `scala_version`       | STRING   | Version of Scala that the spark session will use. Take into consideration that the scala version [MUST be compatible](https://mvnrepository.com/artifact/io.delta/delta-core) with the Delta-core version used |
| `delta_version`       | STRING   | Version of delta core used. The version used highly depends [on the pyspark version](https://docs.delta.io/latest/releases.html)                                                                               |
| `snapshot_partitions` | INTEGER  | Tells delta how should the partitions be done                                                                                                                                                                  |
| `log_cache_size`      | INTEGER  | Limits the Delta log cache                                                                                                                                                                                     |

> **Important note:** If you enable Delta capabilities, check your pyspark version, and configure the right value of
scala and delta version.

> **Important note 2:** For the delta configuration, take into consideration that ALL VALUES should be explicitly set-up, there is no default
value for each one of them.
