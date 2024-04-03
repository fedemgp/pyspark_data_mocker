# Configuration
## Default configuration
`pyspark-data-mocker` configures spark in a way that optimize tests executions.

``` python
>>> from pyspark_data_mocker import DataLakeBuilder
>>> builder = DataLakeBuilder().load_from_dir("./tests/data/basic_datalake")  # byexample: +timeout=20 +pass
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
on Sergey Ivanychev's excellent research on ["Faster PySpark Unit Test"](https://medium.com/constructor-engineering/faster-pyspark-unit-tests-1cb7dfa6bdf6).

## Custom configuration

Some of these configurations can be overridden by providing a config yaml file. For example lets build a custom
configuration.

```bash
$ echo "
> spark_configuration:
>    app_name: test_complete
>    number_of_cores: 4
>    enable_hive: True
>    warehouse_dir: "/tmp/full_delta_lake"
>    delta_configuration:
>        scala_version: '2.12'
>        delta_version: '2.0.2'
>        snapshot_partitions: 2
>        log_cache_size: 3
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
>>> builder = DataLakeBuilder(app_config="/tmp/custom_config.yaml").load_from_dir("./tests/data/basic_datalake")  # byexample: +timeout=20
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
<!--
# Validate that the dataclass didn't change, and alert me if it did to update this part of the documentation
$ cat ./pyspark_data_mocker/config/app_config.py  # byexample: +rm=~
<...>
@dataclasses.dataclass
class AppConfig:
    disable_spark_configuration: bool
    schema: "SchemaConfig"
    spark_configuration: Optional["SparkConfig"] = None
<...>
@dataclasses.dataclass
class SparkConfig:
    app_name: str
    number_of_cores: int
    enable_hive: bool
    warehouse_dir: Dir
    delta_configuration: Optional["DeltaConfig"] = None
<...>
@dataclasses.dataclass
class DeltaConfig:
    scala_version: str
    delta_version: str
    snapshot_partitions: int
    log_cache_size: int
<...>
@dataclasses.dataclass
class SchemaConfig:
    infer: bool
    config_file: str
<...>
-->
¿But, what do those values represent? Let's take a closer look on the levers that we can control in this configuration
file 
### App configuration

| config name                   | type          | default value   | description                                                                                                                                       |
|-------------------------------|---------------|-----------------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| `schema`                      | SCHEMA_CONFIG | DEFAULT_CONFIG  | Schema configuration. You can set a custom yaml where you will define the schema of each table, or let spark to infer it. More info below         |
| `disable_spark_configuration` | BOOL          | False           | If set to true, then all spark optimization mentioned before will be disabled. It is responsability of the developer to configure spark as he wish |
| `spark_configuration`         | SPARK_CONFIG  |                 | A reduced amount of levers to modify the spark configuration recommended for tests.                                                               |

### Schema configuration
Inside the app configuration, there is a special configuration for the schema. There you can set these options as you
please

| config name   | type   | default            | description                                           |
|---------------|--------|--------------------|-------------------------------------------------------|
| `infer`       | BOOL   | false              | Enable automatic column type infering                 |
| `config_file` | STRING | schema_config.yaml | Config file name to read for manual schema definition |

More about schema inferring can be seen [here](https://fedemgp.github.io/Documentation/schema_infering/)


### Spark configuration
This parameter is desired if you want to let `pyspark-data-mocker` to handle the spark configuration for you.
It tries to abstract the user how the session should be and let him concentrate on define good tests without worrying
much about performance and fine-tuning.


| config name            | type          | default value                 | description                                                                                                                                                                                                                                                                 |
|------------------------|---------------|-------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `number_of_cores`      | INTEGER       | 1                             | change the amount of CPU cores  The spark session will use                                                                                                                                                                                                                  |
| `enable_hive`          | BOOL          | false                         | Enables the usage of [Apache Hive's catalog](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.SparkSession.builder.enableHiveSupport.html)                                                                                                          |
| `warehouse_dir`        | STRING        | tempfile.TemporaryDirectory() | If set, it will create a persistent directory where the wharehouse will live. By default `pyspark_data_mocker` uses a [TemporaryDirectory](https://docs.python.org/3/library/tempfile.html#tempfile.TemporaryDirectory) that will exists as long the builder instance exists |
| `delta_configuration`  | DELTA_CONFIG  | None                          | If set, it will enable [Delta Lake framework](https://delta.io/)                                                                                                                                                                                                            |

#### Delta configuration
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


### Disable spark optimizations
One advance developer may ask by himself: "why are you obfuscating me how the spark session is configured? Let me
handle it". To that advance user , **you can disable the automatic spark configuration** by setting as `True` the
value `disable_spark_configuration`.

That Engineer is responsible to configure spark before using this package, using the jars he wants. Here we
recommend to still stick to the recommendations commented [here](https://medium.com/constructor-engineering/faster-pyspark-unit-tests-1cb7dfa6bdf6)
in order to make the tests as fast as possible. Take in mind that the default spark configuration behaves poorly when
handling a low amount of data, and if you write a considerable amount of test, the pipeline
that run all the test suit may take forever!.
