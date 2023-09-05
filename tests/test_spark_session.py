from pyspark_data_mocker.config import AppConfig
from pyspark_data_mocker.config.app_config import DeltaConfig
from pyspark_data_mocker.spark_session import SparkTestSession


def test_spark_session_without_delta():
    app_config = AppConfig(
        app_name="test", number_of_cores=4, enable_hive=False, warehouse_dir="/tmp/foo/bar", delta_configuration=None
    )
    spark = SparkTestSession(app_config)
    assert spark.session is not None

    spark_conf = spark.session.conf
    assert spark_conf.get("spark.app.name") == "test"
    assert spark_conf.get("spark.master") == "local[4]"
    assert spark_conf.get("spark.sql.warehouse.dir") == "/tmp/foo/bar/spark_warehouse"
    assert spark_conf.get("spark.sql.shuffle.partitions") == "1"

    # There is no delta configuration
    assert spark_conf.get("spark.sql.extensions") is None
    assert spark_conf.get("spark.sql.catalog.spark_catalog") is None

    assert spark_conf.get("spark.ui.showConsoleProgress") == "false"
    assert spark_conf.get("spark.ui.enabled") == "false"
    assert spark_conf.get("spark.ui.dagGraph.retainedRootRDDs") == "1"
    assert spark_conf.get("spark.ui.retainedJobs") == "1"
    assert spark_conf.get("spark.ui.retainedStages") == "1"
    assert spark_conf.get("spark.ui.retainedTasks") == "1"
    assert spark_conf.get("spark.sql.ui.retainedExecutions") == "1"
    assert spark_conf.get("spark.worker.ui.retainedExecutors") == "1"
    assert spark_conf.get("spark.worker.ui.retainedDrivers") == "1"
    # Hive is not enabled
    assert spark_conf.get("spark.sql.catalogImplementation") == "in-memory"


def test_spark_session_with_delta():
    app_config = AppConfig(
        app_name="foo",
        number_of_cores=1,
        enable_hive=True,
        warehouse_dir="/tmp/baz/bar",
        delta_configuration=DeltaConfig(
            scala_version="2.12", delta_version="2.0.2", snapshot_partitions=1, log_cache_size=2
        ),
    )
    spark = SparkTestSession(app_config)
    assert spark.session is not None

    spark_conf = spark.session.conf
    assert spark_conf.get("spark.app.name") == "foo"
    assert spark_conf.get("spark.master") == "local[1]"
    assert spark_conf.get("spark.sql.warehouse.dir") == "/tmp/baz/bar/spark_warehouse"
    assert spark_conf.get("spark.sql.shuffle.partitions") == "1"

    # Delta is enabled
    assert spark_conf.get("spark.jars.packages") == "io.delta:delta-core_2.12:2.0.2"
    assert spark_conf.get("spark.sql.extensions") == "io.delta.sql.DeltaSparkSessionExtension"
    assert spark_conf.get("spark.databricks.delta.snapshotPartitions") == "1"
    assert spark_conf.get("spark.sql.catalog.spark_catalog") == "org.apache.spark.sql.delta.catalog.DeltaCatalog"

    assert spark_conf.get("spark.ui.showConsoleProgress") == "false"
    assert spark_conf.get("spark.ui.enabled") == "false"
    assert spark_conf.get("spark.ui.dagGraph.retainedRootRDDs") == "1"
    assert spark_conf.get("spark.ui.retainedJobs") == "1"
    assert spark_conf.get("spark.ui.retainedStages") == "1"
    assert spark_conf.get("spark.ui.retainedTasks") == "1"
    assert spark_conf.get("spark.sql.ui.retainedExecutions") == "1"
    assert spark_conf.get("spark.worker.ui.retainedExecutors") == "1"
    assert spark_conf.get("spark.worker.ui.retainedDrivers") == "1"
    # Hive is enabled
    assert spark_conf.get("spark.sql.catalogImplementation") == "hive"
