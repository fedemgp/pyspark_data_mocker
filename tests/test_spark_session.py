import pytest
from pyspark.sql import SparkSession

from pyspark_data_mocker.config.app_config import DeltaConfig, SparkConfig
from pyspark_data_mocker.spark_session import SparkTestSession


def test_spark_session_without_delta():
    app_config = SparkConfig(
        app_name="test",
        number_of_cores=4,
        warehouse_dir="/tmp/foo/bar",
        delta_configuration=None,
    )
    spark = SparkTestSession(app_config)
    try:
        assert spark.session is not None

        spark_conf = spark.session.conf
        assert spark_conf.get("spark.app.name") == "test"
        assert spark_conf.get("spark.master") == "local[4]"
        assert spark_conf.get("spark.sql.warehouse.dir").endswith("/tmp/foo/bar/spark_warehouse")
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
    finally:
        spark.session.stop()
        # TODO: this is way to ugly but i need it because if not the next tests will use delta configuration
        SparkSession.builder._options = {}


def test_spark_session_with_delta():
    app_config = SparkConfig(
        app_name="foo",
        number_of_cores=1,
        warehouse_dir="/tmp/baz/bar",
        delta_configuration=DeltaConfig(
            scala_version="2.12", delta_version="2.0.2", snapshot_partitions=1, log_cache_size=2
        ),
    )
    spark = SparkTestSession(app_config)
    try:

        assert spark.session is not None

        spark_conf = spark.session.conf
        assert spark_conf.get("spark.app.name") == "foo"
        assert spark_conf.get("spark.master") == "local[1]"
        assert spark_conf.get("spark.sql.warehouse.dir").endswith("/tmp/baz/bar/spark_warehouse")
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
        assert spark_conf.get("spark.sql.catalogImplementation") == "in-memory"
    finally:
        spark.session.stop()
        # TODO: this is way to ugly but i need it because if not the next tests will use delta configuration
        SparkSession.builder._options = {}


@pytest.mark.parametrize(
    "scala_version,delta_version,expected",
    [
        ("2.12", "2.4.1", "delta-core"),
        ("2.12", "2.3.4", "delta-core"),
        ("2.12", "3.0.0", "delta-spark"),
        ("2.12", "4.0.0", "delta-spark"),
    ],
)
def test_get_delta_package(scala_version, delta_version, expected):
    result = SparkTestSession._get_delta_package(scala_version, delta_version)
    assert result == f"io.delta:{expected}_{scala_version}:{delta_version}"
