import logging
from tempfile import TemporaryDirectory
from typing import Union

from pyspark.sql import SparkSession

from pyspark_data_mocker.config.app_config import SparkConfig

Dir = Union[str, TemporaryDirectory]


class SparkTestSession:
    def __init__(self, config: SparkConfig):
        self.log = logging.getLogger()
        self.config = config
        self._session = self.__create_session(config)

    def __create_session(self, config: SparkConfig) -> SparkSession:
        """
        Get a spark_session. If the config argument is defined, then the session will
        be optimized for testing porpuses. In the case the optimization is disabled (by sending
        a None config) then is responsability of the developer to configure Spark as you wish.

        :param config:
        :return:
        """
        builder = SparkSession.builder

        if config:
            builder = self.__configure_spark(config)

        return builder.getOrCreate()

    def __configure_spark(self, config: SparkConfig) -> SparkSession.Builder:
        """
        Creates a spark session with configuration that improves the local setup and
        execution. This is intended because the datalake that will be mocked for tests will have usually low
        amount of data. If we use the default spark configuration its probably that the tests takes too long
        to run due to unnecessary optimizations for big data.

        Special Thanks to Sergey Ivanychev for his research on "Faster PySpark Unit Test"
        [blog](https://medium.com/constructor-engineering/faster-pyspark-unit-tests-1cb7dfa6bdf6)

        :param config: Application configuration
        :return: a new spark session configured to be performant for tests
        """
        self.log.info(f"Creating session of name '{config.app_name}' with {config.number_of_cores} cores")
        builder = (
            SparkSession.builder.master(f"local[{config.number_of_cores}]")
            .appName(config.app_name)
            .config("spark.sql.warehouse.dir", config.spark_warehouse_dir_path)
        )

        self.log.info(f"Spark warehouse dir: {config.spark_warehouse_dir_path}")
        # The default value of this config is 200, that is too much for local tests that will not handle
        # so much data.
        builder = builder.config("spark.sql.shuffle.partitions", 1)

        # accumulate all JVM options and at the end set the JVM configuration
        # -XX:+CMSClassUnloadingEnabled: allows JVM garbage collector to remove unused classes that spark generates
        # -XX:+UseCompressedOops: tells JVM to use 32-bit addresses instead of 64 (if you don't use more that 32GB of
        #                         ram there would not be any problem
        jvm_options = ["-XX:+CMSClassUnloadingEnabled", "-XX:+UseCompressedOops"]

        # Enable delta optimizations if configured
        if config.delta_configuration:
            dconfig = config.delta_configuration
            self.log.info(f"Enabling delta configuration using delta version '{dconfig.delta_version}'")
            delta_package = f"io.delta:delta-core_{dconfig.scala_version}:{dconfig.delta_version}"
            builder = (
                builder.config("spark.jars.packages", delta_package)
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.databricks.delta.snapshotPartitions", dconfig.snapshot_partitions)
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            )
            jvm_options.append(f"-Ddelta.log.cacheSize={dconfig.log_cache_size}")

        builder = (
            # disable console progress bar, TODO: maybe add configure this using the config file
            builder.config("spark.ui.showConsoleProgress", "false")
            # Disable spark ui to reduce RAM and lower the local HTTP request to the server
            .config("spark.ui.enabled", "false")
            .config("spark.ui.dagGraph.retainedRootRDDs", "1")
            .config("spark.ui.retainedJobs", "1")
            .config("spark.ui.retainedStages", "1")
            .config("spark.ui.retainedTasks", "1")
            .config("spark.sql.ui.retainedExecutions", "1")
            .config("spark.worker.ui.retainedExecutors", "1")
            .config("spark.worker.ui.retainedDrivers", "1")
        )

        builder = builder.config("spark.driver.extraJavaOptions", " ".join(jvm_options))

        # Set a low memory ram to control the memory usage. Consider that this will affect the master, and
        # all cores used (the RAM consumed will be this value times the amount of core used)
        # TODO: parametrize this
        builder = builder.config("spark.driver.memory", "1g")
        if config.enable_hive:
            self.log.info("Enabling Hive support")
            builder = builder.enableHiveSupport()
        return builder

    @property
    def session(self) -> SparkSession:
        return self._session
