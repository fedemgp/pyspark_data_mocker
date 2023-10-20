import pathlib
from typing import List, Optional, Set

from pyspark_data_mocker import config, utils
from pyspark_data_mocker.config import AppConfig
from pyspark_data_mocker.spark_session import SparkTestSession
from pyspark_data_mocker.utils import PathLike


class DataLakeBuilder:
    def __init__(
        self, spark_test: SparkTestSession, app_config: AppConfig, schema_configuration: Optional[PathLike] = None
    ):
        self.dbs: Set[str] = set()
        self.tables: List[dict] = list()
        self.spark_test = spark_test
        self.spark = spark_test.session
        self.app_config = app_config

        self.schema: Optional[dict] = None
        if schema_configuration:
            schema_configuration = utils.to_path(schema_configuration)
            self.schema = config.get_schema_configuration_from_dir(schema_configuration)

    def with_db(self, name: str) -> "DataLakeBuilder":
        """
        Register a database to be created when executing the plan. Ignore database already registered.

        :param name:    STRING  Name of the database to be created
        :return:        An instance of the DataLakeMocker modified, to be able to chain methods
        """
        if name in self.dbs:
            return self
        self.dbs.add(name)
        return self

    def with_table(self, table_name: str, fmt: str, path: PathLike, db_name: str = "default") -> "DataLakeBuilder":
        """
        Register a new table in the datalake. The creation of this table will be later when executing the whole
        plan. If <path> is provided, then the table will be loaded reading that file. It is mandatory that if you
        set a file <path> then, a format should be provided.

        TODO:   in further features, a possiblity to pass as argument a TableBuilder or similar will be possible,
                to be able to define the table using a DSL
        :param table_name:  STRING          The name of the table that will be created
        :param fmt:         STRING          'csv', 'parquet', 'text' or 'json'
        :param path:        STRING or Path  location of the file
        :param db_name:     STRING          name of the database where the table will leave. Default value: 'default'
        :return:            An instance of the DataLakeMocker modified, to be able to chain methods
        """
        # TODO: make a dataclass of the table
        self.tables.append({"path": str(path), "format": fmt, "table_name": table_name, "db_name": db_name})
        return self

    def run(self) -> "DataLakeBuilder":
        """
        Executes the plan by creating each database and table configured

        :return:    The reference of the builder
        """
        for db in self.dbs:
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

        for table in self.tables:
            # TODO: configure schema infering
            opts = dict(header=True, inferSchema=self.app_config.schema.infer)
            table_full_name = f"{table['db_name']}.{table['table_name']}"
            reader = self.spark.read
            if self.schema and table_full_name in self.schema:
                reader = reader.schema(utils.dict_to_ddl_string(self.schema[table_full_name]))
            df = reader.format(table["format"]).options(**opts).load(table["path"])
            writer = df.write
            # TODO: make it easier
            if self.spark_test.config.delta_configuration:
                writer = writer.format("delta")
            writer.mode("overwrite").saveAsTable(table_full_name)

        return self

    def cleanup(self):
        for table in self.tables:
            if not self.spark_test.config.delta_configuration:
                self.spark.sql(f"TRUNCATE TABLE {table['db_name']}.{table['table_name']}")
            self.spark.sql(f"DROP TABLE {table['db_name']}.{table['table_name']}")
        for db in self.dbs:
            self.spark.sql(f"DROP DATABASE IF EXISTS {db}")

    @staticmethod
    def load_from_dir(datalake_dir: PathLike, app_config_path: Optional[PathLike] = None) -> "DataLakeBuilder":
        """
        Navigates over the <datalake_dir> to create the datalake automatically. The file structure needs to be like
        this:
            * The root folder should contain table_like data (csv files, parquet files, json files, all files that
            can be read and be interpreted as a dataframe) or folders.
                * if the <datalake_dir> contains at the root level table-like data, those will be considered as tables
                in the 'default' database.
                * All directories inside the <datalake_dir> folder are considered as other databases, and inside
                the directory it should only contain table_like data

        :param datalake_dir: Directory that contains the datalake definition (table-like files to load as tables and/or
                             folders that will be considered as databases)
        :param app_config_path:   Optional argument with a path of a yaml file to configure the spark session to use
        """
        datalake_dir = utils.to_path(datalake_dir)
        if app_config_path:
            app_config = config.get_config_from_dir(app_config_path)
        else:
            app_config = config.default_config()
        if not datalake_dir.exists():
            raise ValueError(f"The path provided '{datalake_dir}' does not exists")

        if not datalake_dir.is_dir():
            raise ValueError(f"The path '{datalake_dir}' is not a directory with a delta lake data")

        spark_test = SparkTestSession(app_config.spark_configuration)
        schema_config_path = pathlib.Path(datalake_dir, app_config.schema.config_file)
        schema_config = schema_config_path if schema_config_path.exists() else None
        builder = DataLakeBuilder(spark_test, schema_configuration=schema_config, app_config=app_config)

        for d in datalake_dir.iterdir():
            if d.name == app_config.schema.config_file:
                continue

            if d.is_file():
                table_name, extension = d.name.split(".")
                builder = builder.with_table(table_name=table_name, fmt=extension, path=d.resolve())
            else:
                db_name = d.name
                builder = builder.with_db(db_name)
                for table in d.iterdir():
                    table_name, extension = table.name.split(".")
                    builder = builder.with_table(
                        table_name=table_name, fmt=extension, path=table.resolve(), db_name=db_name
                    )

        builder.run()
        return builder
