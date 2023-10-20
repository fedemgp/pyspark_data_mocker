import re
import tempfile

import schema

from pyspark_data_mocker.config.validators import range_between, validate_version

# TODO: improve this by configuring valid versions depending on the spark version used
# Values extracted from https://mvnrepository.com/artifact/io.delta/delta-core
_VALID_SCALA_VERSIONS = {"2.11", "2.12", "2.13"}
_VALID_DELTA_VERSIONS = {
    "1.1.0",
    "1.2.0",
    "1.2.1",
    "2.0.0",
    "2.0.1",
    "2.0.2",
    "2.1.0",
    "2.1.1",
    "2.2.0",
    "2.3.0",
    "2.4.0",
}


def _get_tmp_dir() -> tempfile.TemporaryDirectory:
    return tempfile.TemporaryDirectory()


app_config_schema = schema.Schema(
    {
        schema.Optional("schema", default={"infer": False, "config_file": "schema_config.yaml"}): {
            schema.Optional("infer", default=False): bool,
            schema.Optional("config_file", default="schema_config.yaml"): schema.And(
                str, schema.Regex(r"[\w_-]+\.(yaml|yml)", error="The config file must be a yaml file", flags=re.I)
            ),
        },
        schema.Optional("disable_spark_configuration", default=False): bool,
        schema.Optional("spark_configuration", default={}): dict,
    }
)

spark_conf_schema = schema.Schema(
    {
        "app_name": schema.And(str, len),
        "number_of_cores": schema.And(schema.Use(int), range_between(1, 8)),
        schema.Optional("enable_hive", default=False): bool,
        schema.Optional("warehouse_dir", default=_get_tmp_dir): schema.Or(str, tempfile.TemporaryDirectory),
        schema.Optional("delta_configuration"): {
            "scala_version": validate_version(_VALID_SCALA_VERSIONS),
            "delta_version": validate_version(_VALID_DELTA_VERSIONS),
            "snapshot_partitions": schema.And(schema.Use(int), range_between(1, 5)),
            "log_cache_size": schema.And(schema.Use(int), range_between(1, 5)),
        },
    }
)


def validate_schema(config: dict) -> dict:
    app_config = app_config_schema.validate(config)
    if not app_config["disable_spark_configuration"]:
        spark_config = config["spark_configuration"] if "spark_configuration" in config else {}
        app_config["spark_configuration"] = spark_conf_schema.validate(spark_config)
    else:
        # erase spark_configuration, we will not be needed if spark_configuration is disabled
        app_config.pop("spark_configuration")
    return app_config
