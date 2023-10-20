import dataclasses
import pathlib
from tempfile import TemporaryDirectory
from typing import Optional, Union

Dir = Union[str, TemporaryDirectory]


@dataclasses.dataclass
class AppConfig:
    disable_spark_configuration: bool
    schema: "SchemaConfig"
    spark_configuration: Optional["SparkConfig"] = None


@dataclasses.dataclass
class SparkConfig:
    app_name: str
    number_of_cores: int
    enable_hive: bool
    warehouse_dir: Dir
    delta_configuration: Optional["DeltaConfig"] = None

    @property
    def spark_warehouse_dir_path(self) -> str:
        if isinstance(self.warehouse_dir, TemporaryDirectory):
            return str(pathlib.Path(self.warehouse_dir.name, "spark_warehouse"))
        return str(pathlib.Path(self.warehouse_dir, "spark_warehouse"))


@dataclasses.dataclass
class DeltaConfig:
    scala_version: str
    delta_version: str
    snapshot_partitions: int
    log_cache_size: int


@dataclasses.dataclass
class SchemaConfig:
    infer: bool
    config_file: str
