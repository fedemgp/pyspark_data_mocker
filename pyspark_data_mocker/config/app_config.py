import dataclasses
import pathlib
from tempfile import TemporaryDirectory
from typing import Optional, Union

Dir = Union[str, TemporaryDirectory]


@dataclasses.dataclass
class AppConfig:
    app_name: str
    number_of_cores: int
    enable_hive: bool
    warehouse_dir: Dir
    delta_configuration: Optional["DeltaConfig"]

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
