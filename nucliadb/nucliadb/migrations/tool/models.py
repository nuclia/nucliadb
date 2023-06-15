import types
from dataclasses import dataclass
from nucliadb.common.maindb.driver import Driver
from typing import Optional


@dataclass
class MigrationContext:
    from_version: int
    to_version: int
    kv_driver: Driver


@dataclass
class Migration:
    version: int
    module: types.ModuleType


@dataclass
class KnowledgeBoxInfo:
    current_version: int


@dataclass
class GlobalInfo:
    current_version: int
    target_version: Optional[int] = None
