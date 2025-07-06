from typing import Callable

from duckcp.entity.repository import Repository
from duckcp.entity.statement import Statement
from duckcp.entity.storage import Storage

# 定义迁移函数接口
type Transform[T: Repository] = Callable[[Statement, T, Storage], None]
