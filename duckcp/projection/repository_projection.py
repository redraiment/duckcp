from typing import NamedTuple


class RepositoryProjection(NamedTuple):
    kind: str  # 类型
    code: str  # 编码
    storages: int  # 关联存储数量
    transformers: int  # 关联迁移数量
