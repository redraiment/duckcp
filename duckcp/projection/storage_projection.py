from typing import NamedTuple


class StorageProjection(NamedTuple):
    repository_kind: str  # 所属仓库类型
    repository_code: str  # 所属仓库编码
    code: str  # 编码
    transformers: int  # 关联迁移数量
