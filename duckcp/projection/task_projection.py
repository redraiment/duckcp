from typing import NamedTuple


class TaskProjection(NamedTuple):
    code: str  # 编码
    transformers: int  # 关联迁移的数量
