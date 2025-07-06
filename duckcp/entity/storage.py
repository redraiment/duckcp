from datetime import datetime
from typing import NamedTuple, Any


class Storage(NamedTuple):
    id: int
    repository_id: int  # 所属仓库
    code: str  # 编码
    properties: dict[str, Any]  # 介质信息
    created_at: datetime
    updated_at: datetime
