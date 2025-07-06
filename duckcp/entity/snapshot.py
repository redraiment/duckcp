from datetime import datetime
from typing import NamedTuple, Any


class Snapshot(NamedTuple):
    id: int
    storage_id: int  # 所属存储单元
    checksum: str  # 摘要
    records: list[Any]  # 记录
    created_at: datetime
    updated_at: datetime
