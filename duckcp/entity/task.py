from datetime import datetime
from typing import NamedTuple


class Task(NamedTuple):
    id: int
    code: str  # 编码
    created_at: datetime
    updated_at: datetime
