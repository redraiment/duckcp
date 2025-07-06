from datetime import datetime
from typing import NamedTuple


class Transformer(NamedTuple):
    id: int
    code: str  # 编号
    source_id: int  # 来源仓库
    target_id: int  # 目标表格
    script_file: str  # 迁移脚本
    created_at: datetime
    updated_at: datetime
