from datetime import datetime
from typing import NamedTuple


class TaskTransformer(NamedTuple):
    task_id: int  # 所属任务
    transformer_id: int  # 所属迁移
    sort: int  # 排序
    created_at: datetime
    updated_at: datetime
