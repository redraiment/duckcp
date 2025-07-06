from typing import NamedTuple


class TransformerProjection(NamedTuple):
    code: str  # 编码
    source_repository_kind: str  # 来源仓库类型
    source_repository_code: str  # 来源仓库编码
    target_repository_kind: str  # 目标仓库类型
    target_repository_code: str  # 目标仓库编码
    target_storage_code: str  # 目标存储编码
    script_file: str  # 迁移脚本
    tasks: int  # 关联任务数量
