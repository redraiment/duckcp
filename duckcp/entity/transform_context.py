from typing import NamedTuple


class TransformContext(NamedTuple):
    """
    迁移的上下文。
    """
    source_repository_code: str  # 来源仓库编码
    target_repository_code: str  # 去向仓库编码
    target_storage_code: str  # 目标存储编码
