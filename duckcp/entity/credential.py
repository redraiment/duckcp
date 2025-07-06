from datetime import datetime
from typing import NamedTuple


class Credential(NamedTuple):
    id: int
    platform_code: str  # 所属平台
    app_code: str  # 应用编码
    access_token: str  # 访问凭证
    expired_at: datetime
    created_at: datetime
    updated_at: datetime
