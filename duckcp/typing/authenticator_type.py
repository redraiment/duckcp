"""
取得授信（访问凭证）的函数
"""
from typing import Callable, Optional

type Authenticator = Callable[[], Optional[str]]
