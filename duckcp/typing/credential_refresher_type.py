"""
访问凭证的刷新函数
"""
from datetime import datetime
from typing import Callable

from duckcp.typing.authentication_token_type import AuthenticationToken

type CredentialRefresher = Callable[[AuthenticationToken], tuple[str, datetime]]
