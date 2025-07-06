from abc import abstractmethod
from datetime import datetime
from typing import NamedTuple, Any

from duckcp.entity.connection import Connection
from duckcp.typing.connection_protocol import ConnectionProtocol


class Repository(NamedTuple):
    id: int = None
    kind: str = None  # 类型
    code: str = None  # 编码
    properties: dict[str, Any] = None  # 连接信息
    created_at: datetime = None
    updated_at: datetime = None

    @abstractmethod
    def establish_connection[T: ConnectionProtocol](self) -> T:
        """
        建立新的数据库连接，并返回原始的连接类型。
        """
        pass

    def connect(self) -> Connection:
        """
        建立新的数据库连接。
        """
        return Connection(self.establish_connection())
