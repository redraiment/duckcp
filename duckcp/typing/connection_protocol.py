from typing import Protocol

from duckcp.typing.cursor_protocol import CursorProtocol


class ConnectionProtocol(Protocol):
    """
    一个抽象基类，定义数据库连接协议。
    """

    def close(self):
        """
        断开连接。
        """
        ...

    def cursor(self) -> CursorProtocol:
        """
        创建新的游标对象。
        """
        ...
