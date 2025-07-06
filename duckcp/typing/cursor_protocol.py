from typing import Protocol, Sequence, Any

from duckcp.typing.supports_get_item_protocol import SupportsGetItemProtocol


class CursorProtocol(Protocol):
    """
    一个抽象基类，定义数据库游标协议。
    """

    @property
    def description(self) -> Sequence[SupportsGetItemProtocol]:
        ...

    def close(self):
        """
        关闭游标
        """
        ...

    def executemany(self, sql: str, parameters: list[Sequence[Any]]):
        """
        批量执行。
        """
        ...

    def execute(self, sql: str, parameters: Sequence[Any] = None):
        """
        单句执行。
        """
        ...

    def fetchall(self) -> list[Sequence[Any]]:
        """
        获取查询结果。
        """
        ...
