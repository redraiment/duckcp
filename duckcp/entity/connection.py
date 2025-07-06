from typing import Self

from duckcp.entity.executor import Executor
from duckcp.entity.statement import Statement
from duckcp.typing.connection_protocol import ConnectionProtocol


class Connection:
    """
    数据库连接对象。
    """
    connection: ConnectionProtocol

    def __init__(self, connection: ConnectionProtocol):
        self.connection = connection

    def __enter__(self) -> Self:
        """
        生命周期开始。
        """
        return self

    def __exit__(self, exception_class, exception, traceback):
        """
        生命周期结束。
        """
        self.close()

    def close(self):
        """
        断开连接。
        """
        self.connection.close()

    def executor(self) -> Executor:
        """
        创建新的语句对象，对于执行查询语句。
        """
        return Executor(self.connection.cursor())

    def prepare(self, sql: str) -> Statement:
        """
        准备SQL语句用于后续执行。
        """
        return Statement(self.executor(), sql)
