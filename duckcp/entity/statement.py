from typing import Self, Sequence, Any, Optional

from pandas import DataFrame

from duckcp.entity.executor import Executor
from duckcp.typing.record_constructor_protocol import RecordConstructorProtocol


class Statement:
    """
    语句：预置SQL语句的执行器。
    """
    executor: Executor
    sql: str

    def __init__(self, executor: Executor, sql: str):
        self.executor = executor
        self.sql = sql

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
        关闭本次会话/游标。
        """
        self.executor.close()

    def batch(self, parameters: list[Sequence[Any]]):
        """
        批量执行语句，无返回结果。
        """
        self.executor.batch(self.sql, parameters)

    def execute(self, *parameters: Any) -> tuple[list[str], list[Sequence[Any]]]:
        """
        执行单条语句，返回原始的结果。
        - 头信息：列名和类型。
        - 记录：原始数据。
        """
        return self.executor.execute(self.sql, *parameters)

    def __call__(self, *parameters: Any) -> DataFrame:
        """
        执行查询语句，返回DataFrame结构。
        """
        return self.executor(self.sql, *parameters)

    def records[T: tuple[Any, ...]](self, *parameters: Any, constructor: RecordConstructorProtocol[T] = None) -> list[T]:
        """
        执行查询语句，返回NamedTuple形式的记录数据。
        """
        return self.executor.records(self.sql, *parameters, constructor=constructor)

    def record[T: tuple[Any, ...]](self, *parameters: Any, constructor: RecordConstructorProtocol[T] = None) -> Optional[T]:
        """
        执行查询语句，并返回第一行NamedTuple形式的记录数据。
        """
        return self.executor.record(self.sql, *parameters, constructor=constructor)

    def value(self, *parameters: Any) -> Optional[Any]:
        """
        执行查询语句，并返回第一行第一列数据。
        """
        return self.executor.value(self.sql, *parameters)

    def values(self, *parameters: Any) -> list[Any]:
        """
        执行查询语句，并返回所有行的第一列数据。
        """
        return self.executor.values(self.sql, *parameters)

    def all(self, *parameters: Any) -> list[dict[str, Any]]:
        """
        执行查询语句，返回字典结构的记录。
        """
        return self.executor.all(self.sql, *parameters)

    def one(self, *parameters: Any) -> dict[str, Any]:
        """
        执行查询语句，返回字典结构的第一行记录。
        """
        return self.executor.one(self.sql, *parameters)
