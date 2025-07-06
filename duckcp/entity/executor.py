import logging
from collections import namedtuple
from typing import Self, Any, Sequence, Optional

from pandas import DataFrame

from duckcp.typing.cursor_protocol import CursorProtocol
from duckcp.typing.record_constructor_protocol import RecordConstructorProtocol

logger = logging.getLogger(__name__)


class Executor:
    """
    执行器：执行SQL语句。
    """
    cursor: CursorProtocol

    def __init__(self, cursor: CursorProtocol):
        self.cursor = cursor

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
        self.cursor.close()

    def batch(self, sql: str, parameters: list[Sequence[Any]]):
        """
        批量执行语句，无返回结果。
        """
        logger.debug('sql=%s, parameters=%s', sql, parameters)
        self.cursor.executemany(sql, parameters)

    def execute(self, sql: str, *parameters: Any) -> tuple[list[str], list[Sequence[Any]]]:
        """
        执行单条语句，返回原始的结果。
        - 头信息：列名和类型。
        - 记录：原始数据。
        """
        logger.debug('sql=%s, parameters=%s', sql, parameters)
        if parameters:
            self.cursor.execute(sql, parameters)
        else:
            self.cursor.execute(sql)  # 勿删：不同类型Cursor中参数默认值不同，无法统一处理
        columns = [column[0] for column in self.cursor.description] if self.cursor.description else []
        records = self.cursor.fetchall()
        return columns, records

    def __call__(self, sql: str, *parameters: Any) -> DataFrame:
        """
        执行查询语句，返回DataFrame结构。
        """
        columns, records = self.execute(sql, *parameters)
        return DataFrame(records, columns=columns)

    def records[T: tuple[Any, ...]](self, sql: str, *parameters: Any, constructor: RecordConstructorProtocol[T] = None) -> list[T]:
        """
        执行查询语句，返回NamedTuple形式的记录数据。
        """
        columns, records = self.execute(sql, *parameters)
        if constructor is None:
            record_class = namedtuple('_DuckCP_Record', columns)
            return [record_class(*record) for record in records]
        else:
            return [constructor(record) for record in records]

    def record[T: tuple[Any, ...]](self, sql: str, *parameters: Any, constructor: RecordConstructorProtocol[T] = None) -> Optional[T]:
        """
        执行查询语句，并返回第一行NamedTuple形式的记录数据。
        """
        records = self.records(sql, *parameters, constructor=constructor)
        return records[0] if records else None

    def value(self, sql: str, *parameters: Any) -> Optional[Any]:
        """
        执行查询语句，并返回第一行第一列数据。
        """
        record = self.record(sql, *parameters)
        return record[0] if record else None

    def values(self, sql: str, *parameters: Any) -> list[Any]:
        """
        执行查询语句，并返回所有行的第一列数据。
        """
        return [record[0] for record in self.records(sql, *parameters)]

    def all(self, sql: str, *parameters: Any) -> list[dict[str, Any]]:
        """
        执行查询语句，返回字典结构的记录。
        """
        columns, records = self.execute(sql, *parameters)
        return [
            {column: value for column, value in zip(columns, record)}
            for record in records
        ]

    def one(self, sql: str, *parameters: Any) -> dict[str, Any]:
        """
        执行查询语句，返回字典结构的第一行记录。
        """
        records = self.all(sql, *parameters)
        return records[0] if records else None
