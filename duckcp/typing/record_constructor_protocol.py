"""
查询结果返回记录的构造函数。
"""
from typing import Protocol, Any, Sequence


class RecordConstructorProtocol[T: tuple[Any, ...]](Protocol):
    """
    一个抽象基类，含一个抽象方法 __call__，该方法用于构造所需的记录类型。
    """

    def __call__(self, record: Sequence[Any]) -> T:
        ...
