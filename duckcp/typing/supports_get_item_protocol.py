from typing import Protocol, Any


class SupportsGetItemProtocol(Protocol):
    """
    一个抽象基类，含一个抽象方法 __getitem__，该方法与其返回类型协变。
    """

    def __getitem__(self, index) -> Any:
        ...
