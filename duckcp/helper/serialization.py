"""
序列化与反序列化帮助函数。
"""
from decimal import Decimal
from json import JSONEncoder, dumps, loads
from typing import Any


class JsonEncoder(JSONEncoder):
    """
    序列化JSON的数据：
    - 添加Decimal类型支持。
    - 其他类型数据通过`vars`支持。
    """

    def default(self, value) -> any:
        return float(value) if isinstance(value, Decimal) else vars(value)


def json_encode(data: Any) -> str:
    """
    将数据序列化成JSON字符串：
    - 保留中文
    - 去掉多于空格
    - 支持自定义的类型
    """
    return dumps(data, separators=(',', ':'), ensure_ascii=False, cls=JsonEncoder)


def json_decode(data: str | bytes) -> any:
    """
    将JSON字符串反序列化成对象
    """
    return loads(data)
