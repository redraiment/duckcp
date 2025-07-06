"""
序列化与反序列化帮助函数。
"""
import json
from typing import Any


def json_encode(data: Any) -> str:
    """
    将数据序列化成JSON字符串：
    - 保留中文
    - 去掉多于空格
    - 支持自定义的类型
    """
    return json.dumps(data, separators=(',', ':'), ensure_ascii=False, default=vars)


def json_decode(data: str | bytes) -> any:
    """
    将JSON字符串反序列化成对象
    """
    return json.loads(data)
