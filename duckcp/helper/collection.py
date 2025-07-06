"""
数据结构帮助函数。
"""


def chunk[T](data: list[T], size: int) -> list[list[T]]:
    """
    将数据按照每[size]个一组分组。
    """
    return [data[index:index + size] for index in range(0, len(data), size)]
