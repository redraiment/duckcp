"""
摘要算法帮助函数
"""
import hashlib
from typing import Callable


# bytes版本

def hash_encode_bytes(algorithm: Callable, data: bytes) -> bytes:
    """
    通过Hash算法编码明文数据。
    :param algorithm: hashlib包里的算法。
    :param data: 明文字节流。
    :return: 密文字节流。
    """
    encoder = algorithm()
    encoder.update(data)
    return encoder.digest()


def md5_bytes(data: bytes) -> bytes:
    """
    计算明文数据的MD5摘要。
    :param data: 明文字节流。
    :return: MD5字节流。
    """
    return hash_encode_bytes(hashlib.md5, data)


def sha256_bytes(data: bytes) -> bytes:
    """
    计算明文数据的SHA256摘要。
    :param data: 明文字节流。
    :return: SHA256字节流。
    """
    return hash_encode_bytes(hashlib.sha256, data)


def sha512_bytes(data: bytes) -> bytes:
    """
    计算明文数据的SHA512摘要。
    :param data: 明文字节流。
    :return: SHA512字节流。
    """
    return hash_encode_bytes(hashlib.sha512, data)


# str 版本

def hash_encode(algorithm: Callable, data: str) -> str:
    """
    通过Hash算法编码明文数据。
    :param algorithm: hashlib包里的算法。
    :param data: 明文字符串。
    :return: 密文字符串。
    """
    return hash_encode_bytes(algorithm, data.encode('utf-8')).hex()


def md5(data: str) -> str:
    """
    计算明文数据的MD5摘要。
    :param data: 明文字符串。
    :return: MD5字符串。
    """
    return hash_encode(hashlib.md5, data)


def sha256(data: str) -> str:
    """
    计算明文数据的SHA256摘要。
    :param data: 明文字符串。
    :return: SHA256字符串。
    """
    return hash_encode(hashlib.sha256, data)


def sha512(data: str) -> str:
    """
    计算明文数据的SHA512摘要。
    :param data: 明文字符串。
    :return: SHA512字符串。
    """
    return hash_encode(hashlib.sha512, data)
