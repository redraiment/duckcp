"""
HTTP客户端帮助函数。
"""
import logging
from typing import Any
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from urllib.request import urlopen, Request

from duckcp.helper.serialization import json_decode, json_encode

logger = logging.getLogger(__name__)


class HttpClientError(Exception):
    """
    HTTP客户端执行失败时，抛出本异常。
    """

    def __init__(self, url: str, message: str):
        super().__init__(f'请求{url}失败：{message}')


def request(body: Request, timeout: float = 10) -> Any:
    with urlopen(body, timeout=timeout) as response:
        if response.status // 100 == 2:  # 2xx
            data = response.read().decode()
            return json_decode(data)
        else:
            raise HttpClientError(body.full_url, response.status)


def _append_search(url: str, query: dict) -> str:
    """
    将查询条件合并到URL中。
    """
    if query is not None:
        uri = urlparse(url)
        query = {**parse_qs(uri.query), **query}  # 合并参数，并且用新参数覆盖已有参数
        search = urlencode(query, doseq=True)
        return str(urlunparse(uri._replace(query=search)))
    else:
        return url


def get(url: str, headers: dict = None, query: dict = None, timeout: float = 10) -> Any:
    """
    发起HTTP GET请求。
    """
    logger.debug('url=%s, headers=%s, params=%s, timeout=%s', url, headers, query, timeout)

    if headers is None:
        headers = {}
    url = _append_search(url, query)

    return request(Request(url, method='GET', headers=headers), timeout)


def post(url: str, headers: dict = None, query: dict = None, params: dict = None, timeout: float = 10) -> Any:
    """
    发起HTTP POST请求。
    """
    logger.debug('url=%s, headers=%s, params=%s, timeout=%s', url, headers, params, timeout)
    if headers is None:
        headers = {}
    if 'Content-Type' not in headers:
        headers['Content-Type'] = 'application/json; charset=utf-8'
    url = _append_search(url, query)
    data = json_encode(params).encode() if params is not None else None

    return request(Request(url, method='POST', headers=headers, data=data), timeout)
