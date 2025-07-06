"""
飞书开放接口
"""
import logging
from datetime import datetime, timedelta
from typing import TypedDict

from duckcp.helper import http
from duckcp.typing.authentication_token_type import AuthenticationToken

OPEN_API = 'https://open.feishu.cn/open-apis'
ACCESS_TOKEN_API = f'{OPEN_API}/auth/v3/tenant_access_token/internal/'

logger = logging.getLogger(__name__)


class FeiShuError(Exception):
    """
    飞书多维表格接口执行失败时，抛出本异常。
    """

    def __init__(self, api: str, message: str):
        super().__init__(f'{api}请求失败：{message}')


class CredentialResponse(TypedDict):
    """
    飞书应用凭证响应。
    """
    code: int  # 错误编码。
    msg: str  # 错误消息。
    expire: int  # 有效时长；单位秒。
    tenant_access_token: str  # 租户级别凭证。


def tenant_access_token(token: AuthenticationToken) -> tuple[str, datetime]:
    """
    获得租户级别的访问凭证
    @param token: 认证信息
    """
    logger.debug('token=%s', token)
    response: CredentialResponse = http.post(ACCESS_TOKEN_API, params={
        'app_id': token['access_key'],
        'app_secret': token['access_secret'],
    })
    if response['code'] == 0:
        access_token = response['tenant_access_token']
        expired_at = datetime.now() + timedelta(seconds=response['expire'] - 30)
        return access_token, expired_at
    else:
        raise FeiShuError('刷新凭证', response.get('msg', ''))
