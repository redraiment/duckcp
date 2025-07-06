"""
开放平台认证服务。
"""
import logging
from typing import Optional

from duckcp.configuration import meta_configuration as metadata
from duckcp.entity.credential import Credential
from duckcp.helper.validation import ensure
from duckcp.typing.authentication_token_type import AuthenticationToken
from duckcp.typing.authenticator_type import Authenticator
from duckcp.typing.credential_refresher_type import CredentialRefresher

logger = logging.getLogger(__name__)


def authenticate(platform_code: str, app_code: str, token: AuthenticationToken, refresher: CredentialRefresher) -> Authenticator:
    """
    认证并获取最新的访问凭证。
    """
    logger.debug('platform_code=%s, app_code=%s, token=%s', platform_code, app_code, token)
    ensure(platform_code is not None and app_code is not None and token is not None, '缺少应用信息')

    def authenticator() -> Optional[str]:
        """
        认证闭包：包含了认证信息。
        """
        with metadata.connect() as meta:
            access_token = meta.value('''
              select
                access_token
              from
                credentials
              where
                platform_code = ?
                and app_code = ?
                and datetime(current_timestamp, 'localtime') < expired_at
            ''', platform_code, app_code)
            if access_token is None:
                access_token, expired_at = refresher(token)
                credential = meta.record('''
                  insert or replace into credentials
                    (platform_code, app_code, access_token, expired_at)
                  values
                    (?, ?, ?, ?)
                  returning *
                ''', platform_code, app_code, access_token, expired_at, constructor=Credential._make)
                logger.info('刷新凭证(%s, %s)', platform_code, app_code)
                logger.debug('credential=%s', credential)
            return access_token

    return authenticator
