import logging

from odps import ODPS, dbapi as odps

from duckcp.entity.repository import Repository
from duckcp.helper.validation import ensure

logger = logging.getLogger(__name__)


class OdpsRepository(Repository):
    """
    MaxCompute(ODPS)类型仓库。
    """

    def establish_connection(self) -> odps.Connection:
        """
        创建Odps仓库连接。
        """
        ensure(bool(self.properties), '缺少连接参数')
        ensure(bool(self.properties.get('end_point')), '缺少接入点')
        end_point = self.properties.get('end_point')
        ensure(bool(self.properties.get('project')), '缺少项目名')
        project = self.properties.get('project')
        ensure(bool(self.properties.get('access_key')), '缺少访问编码')
        access_key = self.properties.get('access_key')
        ensure(bool(self.properties.get('access_secret')), '缺少访问密钥')
        access_secret = self.properties.get('access_secret')

        logger.debug('end_point=%s, project=%s, access_key=%s', end_point, project, access_key)
        db = ODPS(access_key, access_secret, project, end_point)
        return odps.connect(db)
