import logging

import psycopg2

from duckcp.entity.repository import Repository
from duckcp.helper.validation import ensure

logger = logging.getLogger(__name__)


class PostgresRepository(Repository):
    """
    Postgres类型仓库。
    """

    def establish_connection(self) -> psycopg2.extensions.connection:
        """
        创建Postgres连接。
        """
        ensure(bool(self.properties), '缺少连接参数')
        host = self.properties.get('host')
        port = self.properties.get('port')
        ensure(bool(self.properties.get('database')), '缺少数据库名称')
        database = self.properties.get('database')
        username = self.properties.get('username')
        password = self.properties.get('password')
        logger.debug('host=%s, port=%s, database=%s, username=%s', host, port, database, username)

        return psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=username,
            password=password
        )
