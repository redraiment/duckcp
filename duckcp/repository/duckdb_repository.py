import logging

import duckdb
from duckdb.duckdb import DuckDBPyConnection

from duckcp.entity.repository import Repository

logger = logging.getLogger(__name__)


class DuckDBRepository(Repository):
    """
    DuckDB类型仓库。
    """

    def establish_connection(self) -> DuckDBPyConnection:
        """
        创建DuckDB数据库连接。
        """
        file = self.properties['file'] if self.properties and 'file' in self.properties else ':memory:'
        logger.debug('file=%s', file)
        return duckdb.connect(file)
