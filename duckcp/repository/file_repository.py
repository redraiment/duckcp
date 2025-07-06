import logging
from collections.abc import Iterator
from contextlib import contextmanager

import duckdb
from duckdb.duckdb import DuckDBPyConnection

from duckcp.entity.connection import Connection
from duckcp.entity.repository import Repository
from duckcp.helper.fs import WorkDirectory
from duckcp.helper.validation import ensure

logger = logging.getLogger(__name__)


class FileRepository(Repository):
    """
    文件类型仓库。
    """

    def establish_connection(self) -> DuckDBPyConnection:
        """
        创建DuckDB内存数据库连接。
        """
        return duckdb.connect(':memory:')

    @contextmanager
    def connect(self) -> Iterator[Connection]:
        """
        建立在目标目录下的临时连接。
        """
        ensure(bool(self.properties), '缺少连接参数')
        ensure(bool(self.properties.get('folder')), '缺少文件夹')
        folder = self.properties.get('folder')
        with WorkDirectory(folder):
            yield Connection(self.establish_connection())
