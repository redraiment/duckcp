"""
数据迁移至本地文件，原理如下：
1. 在来源仓库上执行SQL，并将查询结果封装成DataFrame。
2. 将DataFrame映射成DuckDB的只读视图。
3. 执行`COPY ... to ...`导出数据到本地文件。
"""
import logging

from duckcp.entity.statement import Statement
from duckcp.entity.storage import Storage
from duckcp.helper.fs import WorkDirectory
from duckcp.helper.sql import copy_to
from duckcp.repository.duckdb_repository import DuckDBRepository

logger = logging.getLogger(__name__)


def file_transform(statement: Statement, repository: DuckDBRepository, storage: Storage):
    """
    将数据源迁移到本地文件中。
    """
    folder = repository.properties['folder']
    with WorkDirectory(folder):
        with repository.establish_connection() as connection:
            with connection.cursor() as cursor:
                data = statement()
                cursor.execute(f' set global pandas_analyze_sample = {len(data)} ')
                cursor.register(storage.code, data)  # 表的全名是`temp.main.<table>`
                file_name = storage.properties.pop('file')
                # 通过sqlglot生成SQL，避免字符串转义或SQL注入等问题。
                ast = copy_to(storage.code, file_name, storage.properties)
                sql = ast.sql(dialect='duckdb')
                logger.debug('sql=%s', sql)
                cursor.execute(sql)
