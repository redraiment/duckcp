"""
数据迁移至DuckDB数据库表，原理如下：
1. 在来源仓库上执行SQL，并将查询结果封装成DataFrame。
2. 将DataFrame映射成DuckDB的只读视图。
3. 执行`create ore replace table ... from ...`替换目标表内的数据。
"""
import logging

from duckcp.entity.statement import Statement
from duckcp.entity.storage import Storage
from duckcp.helper.sql import create_or_replace_table
from duckcp.repository.duckdb_repository import DuckDBRepository

logger = logging.getLogger(__name__)


def duckdb_transform(statement: Statement, repository: DuckDBRepository, storage: Storage):
    """
    将数据源迁移到DuckDB数据库表中。
    """
    catalog = storage.properties.get('catalog')
    schema = storage.properties.get('schema')
    table = storage.properties['table']

    with repository.establish_connection() as connection:
        with connection.cursor() as cursor:
            data = statement()
            cursor.execute(f' set global pandas_analyze_sample = {len(data)} ')
            cursor.register(storage.code, data)  # 表的全名是`temp.main.<table>`
            # 通过sqlglot生成SQL，避免字符串转义或SQL注入等问题。
            ast = create_or_replace_table(catalog, schema, table, storage.code)
            sql = ast.sql(dialect='duckdb')
            logger.debug('sql=%s', sql)
            cursor.execute(sql)
