"""
数据迁移至管系统数据库表，原理如下：
1. 在来源仓库上执行SQL。
2. 根据查询结果生成DELETE语句与INSERT语句。
3. 先执行删除语句清空表。
4. 再执行插入语句新增记录。
"""
import logging

from duckcp.entity.repository import Repository
from duckcp.entity.statement import Statement
from duckcp.entity.storage import Storage
from duckcp.helper.sql import delete_from, insert_into

logger = logging.getLogger(__name__)


def database_transform(statement: Statement, repository: Repository, storage: Storage):
    """
    将数据源迁移到关系型数据库表中。
    """
    catalog = storage.properties.get('catalog')
    schema = storage.properties.get('schema')
    table = storage.properties['table']

    # 通过sqlglot生成SQL，避免字符串转义或SQL注入等问题。
    with repository.connect() as connection:
        with connection.executor() as executor:
            sql = delete_from(catalog, schema, table).sql()
            logger.info('清空表(%s)', sql)
            executor.execute(sql)

            columns, records = statement.execute()
            sql = insert_into(catalog, schema, table, columns).sql()
            logger.info('批量添加数据(%s)', sql)
            executor.batch(sql, records)
