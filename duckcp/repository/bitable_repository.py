import logging
from typing import Any, Sequence, cast, NamedTuple

import duckdb
from duckdb.duckdb import DuckDBPyConnection
from pandas import DataFrame

from duckcp.configuration import meta_configuration as metadata
from duckcp.entity.connection import Connection
from duckcp.entity.executor import Executor
from duckcp.entity.repository import Repository
from duckcp.feishu import tenant_access_token, bitable
from duckcp.helper.sql import extract_tables
from duckcp.helper.validation import ensure
from duckcp.service.authentication_service import Authenticator, authenticate
from duckcp.typing.connection_protocol import ConnectionProtocol
from duckcp.typing.supports_get_item_protocol import SupportsGetItemProtocol

logger = logging.getLogger(__name__)


class BiTable(NamedTuple):
    name: str  # 在SQL中使用的表名
    code: str  # 飞书多维表格编码
    document_code: str  # 飞书多维文档编码


class BiTableCursor:
    """
    飞书多维表格游标：增删改等变更操作只影响本地缓存数据；不会同步至多维表格。
    """
    cursor: duckdb.DuckDBPyConnection
    authenticator: Authenticator  # 授信服务。
    tables: dict[str, BiTable]

    def __init__(self, cursor: DuckDBPyConnection, authenticator: Authenticator, tables: dict[str, BiTable]):
        self.cursor = cursor
        self.authenticator = authenticator
        self.tables = tables

    @property
    def description(self) -> Sequence[SupportsGetItemProtocol]:
        return self.cursor.description

    def close(self):
        """
        关闭游标
        """
        self.cursor.close()

    def __prepare(self, sql: str):
        """
        根据SQL语句中查询用到的表，动态地从远程多维表格中实时加载最新的数据。
        """
        for table_name in extract_tables(sql):
            if table := self.tables.get(table_name):
                records = bitable.list_records(self.authenticator(), table.document_code, table.code)
                frame = DataFrame([{
                    **record['fields'],
                    'id': record['record_id'],
                } for record in records])
                self.cursor.execute(f' set global pandas_analyze_sample = {len(frame)} ')
                self.cursor.register(table.name, frame)

    def executemany(self, sql: str, parameters: list[Sequence[Any]]):
        """
        批量执行。
        """
        self.__prepare(sql)
        self.cursor.executemany(sql, parameters)

    def execute(self, sql: str, parameters: Sequence[Any]):
        """
        单句执行。
        """
        self.__prepare(sql)
        return self.cursor.execute(sql, parameters)

    def fetchall(self) -> list[Sequence[Any]]:
        """
        获取查询结果。
        """
        return self.cursor.fetchall()


class BiTableConnection(Connection):
    """
    飞书多维表格连接：获取访问凭证。
    """
    authenticator: Authenticator  # 授信服务。
    tables: dict[str, BiTable]

    def __init__(self, connection: ConnectionProtocol, authenticator: Authenticator, tables: dict[str, BiTable]):
        super().__init__(connection)
        self.authenticator = authenticator
        self.tables = tables

    def executor(self) -> Executor:
        """
        创建新的语句对象，对于执行查询语句。
        """
        cursor = cast(DuckDBPyConnection, self.connection.cursor())
        return Executor(BiTableCursor(cursor, self.authenticator, self.tables))


class BiTableRepository(Repository):
    """
    飞书多维表格类型仓库。
    """

    def establish_connection(self) -> DuckDBPyConnection:
        """
        创建DuckDB内存数据库连接。
        """
        return duckdb.connect(':memory:')

    @property
    def authenticator(self) -> Authenticator:
        """
        飞书开放平台的身份校验器。
        """
        ensure(bool(self.properties), '缺少连接参数')
        ensure(bool(self.properties.get('access_key')), '缺少访问编码')
        access_key = self.properties.get('access_key')
        ensure(bool(self.properties.get('access_secret')), '缺少访问密钥')
        access_secret = self.properties.get('access_secret')
        logger.debug('access_key=%s', access_key)
        return authenticate('feishu', access_key, {
            'access_key': access_key,
            'access_secret': access_secret,
        }, tenant_access_token)

    def connect(self) -> Connection:
        """
        连接多维表格。
        """
        tables = {}
        if self.id is not None:
            with metadata.connect() as meta:
                tables = {
                    table.name: table
                    for table in meta.records('''
                      select
                        code as name,
                        properties->>'table' as code,
                        properties->>'document' as document_code
                      from
                        storages
                      where
                        repository_id = ?
                    ''', self.id, constructor=BiTable._make)
                }
        connection = self.establish_connection()
        return BiTableConnection(connection, self.authenticator, tables)
