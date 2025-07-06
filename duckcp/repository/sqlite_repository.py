import logging
import sqlite3
from typing import Any

from duckcp.entity.repository import Repository
from duckcp.helper.serialization import json_encode, json_decode

logger = logging.getLogger(__name__)


def to_bool(value: bytes) -> bool:
    """
    将SQLite返回的Bytes类型布尔值转成Python的布尔值。
    """
    return bool(int(value)) if value is not None else None


def to_value(value: bytes) -> Any:
    """
    将SQLite的JSON转成Python的值。
    """
    return json_decode(value) if value is not None else None


def to_json(value: Any) -> str:
    """
    将Python的值转成SQLite的JSON。
    """
    return json_encode(value) if value is not None else None


sqlite3.register_converter('boolean', to_bool)
sqlite3.register_converter('bool', to_bool)
sqlite3.register_converter('jsonb', to_value)
sqlite3.register_converter('json', to_value)
sqlite3.register_adapter(dict, to_json)
sqlite3.register_adapter(list, to_json)


class SqliteRepository(Repository):
    """
    Sqlite类型仓库。
    """

    def establish_connection(self) -> sqlite3.Connection:
        """
        创建Sqlite连接。
        """
        file = self.properties['file'] if self.properties and 'file' in self.properties else ':memory:'
        logger.debug('file=%s', file)
        connection = sqlite3.connect(file, detect_types=sqlite3.PARSE_DECLTYPES, autocommit=True)
        connection.execute('PRAGMA foreign_keys=ON')  # 启用on delete cascade
        return connection
