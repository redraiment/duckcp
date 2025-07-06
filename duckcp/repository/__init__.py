import logging
from collections.abc import Sequence
from enum import Enum
from typing import Any, NamedTuple

from duckcp.repository.bitable_repository import BiTableRepository
from duckcp.repository.duckdb_repository import DuckDBRepository
from duckcp.repository.file_repository import FileRepository
from duckcp.repository.odps_repository import OdpsRepository
from duckcp.repository.postgres_repository import PostgresRepository
from duckcp.repository.sqlite_repository import SqliteRepository
from duckcp.transform.bitable_transform import bitable_transform
from duckcp.transform.database_transform import database_transform
from duckcp.transform.duckdb_transform import duckdb_transform
from duckcp.transform.file_transform import file_transform
from duckcp.typing.transform_type import Transform

logger = logging.getLogger(__name__)


class RepositoryKind(Enum):
    """
    仓库类型。用于管理不同类型仓库的选项.
    """
    Postgres = (
        'postgres',
        PostgresRepository,
        ['database'],
        ['table'],
        database_transform,
    )
    Odps = (
        'odps',
        OdpsRepository,
        ['end_point', 'project', 'access_key', 'access_secret'],
        ['table'],
        database_transform,
    )
    BiTable = (
        'bitable',
        BiTableRepository,
        ['access_key', 'access_secret'],
        ['document', 'table'],
        bitable_transform,
    )
    DuckDB = (
        'duckdb',
        DuckDBRepository,
        ['file'],
        ['table'],
        duckdb_transform,
    )
    Sqlite = (
        'sqlite',
        SqliteRepository,
        ['file'],
        ['table'],
        database_transform,
    )
    File = (
        'file',
        FileRepository,
        ['folder'],
        ['file'],
        file_transform,
    )

    @staticmethod
    def codes() -> list[str]:
        """
        仓库类型的编码：用于交互选择仓库类型。
        """
        return [kind.code for kind in RepositoryKind]

    @staticmethod
    def of(code: str) -> 'RepositoryKind':
        """
        根据编码获取仓库类型。
        """
        for kind in RepositoryKind:
            if kind.code == code:
                return kind
        else:
            raise ValueError(f'仓库类型({code})不支持')

    @staticmethod
    def ensure(code: str):
        """
        确认支持仓库编码对应的仓库类型。
        """
        RepositoryKind.of(code)

    @property
    def code(self) -> str:
        """
        当前仓库类型的唯一编码。
        """
        return self.value[0]

    @property
    def repository(self) -> type[NamedTuple]:
        """
        当前类型的仓库。
        """
        return self.value[1]

    @property
    def required_connection_options(self) -> list[str]:
        """
        当前类型仓库必要的连接选项。
        """
        return self.value[2]

    def ensure_connection_properties(self, properties: dict[str, Any]):
        """
        确保仓库类型需要的连接选项有提供。
        """
        for name in self.required_connection_options:
            option = f'--{name.replace("_", "-")}'
            if not bool(properties.get(name)):
                raise AssertionError(f'{self.code}类型仓库缺少`{option}`')

    @property
    def required_medium_options(self) -> list[str]:
        """
        当前类型仓库的存储单元中必要的介质选项。
        """
        return self.value[3]

    def ensure_medium_properties(self, properties: dict[str, Any]):
        """
        确保仓库类型的存储单元需要的介质选项有提供。
        """
        for name in self.required_medium_options:
            option = f'--{name.replace("_", "-")}'
            if not bool(properties.get(name)):
                raise AssertionError(f'{self.code}类型仓库的存储缺少`{option}`')

    @property
    def transform(self) -> Transform:
        """
        当前类型仓库的迁移函数。
        """
        return self.value[4]


def repository_constructor[T: tuple](record: Sequence[Any]) -> T:
    """
    根据记录的仓库类型，创建对应类型的仓库。
    """
    logger.debug('record=%s', record)
    kind = RepositoryKind.of(record[1])
    repository_class = kind.repository
    return repository_class._make(record)
