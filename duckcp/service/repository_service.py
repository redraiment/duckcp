import logging
from contextlib import contextmanager
from typing import Any, Optional, Sequence, Iterator

from duckcp.configuration import meta_configuration as metadata
from duckcp.entity.executor import Executor
from duckcp.entity.repository import Repository
from duckcp.helper.validation import ensure
from duckcp.projection.repository_projection import RepositoryProjection
from duckcp.repository import repository_constructor, RepositoryKind

logger = logging.getLogger(__name__)


def repository_find[T: Repository](code: str) -> Optional[T]:
    """
    根据编码查找仓库。
    """
    with metadata.connect() as meta:
        return meta.record('select * from repositories where code = ?', code, constructor=repository_constructor)


def repository_exists(code: str) -> bool:
    """
    判断编码对应的仓库是否已存在。
    """
    return repository_find(code) is not None


def repository_storages(code: str) -> int:
    """
    统计仓库的存储数。
    """
    with metadata.connect() as meta:
        return meta.value('''
          select
            count(*) as tables
          from
            repositories
          inner join
            storages
          on
            repositories.id = storages.repository_id
            and repositories.code = ?
        ''', code)


def repository_transformers(code: str) -> int:
    """
    统计仓库的迁移数。
    """
    with metadata.connect() as meta:
        return meta.value('''
          select
            count(*) as transformers
          from
            repositories
          inner join
            transformers
          on
            repositories.id = transformers.source_id
            and repositories.code = ?
        ''', code)


def repository_create(code: str, kind_code: str, properties: dict[str, Any]):
    """
    创建仓库。
    """
    logger.debug('code=%s, kind_code=%s, properties=%s', code, kind_code, properties)
    ensure(code is not None, '缺少仓库名称')
    ensure(kind_code is not None, f'仓库({code})缺少类型')
    kind = RepositoryKind.of(kind_code)
    ensure(bool(properties), f'仓库({code})缺少连接参数')
    kind.ensure_connection_properties(properties)
    ensure(not repository_exists(code), f'仓库({code})已存在')

    with metadata.connect() as meta:
        repository = meta.record('''
          insert into repositories
            (code, kind, properties)
          values
            (?, ?, ?)
          returning *
        ''', code, kind.code, {
            key: value
            for key, value in properties.items()
            if bool(value)
        }, constructor=repository_constructor)
        logger.info('创建仓库(%s)', code)
        logger.debug('repository=%s', repository)


def repository_update(code: str, kind_code: str, properties: dict[str, Any]):
    """
    更新仓库信息。
    """
    logger.debug('code=%s, kind_code=%s, properties=%s', code, kind_code, properties)
    ensure(code is not None, '缺少仓库名称')
    properties = {key: value for key, value in properties.items() if value is not None} if properties is not None else {}
    ensure(kind_code is not None or bool(properties), f'仓库({code})缺少更新内容')

    repository = repository_find(code)
    ensure(repository is not None, f'仓库({code})不存在')

    kind = RepositoryKind.of(kind_code or repository.kind)
    properties = {**repository.properties, **properties}
    kind.ensure_connection_properties(properties)

    with metadata.connect() as meta:
        repository = meta.record('''
          update
            repositories
          set
            kind = ?,
            properties = ?,
            updated_at = current_timestamp
          where
            code = ?
          returning *
        ''', kind.code, {
            key: value
            for key, value in properties.items()
            if bool(value)  # 移除手工强制设为空值的项
        }, code, constructor=repository_constructor)
        logger.info('更新仓库(%s)', code)
        logger.debug('repository=%s', repository)


def repository_delete(code: str):
    """
    删除仓库。
    - 解绑数据表。
    - 删除迁移。
    """
    logger.debug('code=%s', code)
    ensure(code is not None, '缺少仓库名称')
    ensure(repository_exists(code), f'仓库({code})不存在')
    with metadata.connect() as meta:
        repository = meta.record('''
          delete from repositories where code = ? returning *
        ''', code, constructor=repository_constructor)
        logger.info('删除仓库(%s)', code)
        logger.debug('repository=%s', repository)


def repository_list(kind: Optional[str] = None) -> list[RepositoryProjection]:
    """
    列出符合条件的仓库。
    """
    logger.debug('kind=%s', kind)
    filters = []
    parameters = []
    if kind is not None:
        RepositoryKind.ensure(kind)
        filters.append('where repositories.kind = ?')
        parameters.append(kind)

    with metadata.connect() as meta:
        return meta.records(f'''
          with repositories_transformers as (
            select
              repositories.id as repository_id,
              count(*) as transformers
            from
              repositories
            inner join
              transformers
            on
              repositories.id = transformers.source_id
            group by
              repositories.id
          ), repositories_storages as (
            select
              repositories.id as repository_id,
              count(*) as storages
            from
              repositories
            inner join
              storages
            on
              repositories.id = storages.repository_id
            group by
              repositories.id
          )
          select
            repositories.kind,
            repositories.code,
            coalesce(repositories_storages.storages, 0) as storages,
            coalesce(repositories_transformers.transformers, 0) as transformers
          from
            repositories
          left join
            repositories_transformers
          on
            repositories.id = repositories_transformers.repository_id
          left join
            repositories_storages
          on
            repositories.id = repositories_storages.repository_id
          {' '.join(filters)}
          order by
            repositories.kind,
            repositories.code
        ''', *parameters, constructor=RepositoryProjection._make)


@contextmanager
def repository_connect(code: str) -> Iterator[Executor]:
    """
    连接指定的仓库。
    """
    logger.debug('code=%s', code)
    repository = repository_find(code)
    ensure(repository is not None, f'仓库({code})不存在')

    with repository.connect() as connection:
        with connection.executor() as executor:
            yield executor


def repository_execute(code: str, sql: str) -> tuple[list[str], list[Sequence[Any]]]:
    """
    在仓库上执行查询脚本，并返回查询结果。
    """
    logger.debug('code=%s, sql=%s', code, sql)
    with repository_connect(code) as executor:
        return executor.execute(sql)
