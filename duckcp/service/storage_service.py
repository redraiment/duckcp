import logging
from typing import Optional, Any

from duckcp.configuration import meta_configuration as metadata
from duckcp.entity.snapshot import Snapshot
from duckcp.entity.storage import Storage
from duckcp.helper.validation import ensure
from duckcp.projection.repository_projection import RepositoryProjection
from duckcp.projection.storage_projection import StorageProjection
from duckcp.repository import RepositoryKind
from duckcp.service import repository_service

logger = logging.getLogger(__name__)


def storage_find(repository_code: str, code: str) -> Optional[Storage]:
    """
    根据编码查找存储单元。
    """
    with metadata.connect() as meta:
        return meta.record('''
          select
            storages.*
          from
            storages
          inner join
            repositories
          on
            storages.repository_id = repositories.id
            and repositories.code = ?
            and storages.code = ?
        ''', repository_code, code, constructor=Storage._make)


def storage_exists(repository_code: str, code: str) -> bool:
    """
    判断编码对应的存储单元是否已存在。
    """
    return storage_find(repository_code, code) is not None


def storage_create(repository_code: str, code: str, properties: dict[str, Any]):
    """
    添加仓库的存储单元。
    """
    logger.debug('repository_code=%s, code=%s, properties=%s', repository_code, code, properties)
    ensure(repository_code is not None, '缺少仓库名称')
    ensure(code is not None, f'仓库({repository_code})缺少存储单元名称')
    properties = {name: value for name, value in properties.items() if bool(value)} if properties else {}
    ensure(bool(properties), f'仓库({repository_code})的存储单元({code})缺少存储参数')

    repository = repository_service.repository_find(repository_code)
    ensure(repository is not None, f'仓库({repository_code})不存在')
    kind = RepositoryKind.of(repository.kind)
    kind.ensure_medium_properties(properties)
    ensure(not storage_exists(repository.code, code), f'仓库({repository_code})的存储单元({code})已存在')

    with metadata.connect() as meta:
        storage = meta.record('''
          insert into storages
            (repository_id, code, properties)
          values
            (?, ?, ?)
          returning *
        ''', repository.id, code, properties, constructor=Storage._make)
        logger.info('创建仓库(%s)的存储单元(%s)', repository.code, code)
        logger.debug('storage=%s', storage)


def storage_update(repository_code: str, code: str, properties: dict[str, Any]):
    """
    更新仓库的存储单元信息。
    """
    logger.debug('repository_code=%s, code=%s, properties=%s', repository_code, code, properties)
    ensure(repository_code is not None, '缺少仓库名称')
    ensure(code is not None, f'仓库({repository_code})缺少存储单元名称')
    properties = {key: value for key, value in properties.items() if value is not None} if properties else {}
    ensure(bool(properties), f'仓库({repository_code})的存储单元({code})缺少更新内容')

    repository = repository_service.repository_find(repository_code)
    ensure(repository is not None, f'仓库({repository_code})不存在')
    storage = storage_find(repository_code, code)
    ensure(storage is not None, f'仓库({repository_code})的存储单元({code})不存在')
    kind = RepositoryKind.of(repository.kind)
    properties = {**storage.properties, **properties}
    kind.ensure_medium_properties(properties)

    with metadata.connect() as meta:
        storage = meta.record('''
          update
            storages
          set
            properties = ?,
            updated_at = datetime(current_timestamp, 'localtime')
          where
            id = ?
          returning *
        ''', {
            key: value
            for key, value in properties.items()
            if bool(value)  # 移除手工强制设为空值的项
        }, storage.id, constructor=Storage._make)
        logger.info('更新仓库(%s)的存储单元(%s)', repository.code, code)
        logger.debug('storage=%s', storage)


def storage_delete(repository_code: str, code: str):
    """
    删除存储单元。
    """
    logger.debug('repository_code=%s, code=%s', repository_code, code)
    ensure(repository_code is not None, '缺少仓库名称')
    ensure(code is not None, f'仓库({repository_code})缺少存储单元名称')

    repository = repository_service.repository_find(repository_code)
    ensure(repository is not None, f'仓库({repository_code})不存在')
    storage = storage_find(repository_code, code)
    ensure(storage is not None, f'仓库({repository_code})的存储单元({code})不存在')

    with metadata.connect() as meta:
        storage = meta.record('''
          delete from storages where id = ? returning *
        ''', storage.id, constructor=Storage._make)
        logger.info('删除仓库(%s)的存储单元(%s)', repository.code, code)
        logger.debug('storage=%s', storage)


def storage_list(
        repository_kind: Optional[str] = None,
        repository_code: Optional[str] = None
) -> list[StorageProjection]:
    """
    列出符合条件的存储单元。
    """
    logger.debug('repository_kind=%s, repository_code=%s', repository_kind, repository_code)

    filters = []
    parameters = []
    if repository_kind is not None:
        RepositoryKind.ensure(repository_kind)
        filters.append('and repositories.kind = ?')
        parameters.append(repository_kind)
    if repository_code is not None:
        filters.append('and repositories.code = ?')
        parameters.append(repository_code)

    with metadata.connect() as meta:
        return meta.records(f'''
          with storages_transformers as (
            select
              target_id as storage_id,
              count(*)  as transformers
            from
              transformers
            group by
              target_id
          )
          select
            repositories.kind as repository_kind,
            repositories.code as repository_code,
            storages.code,
            coalesce(storages_transformers.transformers, 0) as transformers
          from
            storages
          inner join
            repositories
          on
            storages.repository_id = repositories.id
            {' '.join(filters)}
          left join
            storages_transformers
          on
            storages.id = storages_transformers.storage_id
          order by
            repositories.kind,
            repositories.code,
            storages.code
        ''', *parameters, constructor=StorageProjection._make)
