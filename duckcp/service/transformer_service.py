import logging
from os.path import exists
from typing import Optional

from duckcp.configuration import meta_configuration as metadata
from duckcp.entity.transform_context import TransformContext
from duckcp.entity.transformer import Transformer
from duckcp.helper.fs import absolute_path, slurp
from duckcp.helper.validation import ensure
from duckcp.projection.transformer_projection import TransformerProjection
from duckcp.repository import RepositoryKind
from duckcp.service import repository_service, storage_service

logger = logging.getLogger(__name__)


def transformer_find(code: str) -> Optional[Transformer]:
    """
    根据编码查找迁移。
    """
    with metadata.connect() as meta:
        return meta.record('select * from transformers where code = ?', code, constructor=Transformer._make)


def transformer_exists(code: str) -> bool:
    """
    判断编码对应的迁移是否已存在。
    """
    return transformer_find(code) is not None


def transformer_create(
        code: str,
        source_repository_code: str,
        target_repository_code: str,
        target_storage_code: str,
        script_file: str,
):
    """
    添加迁移。
    """
    logger.debug(
        'code=%s, source_repository_code=%s, target_repository_code=%s, target_storage_code=%s, script_file=%s',
        code, source_repository_code,
        target_repository_code, target_storage_code,
        script_file
    )
    ensure(code is not None, '缺少迁移名称')
    ensure(source_repository_code is not None, f'迁移({code})缺少来源仓库名称')
    ensure(target_repository_code is not None, f'迁移({code})缺少目标仓库名称')
    ensure(target_storage_code is not None, f'迁移({code})缺少目标存储单元')
    ensure(script_file is not None, f'迁移({code})缺少迁移脚本')

    ensure(not transformer_exists(code), f'迁移({code})已存在')
    repository = repository_service.repository_find(source_repository_code)
    ensure(repository is not None, f'来源仓库({source_repository_code})不存在')
    storage = storage_service.storage_find(target_repository_code, target_storage_code)
    ensure(storage is not None, f'目标仓库({target_repository_code})的存储单元({target_storage_code})不存在')
    script_file = absolute_path(script_file)

    with metadata.connect() as meta:
        transformer = meta.record('''
          insert into transformers
            (code, source_id, target_id, script_file)
          values
            (?, ?, ?, ?)
          returning *
        ''', code, repository.id, storage.id, script_file, constructor=Transformer._make)
        logger.info('创建迁移(%s)', code)
        logger.debug('transformer=%s', transformer)


def transformer_update(
        code: str,
        source_repository_code: str,
        target_repository_code: str,
        target_storage_code: str,
        script_file: str
):
    """
    更新迁移信息。
    """
    logger.debug(
        'code=%s, source_repository_code=%s, target_repository_code=%s, target_storage_code=%s, script_file=%s',
        code, source_repository_code,
        target_repository_code, target_storage_code,
        script_file
    )
    ensure(code is not None, '缺少迁移名称')
    ensure(
        source_repository_code is not None
        or (target_repository_code is not None and target_storage_code is not None)
        or script_file is not None,
        '缺少更新内容'
    )

    transformer = transformer_find(code)
    ensure(transformer is not None, f'迁移({code})不存在')

    if source_repository_code is not None:
        repository = repository_service.repository_find(source_repository_code)
        ensure(repository is not None, f'来源仓库({source_repository_code})不存在')
        source_id = repository.id
    else:
        source_id = transformer.source_id

    if target_repository_code is not None and target_storage_code is not None:
        storage = storage_service.storage_find(target_repository_code, target_storage_code)
        ensure(storage is not None, f'目标仓库({target_repository_code})的存储单元({target_storage_code})不存在')
        target_id = storage.id
    else:
        target_id = transformer.target_id

    if script_file is not None:
        script_file = absolute_path(script_file)
    else:
        script_file = transformer.script_file

    with metadata.connect() as meta:
        transformer = meta.record('''
          update
            transformers
          set
            source_id = ?,
            target_id = ?,
            script_file = ?,
            updated_at = datetime(current_timestamp, 'localtime')
          where
            code = ?
          returning *
        ''', source_id, target_id, script_file, code, constructor=Transformer._make)
        logger.info('更新迁移(%s)', code)
        logger.debug('transformer=%s', transformer)


def transformer_delete(code: str):
    """
    删除迁移。
    """
    logger.debug('code=%s', code)
    ensure(transformer_exists(code), f'迁移({code})不存在')
    with metadata.connect() as meta:
        transformer = meta.record('''
          delete from transformers where code = ? returning *
        ''', code, constructor=Transformer._make)
        logger.info('删除迁移(%s)', code)
        logger.debug('transformer=%s', transformer)


def transformer_list(
        source_repository_kind: Optional[str] = None,
        source_repository_code: Optional[str] = None,
        target_repository_kind: Optional[str] = None,
        target_repository_code: Optional[str] = None,
        target_storage_code: Optional[str] = None,
) -> list[TransformerProjection]:
    """
    列出所有迁移。
    """
    logger.debug(
        'source_repository_kind=%s, source_repository_code=%s, target_repository_kind=%s, target_repository_code=%s, target_storage_code=%s',
        source_repository_kind, source_repository_code,
        target_repository_kind, target_repository_code,
        target_storage_code,
    )

    filters = []
    parameters = []
    if source_repository_kind is not None:
        RepositoryKind.ensure(source_repository_kind)
        filters.append('and sources.kind = ?')
        parameters.append(source_repository_kind)
    if source_repository_code is not None:
        filters.append('and sources.code = ?')
        parameters.append(source_repository_code)
    if target_repository_kind is not None:
        RepositoryKind.ensure(target_repository_kind)
        filters.append('and targets.kind = ?')
        parameters.append(target_repository_kind)
    if target_repository_code is not None:
        filters.append('and targets.code = ?')
        parameters.append(target_repository_code)
    if target_storage_code is not None:
        filters.append('and storages.code = ?')
        parameters.append(target_storage_code)

    with metadata.connect() as meta:
        return meta.records(f'''
          with transformers_tasks as (
            select
              transformer_id as id,
              count(*) as tasks
            from
              tasks_transformers
            group by
              transformer_id
          )
          select
            transformers.code,
            sources.kind as source_repository_kind,
            sources.code as source_repository_code,
            targets.kind as target_repository_kind,
            targets.code as target_repository_code,
            storages.code as target_storage_code,
            transformers.script_file,
            coalesce(transformers_tasks.tasks, 0) as tasks
          from
            transformers
          inner join
            repositories as sources
          on
            transformers.source_id = sources.id
          inner join
            storages
          on
            transformers.target_id = storages.id
          inner join
            repositories as targets
          on
            storages.repository_id = targets.id
            {' '.join(filters)}
          left join
            transformers_tasks
          on
            transformers.id = transformers_tasks.id
          order by
            transformers.code
        ''', *parameters, constructor=TransformerProjection._make)


# 执行迁移

def transformer_execute(code: str):
    """
    执行迁移。
    """
    logger.debug('code=%s', code)
    transformer = transformer_find(code)
    ensure(transformer is not None, f'迁移({code})不存在')
    ensure(exists(transformer.script_file), f'迁移脚本({transformer.script_file})不存在')
    sql = slurp(transformer.script_file)

    with metadata.connect() as meta:
        context = meta.record('''
          select
            sources.code as source_repository_code,
            targets.code as target_repository_code,
            storages.code as target_storage_code
          from
            transformers
          inner join
            repositories as sources
          on
            transformers.source_id = sources.id
            and transformers.id = ?
          inner join
            storages
          on
            transformers.target_id = storages.id
          inner join
            repositories as targets
          on
            storages.repository_id = targets.id
        ''', transformer.id, constructor=TransformContext._make)
        source_repository = repository_service.repository_find(context.source_repository_code)
        target_repository = repository_service.repository_find(context.target_repository_code)
        target_storage = storage_service.storage_find(context.target_repository_code, context.target_storage_code)
        kind = RepositoryKind.of(target_repository.kind)

        with source_repository.connect() as source_connection:
            with source_connection.prepare(sql) as statement:
                kind.transform(statement, target_repository, target_storage)
                logger.info('从仓库(%s)迁移数据到仓库(%s)的存储单元(%s)', source_repository.code, target_repository.code, target_storage.code)
