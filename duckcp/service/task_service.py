"""
定时任务调度服务。
"""
import logging
from typing import Optional

from duckcp.configuration import meta_configuration as metadata
from duckcp.entity.task import Task
from duckcp.entity.task_transformer import TaskTransformer
from duckcp.helper.validation import ensure
from duckcp.projection.task_projection import TaskProjection
from duckcp.projection.task_transformer_projection import TaskTransformerProjection
from duckcp.service import transformer_service

logger = logging.getLogger(__name__)


def task_find(code: str) -> Optional[Task]:
    """
    根据编码查找任务。
    """
    with metadata.connect() as meta:
        return meta.record('select * from tasks where code = ?', code, constructor=Task._make)


def task_exists(code: str) -> bool:
    """
    判断编码对应的任务是否已存在。
    """
    return task_find(code) is not None


def task_transformer_find(task_id: int, transformer_id: int) -> TaskTransformer:
    """
    判断任务与迁移是否已绑定。
    """
    with metadata.connect() as meta:
        return meta.record('select * from tasks_transformers where task_id = ? and transformer_id = ?', task_id, transformer_id, constructor=TaskTransformer._make)


def task_transformers(task_id: int) -> int:
    """
    任务关联的迁移数。
    """
    with metadata.connect() as meta:
        return meta.value('select count(*) as transformers from tasks_transformers where task_id = ?', task_id)


def task_create(code: str):
    """
    创建任务。
    """
    logger.debug('code=%s', code)
    ensure(code is not None, '缺少任务名称')
    ensure(not task_exists(code), f'任务({code})已存在')
    with metadata.connect() as meta:
        task = meta.record('''
          insert into tasks (code) values (?) returning *
        ''', code, constructor=Task._make)
        logger.info('创建任务(%s)', code)
        logger.debug('task=%s', task)


def task_delete(code: str):
    """
    删除任务。
    """
    logger.debug('code=%s', code)
    ensure(code is not None, '缺少任务名称')
    ensure(task_exists(code), f'任务({code})不存在')
    with metadata.connect() as meta:
        task = meta.record('''
          delete from tasks where code = ? returning *
        ''', code, constructor=Task._make)
        logger.info('删除任务(%s)', code)
        logger.debug('task=%s', task)


def task_list() -> list[TaskProjection]:
    """
    罗列所有任务。
    """
    with metadata.connect() as meta:
        return meta.records('''
          with task_transformers as (
            select
              task_id,
              count(*) as transformers
            from
              tasks_transformers
            group by
              task_id
          )
          select
            tasks.code,
            coalesce(task_transformers.transformers, 0) as transformers
          from
            tasks
          left join
            task_transformers
          on
            tasks.id = task_transformers.task_id
          group by
            tasks.code
          order by
            tasks.code
        ''', constructor=TaskProjection._make)


def task_execute(code: str):
    """
    执行迁移任务。
    """
    logger.debug('code=%s', code)
    ensure(task_exists(code), f'任务({code})不存在')
    with metadata.connect() as meta:
        for transformer_code in meta.values('''
          select
            transformers.code
          from
            tasks
          inner join
            tasks_transformers
          on
            tasks.id = tasks_transformers.task_id
            and tasks.code = ?
          inner join
            transformers
          on
            tasks_transformers.transformer_id = transformers.id
          order by
            tasks_transformers.sort
        ''', code):
            transformer_service.transformer_execute(transformer_code)


def task_bind(code: str, transformer_code: str, sort: int):
    """
    迁移与任务绑定。
    """
    logger.debug('code=%s, transformer_code=%s', code, transformer_code)
    task = task_find(code)
    ensure(task is not None, f'任务({code})不存在')
    transformer = transformer_service.transformer_find(transformer_code)
    ensure(transformer is not None, f'迁移({transformer_code})不存在')
    ensure(task_transformer_find(task.id, transformer.id) is None, f'任务({code})与迁移({transformer_code})已绑定')
    with metadata.connect() as meta:
        transformers = task_transformers(task.id)
        if sort is not None and 0 < sort <= transformers:
            meta.execute('''
              update
                tasks_transformers
              set
                sort = sort + 1,
                updated_at = datetime(current_timestamp, 'localtime')
              where
                task_id = ?
                and sort >= ?
            ''', task.id, sort)
            logger.info('后移序号(%s-%s)的迁移', sort, transformers)
        else:
            sort = transformers + 1
        task_transformer = meta.record('''
          insert into tasks_transformers
            (task_id, transformer_id, sort)
          values
            (?, ?, ?)
          returning *
        ''', task.id, transformer.id, sort)
        logger.info('绑定任务(%s)与迁移(%s)', code, transformer_code)
        logger.debug('task_transformer=%s', task_transformer)


def task_unbind(code: str, transformer_code: str):
    """
    迁移与任务解绑。
    """
    logger.debug('code=%s, transformer_code=%s', code, transformer_code)
    task = task_find(code)
    ensure(task is not None, f'任务({code})不存在')
    transformer = transformer_service.transformer_find(transformer_code)
    ensure(transformer is not None, f'迁移({transformer_code})不存在')
    task_transformer = task_transformer_find(task.id, transformer.id)
    ensure(task_transformer is not None, f'任务({code})与迁移({transformer_code})未绑定')
    with metadata.connect() as meta:
        transformers = task_transformers(task.id)
        if task_transformer.sort < transformers:
            meta.execute('''
              update
                tasks_transformers
              set
                sort = sort - 1,
                updated_at = datetime(current_timestamp, 'localtime')
              where
                task_id = ?
                and sort > ?
            ''', task.id, task_transformer.sort)
            logger.info('前移序号(%s-%s)的迁移', task_transformer.sort + 1, transformers)
        task_transformer = meta.record('''
          delete from tasks_transformers where task_id = ? and transformer_id = ? returning *
        ''', task.id, transformer.id)
        logger.info('解绑任务(%s)与迁移(%s)', code, transformer_code)
        logger.debug('task_transformer=%s', task_transformer)


def task_transformer_list(code: str) -> list[TaskTransformerProjection]:
    """
    列出任务内所有迁移。
    """
    logger.debug('code=%s', code)
    filters = []
    parameters = []
    if code is not None:
        ensure(task_exists(code), f'任务({code})不存在')
        filters.append('and tasks.code = ?')
        parameters.append(code)

    with metadata.connect() as meta:
        return meta.records(f'''
          select
            tasks.code as task_code,
            tasks_transformers.sort,
            transformers.code as transformer_code,
            sources.kind as source_repository_kind,
            sources.code as source_repository_code,
            targets.kind as target_repository_kind,
            targets.code as target_repository_code,
            storages.code as target_storage_code,
            transformers.script_file
          from
            tasks
          inner join
            tasks_transformers
          on
            tasks.id = tasks_transformers.task_id
            {' '.join(filters)}
          inner join
            transformers
          on
            tasks_transformers.transformer_id = transformers.id
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
          order by
            tasks.code,
            tasks_transformers.sort
        ''', *parameters, constructor=TaskTransformerProjection._make)
