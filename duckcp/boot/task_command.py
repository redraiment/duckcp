import logging

from click import help_option, argument, option, INT
from rich.console import Console
from rich.table import Table

from duckcp.boot import app
from duckcp.service import task_service

logger = logging.getLogger(__name__)


@app.group(short_help='管理任务', help='管理任务定时调度的任务')
@help_option('-h', '--help', help='展示帮助信息')
def task():
    pass


@task.command('create', help='创建任务')
@argument('name', metavar='NAME')
@help_option('-h', '--help', help='展示帮助信息')
def task_create(name: str):
    logger.debug('name=%s', name)
    task_service.task_create(name)


@task.command('delete', help='删除任务')
@argument('name', metavar='NAME')
@help_option('-h', '--help', help='展示帮助信息')
def task_delete(name: str):
    logger.debug('name=%s', name)
    task_service.task_delete(name)


@task.command('list', help='列出所有任务')
@help_option('-h', '--help', help='展示帮助信息')
def task_list():
    table = Table(title='任务列表')
    table.add_column('名称', no_wrap=True)
    table.add_column('关联迁移', justify='right')
    for row in task_service.task_list():
        table.add_row(
            row.code,
            str(row.transformers),
        )

    console = Console()
    console.print(table)


@task.command('execute', help='执行任务')
@argument('name', metavar='NAME')
@help_option('-h', '--help', help='展示帮助信息')
def task_execute(name: str):
    logger.debug('name=%s', name)
    task_service.task_execute(name)


@task.command('bind', help='绑定迁移')
@argument('name', metavar='NAME')
@option('-t', '--transformer', metavar='TRANSFORMER', required=True, help='关联迁移')
@option('-i', '--index', metavar='NUMBER', type=INT, help='执行顺序；默认最后')
@help_option('-h', '--help', help='展示帮助信息')
def task_bind(name: str, transformer: str, index: int):
    logger.debug('name=%s, transformer=%s', name, transformer)
    task_service.task_bind(name, transformer, index)


@task.command('unbind', help='解绑迁移')
@argument('name', metavar='NAME')
@option('-t', '--transformer', metavar='TRANSFORMER', required=True, help='关联迁移')
@help_option('-h', '--help', help='展示帮助信息')
def task_unbind(name: str, transformer: str):
    logger.debug('name=%s, transformer=%s', name, transformer)
    task_service.task_unbind(name, transformer)


@task.command('transformers', help='列出所有迁移')
@argument('name', required=False, metavar='NAME')
@help_option('-h', '--help', help='展示帮助信息')
def task_transformer_list(name: str):
    logger.debug('name=%s', name)
    table = Table(title='任务迁移列表')
    if name is None:
        table.add_column('任务名称', no_wrap=True)
    table.add_column('执行顺序', justify='right')
    table.add_column('来源仓库类型')
    table.add_column('来源仓库编码')
    table.add_column('目标仓库类型')
    table.add_column('目标仓库编码')
    table.add_column('目标存储编码')
    table.add_column('迁移脚本')
    for row in task_service.task_transformer_list(name):
        record = [
            str(row.sort),
            row.source_repository_kind,
            row.source_repository_code,
            row.target_repository_kind,
            row.target_repository_code,
            row.target_storage_code,
            row.script_file
        ]
        if name is None:
            record.insert(0, row.task_code)
        table.add_row(*record)

    console = Console()
    console.print(table)
