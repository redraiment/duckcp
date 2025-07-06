import logging

from click import help_option, argument, option, Choice
from rich.console import Console
from rich.table import Table

from duckcp.boot import app
from duckcp.repository import RepositoryKind
from duckcp.service import transformer_service

logger = logging.getLogger(__name__)


@app.group(help='管理迁移')
@help_option('-h', '--help', help='展示帮助信息')
def transformer():
    pass


@transformer.command('create', help='创建迁移')
@argument('name', metavar='NAME')
@option('-s', '--source', metavar='REPOSITORY', required=True, help='来源仓库')
@option('-t', '--target', metavar='REPOSITORY', required=True, help='目标仓库')
@option('-o', '--storage', metavar='STORAGE', required=True, help='目标存储单元')
@option('-f', '--script', metavar='FILE', required=True, help='迁移脚本')
@help_option('-h', '--help', help='展示帮助信息')
def transformer_create(name: str, source: str, target: str, storage: str, script: str):
    logger.debug('name=%s, source=%s, target=%s, storage=%s, script=%s', name, source, target, storage, script)
    transformer_service.transformer_create(name, source, target, storage, script)


@transformer.command('update', help='更新迁移信息')
@argument('name', metavar='NAME')
@option('-s', '--source', metavar='REPOSITORY', help='来源仓库')
@option('-t', '--target', metavar='REPOSITORY', help='目标仓库')
@option('-o', '--storage', metavar='STORAGE', help='目标存储单元')
@option('-f', '--script', metavar='FILE', help='迁移脚本')
@help_option('-h', '--help', help='展示帮助信息')
def transformer_update(name: str, source: str, target: str, storage: str, script: str):
    logger.debug('name=%s, source=%s, target=%s, storage=%s, script=%s', name, source, target, storage, script)
    transformer_service.transformer_update(name, source, target, storage, script)


@transformer.command('delete', help='删除迁移；更新作业')
@argument('name', metavar='NAME')
@help_option('-h', '--help', help='展示帮助信息')
def transformer_delete(name: str):
    logger.debug('name=%s', name)
    transformer_service.transformer_delete(name)


@transformer.command('list', help='列出所有迁移')
@option('--source-kind', type=Choice(RepositoryKind.codes()), help='来源仓库类型')
@option('--source-repository', metavar='REPOSITORY', help='来源仓库名称')
@option('--target-kind', type=Choice(RepositoryKind.codes()), help='目标仓库类型')
@option('--target-repository', metavar='REPOSITORY', help='目标仓库名称')
@option('--target-storage', metavar='STORAGE', help='目标存储单元')
@help_option('-h', '--help', help='展示帮助信息')
def transformer_list(
        source_kind: str,
        source_repository: str,
        target_kind: str,
        target_repository: str,
        target_storage: str,
):
    table = Table(title='迁移列表')
    table.add_column('名称', no_wrap=True)
    table.add_column('来源类型')
    table.add_column('来源仓库')
    table.add_column('目标类型')
    table.add_column('目标仓库')
    table.add_column('存储单元')
    table.add_column('迁移脚本')
    table.add_column('关联任务', justify='right')
    for row in transformer_service.transformer_list(source_kind, source_repository, target_kind, target_repository, target_storage):
        table.add_row(
            row.code,
            row.source_repository_kind,
            row.source_repository_code,
            row.target_repository_kind,
            row.target_repository_code,
            row.target_storage_code,
            row.script_file,
            str(row.tasks),
        )

    console = Console()
    console.print(table)


@transformer.command('execute', help='执行迁移')
@argument('name', metavar='NAME')
@help_option('-h', '--help', help='展示帮助信息')
def transformer_execute(name: str):
    logger.debug('name=%s', name)
    transformer_service.transformer_execute(name)
