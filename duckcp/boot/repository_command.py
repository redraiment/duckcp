import logging
from os.path import exists

import click
from click import help_option, option, argument, Choice
from rich.console import Console
from rich.table import Table

from duckcp.boot import app
from duckcp.helper.fs import slurp, absolute_path
from duckcp.helper.validation import ensure, confirm
from duckcp.repository import RepositoryKind
from duckcp.service import repository_service

logger = logging.getLogger(__name__)


@app.group(help='管理仓库')
@help_option('-h', '--help', help='展示帮助信息')
def repository():
    pass


@repository.command('create', help='创建仓库')
@argument('name', metavar='NAME')
@option('-k', '--kind', type=Choice(RepositoryKind.codes()), required=True, help='仓库类型')
# Postgres
@option('--host', metavar='HOST', help='主机；用于[postgres]')
@option('--port', type=click.INT, metavar='PORT', help='端口；用于[postgres]')
@option('--database', metavar='DATABASE', help='数据库名；用于[postgres]')
@option('--username', metavar='USERNAME', help='登入用户；用于[postgres]')
@option('--password', metavar='PASSWORD', help='登入密码；用于[postgres]')
# ODPS
@option('--end-point', metavar='END-POINT', help='地址；用于[odps]')
@option('--project', metavar='PROJECT', help='项目；用于[odps]')
# ODPS；BiTable
@option('--access-key', metavar='KEY', help='凭证编码；用于[odps；bitable]')
@option('--access-secret', metavar='SECRET', help='凭证密钥；用于[odps；bitable]')
# DuckDB; SQLite
@option('--file', metavar='FILE', help='文件；用于[duckdb；sqlite]')
# File
@option('--folder', metavar='FOLDER', help='目录；用于[file]')
# Others
@help_option('-h', '--help', help='展示帮助信息')
def repository_create(
        name: str,
        kind: str,
        # Postgres
        host: str,
        port: int,
        database: str,
        username: str,
        password: str,
        # ODPS; BiTable
        end_point: str,
        project: str,
        access_key: str,
        access_secret: str,
        # DuckDB; SQLite
        file: str,
        # File
        folder: str,
):
    logger.debug(
        'name=%s, kind=%s, host=%s, port=%s, database=%s, username=%s, end_point=%s, project=%s, access_key=%s, file=%s, folder=%s',
        name, kind,
        host, port, database, username,
        end_point, project, access_key,
        file, folder,
    )
    repository_service.repository_create(name, kind, {
        'host': host or None,
        'port': port or None,
        'database': database or None,
        'username': username or None,
        'password': password or None,
        'end_point': end_point or None,
        'project': project or None,
        'access_key': access_key or None,
        'access_secret': access_secret or None,
        'file': absolute_path(file) if file else None,
        'folder': absolute_path(folder) if folder else None,
    })


@repository.command('update', help='更新仓库信息')
@argument('name', metavar='NAME')
@option('-k', '--kind', type=Choice(RepositoryKind.codes()), help='仓库类型')
# Postgres
@option('--host', metavar='HOST', help='主机；用于[postgres]')
@option('--port', type=click.INT, metavar='PORT', help='端口；用于[postgres]')
@option('--database', metavar='DATABASE', help='数据库名；用于[postgres]')
@option('--username', metavar='USERNAME', help='登入用户；用于[postgres]')
@option('--password', metavar='PASSWORD', help='登入密码；用于[postgres]')
# ODPS
@option('--end-point', metavar='END-POINT', help='地址；用于[odps]')
@option('--project', metavar='PROJECT', help='项目；用于[odps]')
# ODPS；BiTable
@option('--access-key', metavar='KEY', help='凭证编码；用于[odps；bitable]')
@option('--access-secret', metavar='SECRET', help='凭证密钥；用于[odps；bitable]')
# DuckDB; SQLite
@option('--file', metavar='FILE', help='文件；用于[duckdb；sqlite]')
# File
@option('--folder', metavar='FOLDER', help='目录；用于[file]')
# Others
@help_option('-h', '--help', help='展示帮助信息')
def repository_update(
        name: str,
        kind: str,
        # Postgres
        host: str,
        port: int,
        database: str,
        username: str,
        password: str,
        # ODPS；BiTable
        end_point: str,
        project: str,
        access_key: str,
        access_secret: str,
        # DuckDB; SQLite
        file: str,
        # File
        folder: str,
):
    logger.debug(
        'name=%s, kind=%s, host=%s, port=%s, database=%s, username=%s, end_point=%s, project=%s, access_key=%s, file=%s, folder=%s',
        name, kind,
        host, port, database, username,
        end_point, project, access_key,
        file, folder,
    )
    repository_service.repository_update(name, kind, {
        'host': host,
        'port': port,
        'database': database,
        'username': username,
        'password': password,
        'end_point': end_point,
        'project': project,
        'access_key': access_key,
        'access_secret': access_secret,
        'file': absolute_path(file) if file else file,
        'folder': absolute_path(folder) if folder else file,
    })


@repository.command('delete', help='注销仓库；删除迁移')
@argument('name', metavar='NAME')
@option('-y', '--yes', is_flag=True, help='自动同意')
@help_option('-h', '--help', help='展示帮助信息')
def repository_delete(name: str, yes: bool):
    logger.debug('name=%s, yes=%s', name, yes)
    transformers = repository_service.repository_transformers(name)
    if transformers > 0 and not yes and not confirm(f'仓库({name})还有{transformers}个关联迁移，确认删除？'):
        return
    tables = repository_service.repository_storages(name)
    if tables > 0 and not yes and not confirm(f'仓库({name})还有{tables}个关联表，确认删除？'):
        return
    repository_service.repository_delete(name)


@repository.command('list', help='列出所有仓库')
@option('-k', '--kind', type=Choice(RepositoryKind.codes()), help='仓库类型')
@help_option('-h', '--help', help='展示帮助信息')
def repository_list(kind: str):
    table = Table(title='仓库列表')
    table.add_column('类型', no_wrap=True)
    table.add_column('名称', no_wrap=True)
    table.add_column('关联存储', justify='right')
    table.add_column('关联迁移', justify='right')
    for row in repository_service.repository_list(kind):
        table.add_row(
            row.kind,
            row.code,
            str(row.storages),
            str(row.transformers),
        )

    console = Console()
    console.print(table)


@repository.command('execute', help='执行脚本')
@argument('name', metavar='NAME')
@option('-f', '--script', metavar='FILE', help='查询数据的SQL脚本文件')
@option('-e', '--expression', metavar='SQL', help='查询数据的SQL')
@help_option('-h', '--help', help='展示帮助信息')
def repository_execute(name: str, script: str, expression: str):
    logger.debug('name=%s, script=%s, expression=%s', name, script, expression)
    if script is None and expression is None:
        raise ValueError('请指定查询SQL的脚本文件或查询语句')
    if script is not None and expression is None:
        ensure(exists(script), f'脚本文件({script})不存在')
        expression = slurp(script)

    table = Table(title=name)
    columns, records = repository_service.repository_execute(name, expression)

    for column in columns:
        table.add_column(column)

    for record in records:
        table.add_row(*map(str, record))

    console = Console()
    console.print(table)
