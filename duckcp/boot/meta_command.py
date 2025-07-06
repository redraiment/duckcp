import logging

import click

from duckcp.boot import app
from duckcp.service import meta_service

logger = logging.getLogger(__name__)


@app.group(help='管理元信息数据库')
@click.help_option('-h', '--help', help='展示帮助信息')
def meta():
    pass


@meta.command('delete', help='删除元信息数据库')
@click.help_option('-h', '--help', help='展示帮助信息')
def meta_delete():
    meta_service.meta_delete()


@meta.command('create', help='创建元信息数据库')
@click.option('-f', '--force', is_flag=True, help='强制重建')
@click.help_option('-h', '--help', help='展示帮助信息')
def meta_create(force: bool):
    if force:
        meta_service.meta_delete()
    meta_service.meta_create()
