"""
元信息数据库管理服务。
"""
import logging
from importlib.resources import path
from os import unlink, chmod
from os.path import exists

from duckcp import migration
from duckcp.configuration import meta_configuration as metadata, Configuration

logger = logging.getLogger(__name__)


def meta_create():
    """
    创建元信息数据库。
    """
    if not exists(Configuration.file):
        logger.info('配置文件(%s)初始化', Configuration.file)
        with metadata.connect() as meta:
            with path(migration) as folder:
                for script in sorted(folder.glob('**/*.sql')):
                    logger.info('执行脚本(%s)', script.name)
                    meta.execute(script.read_text())
        chmod(Configuration.file, 0o600)  # 配置文件里包含部分敏感信息，因此只允许当前用户访问
    else:
        logger.warning('配置文件(%s)已存在', Configuration.file)


def meta_delete():
    """
    删除元信息数据库。
    """
    if exists(Configuration.file):
        logger.info('删除配置文件(%s)', Configuration.file)
        unlink(Configuration.file)
    else:
        logger.info('配置文件(%s)不存在；忽略删除操作', Configuration.file)
