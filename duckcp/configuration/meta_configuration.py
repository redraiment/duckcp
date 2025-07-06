import logging
from contextlib import contextmanager
from os import makedirs
from os.path import dirname, exists
from typing import Optional, Iterator

from duckcp.configuration import Configuration
from duckcp.constant import IDENTIFIER
from duckcp.entity.executor import Executor
from duckcp.helper.fs import absolute_path, config
from duckcp.repository.sqlite_repository import SqliteRepository

logger = logging.getLogger(__name__)


def enable_metadata_configuration(file: Optional[str] = None):
    """
    元数据配置。
    """
    file = absolute_path(file) if file is not None else config(IDENTIFIER, 'configuration.db')
    folder = dirname(file)
    if not exists(folder):
        logger.info('创建目录(%s)', folder)
        makedirs(folder)
    Configuration.file = file
    logger.debug('元信息文件(%s)', file)


@contextmanager
def connect() -> Iterator[Executor]:
    """
    链接元信息数据库。
    """
    repository = SqliteRepository(properties={'file': Configuration.file})
    with repository.connect() as connection:
        with connection.executor() as executor:
            yield executor
