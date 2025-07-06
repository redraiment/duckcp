import logging
import sys
from logging import DEBUG

from duckcp.boot import app, meta_command, repository_command, storage_command, transformer_command, task_command

logger = logging.getLogger(__name__)


def main():
    try:
        app()
    except Exception as e:
        if logger.isEnabledFor(DEBUG):
            logger.exception(e)
        else:
            logger.error('%s', e)
        sys.exit(1)
