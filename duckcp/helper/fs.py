"""
文件系统帮助函数。
"""
import logging
from functools import lru_cache
from os import makedirs, unlink, getcwd, chdir
from os.path import abspath, expanduser, expandvars, exists, join, dirname
from platform import system
from shutil import rmtree, move

logger = logging.getLogger(__name__)


class UnsupportedPlatformError(Exception):
    """
    当前操作系统不支持时，抛出本异常
    """

    def __init__(self, platform: str):
        super().__init__(f'不支持操作系统`{platform}`')


# 文件


def move_file(source: str, target: str):
    """
    移动文件。
    """
    if exists(source):
        logger.info('移动文件(%s)至(%s)', source, target)
        move(source, target)


def remove_file(file: str):
    """
    删除已存在的文件。
    """
    if exists(file):
        logger.info('删除文件(%s)', file)
        unlink(file)


def slurp(file: str) -> str:
    """
    读取文件内容。
    """
    with open(file) as f:
        return f.read()


def spit(file: str, content: str):
    """
    写入文件内容。
    """
    with open(file, 'w') as f:
        return f.write(content)


# 路径

class WorkDirectory:
    """
    临时切换当前工作目录。
    """
    origin: str  # 当前工作目录
    directory: str  # 目标工作目录

    def __init__(self, directory: str):
        self.origin = getcwd()
        self.directory = absolute_path(directory)

    def __enter__(self):
        """
        将工作目录临时切换到指定的目录
        """
        chdir(self.directory)

    def __exit__(self, exception_class, exception, tracebacks):
        """
        将工作目录临时切换回原始目录
        """
        chdir(self.origin)


def absolute_path(path: str) -> str:
    """
    将路径转成绝对路径。
    """
    return abspath(expanduser(expandvars(path)))


def ensure_folder(path: str, parent: bool = False) -> str:
    """
    确保指定的文件夹或父级文件夹存在。
    """
    folder = dirname(path) if parent else path
    if not exists(folder):
        logger.info('创建目录(%s)', folder)
        makedirs(folder)
    return path


def remove_folder(folder: str):
    """
    递归地删除目录。
    """
    if exists(folder):
        logger.info('删除目录(%s)', folder)
        rmtree(folder)


# XDG

@lru_cache()
def xdg(folder: str, *path: str) -> str:
    """
    XDG Base Directory Specification。
    """
    file = join(absolute_path(folder), *path)
    parent = dirname(file)
    if not exists(parent):
        makedirs(parent)
    return file


@lru_cache()
def cache(*path: str) -> str:
    """
    基于缓存目录的路径。
    """
    match system():
        case 'Linux':
            folder = f'~/.cache'
        case 'Darwin':
            folder = f'~/Library/Caches'
        case 'Windows':
            folder = f'%LOCALAPPDATA%\\Temp'
        case _:
            raise UnsupportedPlatformError(system())
    return xdg(folder, *path)


@lru_cache()
def config(*path: str) -> str:
    """
    基于配置目录的路径。
    """
    match system():
        case 'Linux':
            folder = f'~/.config'
        case 'Darwin':
            folder = f'~/Library/Application Support'
        case 'Windows':
            folder = f'%LOCALAPPDATA%'
        case _:
            raise UnsupportedPlatformError(system())
    return xdg(folder, *path)
