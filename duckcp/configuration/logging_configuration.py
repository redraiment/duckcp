from logging.config import dictConfig
from os import makedirs
from os.path import exists, dirname

import click
from rich.text import Text


def highlighter(text: Text) -> Text:
    """
    自定义的高亮：根据日志级别展示不同的色彩。
    """
    fragments = text.plain.split(' ', 5)
    if len(fragments) == 6:
        (date, time, level, fn, separator, message) = fragments
        if level == 'DEBUG':
            style = 'dim black'
        elif level == 'INFO':
            style = 'cyan'
        elif level == 'WARNING':
            style = 'yellow'
        elif level == 'ERROR':
            style = 'bold red'
        elif level == 'CRITICAL':
            style = 'red on yellow'
        else:
            style = 'black'
        record = f'[dim][cyan]{date} {time}[/cyan] [{style}]{level}[/{style}] {fn} {separator}[/dim] [{style}]{message}[/{style}]'
        return Text.from_markup(record)
    else:
        return text


def enable_logging_configuration(
        file: str | None,
        levels: list[tuple[str, str]],
        message_only: bool,
        verbose: bool,
        quiet: bool
):
    """
    日志输出配置。
    """
    default_level = 'DEBUG' if verbose else 'INFO'
    formatter = 'message' if message_only else 'base'

    if quiet:
        handler = {
            'class': 'logging.NullHandler'
        }
    elif file is None or file == '':  # 不输出文件，则默认输出到控制台
        handler = {
            'class': 'rich.logging.RichHandler',
            'level': default_level,
            'formatter': formatter,
            'highlighter': highlighter,
            'show_time': False,
            'show_level': False,
            'rich_tracebacks': True,
            'tracebacks_suppress': [click],
        }
    else:
        folder = dirname(file)
        if not exists(folder):
            makedirs(folder)
        handler = {
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'level': default_level,
            'formatter': formatter,
            'filename': file,
            'when': 'D',
            'backupCount': 30,
        }

    dictConfig({
        'version': 1,
        'formatters': {
            'message': {
                'format': '%(message)s',
                'datefmt': '%Y-%m-%d %H:%M:%S',
            },
            'base': {
                'format': '%(asctime)s.%(msecs)03d %(levelname)s %(name)s#%(funcName)s : %(message)s',
                'datefmt': '%Y-%m-%d %H:%M:%S',
            },
        },
        'handlers': {
            'handler': handler,
        },
        'root': {
            'level': default_level,
            'propagate': False,
            'handlers': ['handler'],
        },
        'loggers': {
            'duckcp': {
                'level': default_level,
                'propagate': False,
                'handlers': ['handler'],
            },
            '__main__': {
                'level': default_level,
                'propagate': False,
                'handlers': ['handler'],
            },
            **{name: {
                'level': level.upper(),
                'propagate': False,
                'handlers': ['handler'],
            } for name, level in levels}
        },
    })
