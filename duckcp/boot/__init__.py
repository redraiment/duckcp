import click
from click import Choice, Tuple

from duckcp.configuration.logging_configuration import enable_logging_configuration
from duckcp.configuration.meta_configuration import enable_metadata_configuration


@click.group(help='数据同步工具')
@click.option('-c', '--config-file', metavar='FILE', help='配置文件')
@click.option('-o', '--logging-file', metavar='FILE', help='日志文件')
@click.option('-l', '--logging', type=Tuple([str, Choice([
    'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL',
], case_sensitive=False)]), metavar='<NAME LEVEL>...', multiple=True, nargs=2, help='日志等级')
@click.option('-m', '--message-only', is_flag=True, help='日志只输出内容')
@click.option('-v', '--verbose', is_flag=True, help='开启详细日志')
@click.option('-q', '--quiet', is_flag=True, help='关闭所有日志')
@click.version_option('v0.1.0', '-V', '--version', help='展示版本信息')
@click.help_option('-h', '--help', help='展示帮助信息')
def app(config_file: str, logging_file: str, logging: list[tuple[str, str]], message_only: bool, verbose: bool, quiet: bool) -> None:
    enable_logging_configuration(logging_file, logging, message_only, not quiet and verbose, quiet)
    enable_metadata_configuration(config_file)
