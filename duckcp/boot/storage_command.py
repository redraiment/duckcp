import logging
from typing import Any

from click import help_option, option, argument, INT, Choice
from rich.console import Console
from rich.table import Table

from duckcp.boot import app
from duckcp.helper.click import JSON
from duckcp.repository import RepositoryKind
from duckcp.service import storage_service

logger = logging.getLogger(__name__)


@app.group(help='管理存储单元')
@help_option('-h', '--help', help='展示帮助信息')
def storage():
    pass


@storage.command('create', help='创建存储单元')
@argument('name', metavar='NAME')
@option('-r', '--repository', metavar='REPOSITORY', help='所属仓库')
# Postgres；DuckDB；ODPS
@option('--catalog', metavar='CATALOG', help='目录；用于[postgres；duckdb；odps]')
# Postgres；DuckDB；ODPS；SQLite
@option('--schema', metavar='SCHEMA', help='模式；用于[postgres；duckdb；odps；sqlite]')
# Postgres；DuckDB；ODPS；SQLite；BiTable
@option('--table', metavar='TABLE', help='表；用于[postgres；duckdb；odps；sqlite；bitable]')
# BiTable
@option('--document', metavar='DOCUMENT', help='多维表格文档；用于[bitable]')
# File
@option('--file', metavar='FILE', help='文件名；用于[file]')
@option('--format', metavar='FORMAT', type=Choice(['csv', 'parquet', 'json']), help='文件格式；用于[file]')
@option('--compression', metavar='ALGORITHM', type=Choice(['gzip', 'zstd', 'snappy', 'brotli', 'lz4', 'lz4_raw']), help='压缩算法；用于[file]')
@option('--compression-level', metavar='LEVEL', type=INT, help='压缩等级；用于[file][parquet]')
@option('--parquet-version', metavar='VERSION', type=Choice(['V1', 'V2']), help='Parquet文件版本；用于[file][parquet]')
@option('--field-ids', metavar='JSON', type=JSON, help='字段的编号；用于[file][parquet]')
@option('--row-group-size', metavar='SIZE', type=INT, help='最大行数；用于[file][parquet]')
@option('--row-group-size-bytes', metavar='SIZE', type=INT, help='每行组最大字节数；用于[file][parquet]')
@option('--row-group-per-file', metavar='SIZE', type=INT, help='每个文件最大的行组数；用于[file][parquet]')
@option('--header/--no-header', is_flag=True, default=None, help='是否包含字段名；用于[file][csv]')
@option('--delimiter', metavar='DELIMITER', help='字段分隔符；用于[file][csv]')
@option('--quote-char', metavar='CHAR', help='引号字符；用于[file][csv]')
@option('--escape-char', metavar='CHAR', help='转义字符；用于[file][csv]')
@option('--null-literal', metavar='LITERAL', help='空值字面量；用于[file][csv]')
@option('--force-quote', metavar='COLUMN', multiple=True, help='给指定列强制添加引号；用于[file][csv]')
@option('--prefix', metavar='PREFIX', help='前置内容；用于[file][csv]')
@option('--suffix', metavar='SUFFIX', help='后置内容；用于[file][csv]')
@option('--date-format', metavar='FORMAT', help='日期格式；用于[file][csv；json]')
@option('--timestamp-format', metavar='FORMAT', help='时间戳格式；用于[file][csv；json]')
@option('--array/--no-array', is_flag=True, default=None, help='写入JSON数组；用于[file][json]')
@option('--per-thread-output', is_flag=True, default=None, help='是否每个线程写入独立文件；用于[file][thread]')
@option('--file-size-bytes', metavar='BYTES', type=INT, help='每个独立文件最大字节数；用于[file][thread]')
@option('--partition-by', metavar='COLUMN', multiple=True, help='分区列；用于[file][partition]')
@option('--filename-pattern', metavar='PATTERN', help='文件名模式；用于[file][partition]')
@option('--file-extension', metavar='EXTENSION', help='扩展名；用于[file][partition]')
@option('--write-partition-columns', is_flag=True, default=None, help='是否包含分区列；用于[file][partition]')
@option('--use-tmp-file', is_flag=True, default=None, help='若原始文件存在，是否先写入临时文件；用于[file][partition]')
@option('--delete-before-write', is_flag=True, default=None, help='写入之前删除整个目录；用于[file][partition]')
@option('--overwrite/--no-overwrite', is_flag=True, default=None, help='覆盖已存在的文件；用于[file][partition]')
@option('--append/--no-append', is_flag=True, default=None, help='追加到已存在的文件；用于[file][partition]')
@option('--preserve-order', is_flag=True, default=None, help='是否保留原始顺序；用于[file]')
# Others
@help_option('-h', '--help', help='展示帮助信息')
def storage_create(
        name: str,
        repository: str,
        # Postgres；DuckDB；ODPS
        catalog: str,  # 目录；用于[postgres；duckdb；odps]
        # Postgres；DuckDB；ODPS；SQLite
        schema: str,  # 模式；用于[postgres；duckdb；odps；sqlite]
        # Postgres；DuckDB；ODPS；SQLite；BiTable
        table: str,  # 表；用于[postgres；duckdb；odps；sqlite；bitable]
        # BiTable
        document: str,  # 多维表格文档；用于[bitable]
        # File
        file: str,  # 文件名；用于[file]
        format: str,  # 文件格式；用于[file]
        compression: str,  # 压缩算法；用于[file]
        compression_level: int,  # 压缩等级；用于[file][parquet]
        parquet_version: str,  # Parquet文件版本；用于[file][parquet]
        field_ids: dict[str, Any],  # 字段的编号；用于[file][parquet]
        row_group_size: int,  # 最大行数；用于[file][parquet]
        row_group_size_bytes: int,  # 每行组最大字节数；用于[file][parquet]
        row_group_per_file: int,  # 每个文件最大的行组数；用于[file][parquet]
        header: bool,  # 是否包含字段名；用于[file][csv]
        delimiter: str,  # 字段分隔符；用于[file][csv]
        quote_char: str,  # 引号字符；用于[file][csv]
        escape_char: str,  # 转义字符；用于[file][csv]
        null_literal: str,  # 空值字面量；用于[file][csv]
        force_quote: list[str],  # 给指定列强制添加引号；用于[file][csv]
        prefix: str,  # 前置内容；用于[file][csv]
        suffix: str,  # 后置内容；用于[file][csv]
        date_format: str,  # 日期格式；用于[file][csv；json]
        timestamp_format: str,  # 时间戳格式；用于[file][csv；json]
        array: bool,  # 写入JSON数组；用于[file][json]
        per_thread_output: bool,  # 是否每个线程写入独立文件；用于[file][thread]
        file_size_bytes: int,  # 每个独立文件最大字节数；用于[file][thread]
        partition_by: list[str],  # 分区列；用于[file][partition]
        filename_pattern: str,  # 文件名模式；用于[file][partition]
        file_extension: str,  # 扩展名；用于[file][partition]
        write_partition_columns: bool,  # 是否包含分区列；用于[file][partition]
        use_tmp_file: bool,  # 若原始文件存在，是否先写入临时文件；用于[file][partition]
        delete_before_write: bool,  # 写入之前删除整个目录；用于[file][partition]
        overwrite: bool,  # 覆盖已存在的文件；用于[file][partition]
        append: bool,  # 追加到已存在的文件；用于[file][partition]
        preserve_order: bool,  # 是否保留原始顺序；用于[file]
):
    logger.debug(
        'name=%s, repository=%s, catalog=%s, schema=%s, table=%s, document=%s, file=%s, format=%s, compression=%s, compression_level=%s, parquet_version=%s, field_ids=%s, row_group_size=%s, row_group_size_bytes=%s, row_group_per_file=%s, header=%s, delimiter=%s, quote_char=%s, escape_char=%s, null_literal=%s, force_quote=%s, prefix=%s, suffix=%s, date_format=%s, timestamp_format=%s, array=%s, per_thread_output=%s, file_size_bytes=%s, partition_by=%s, filename_pattern=%s, file_extension=%s, write_partition_columns=%s, use_tmp_file=%s, delete_before_write=%s, overwrite=%s, append=%s, preserve_order=%s',
        name, repository, catalog, schema, table, document,
        file, format, compression, compression_level,
        parquet_version, field_ids, row_group_size, row_group_size_bytes, row_group_per_file,
        header, delimiter, quote_char, escape_char, null_literal, force_quote, prefix, suffix,
        date_format, timestamp_format, array,
        per_thread_output, file_size_bytes,
        partition_by, filename_pattern, file_extension, write_partition_columns, use_tmp_file,
        delete_before_write, overwrite, append, preserve_order,
    )
    storage_service.storage_create(repository, name, {
        'catalog': catalog,
        'schema': schema,
        'table': table,
        'document': document,
        'file': file,
        'format': format,
        'compression': compression,
        'compression_level': compression_level,
        'parquet_version': parquet_version,
        'field_ids': field_ids,
        'row_group_size': row_group_size,
        'row_group_size_bytes': row_group_size_bytes,
        'row_group_per_file': row_group_per_file,
        'header': header,
        'delimiter': delimiter,
        'quote': quote_char,
        'escape': escape_char,
        'nullstr': null_literal,
        'force_quote': force_quote,
        'prefix': prefix,
        'suffix': suffix,
        'dateformat': date_format,
        'timestampformat': timestamp_format,
        'array': array,
        'per_thread_output': per_thread_output,
        'file_size_bytes': file_size_bytes,
        'partition_by': partition_by,
        'filename_pattern': filename_pattern,
        'file_extension': file_extension,
        'write_partition_columns': write_partition_columns,
        'use_tmp_file': use_tmp_file,
        'overwrite': delete_before_write,
        'overwrite_or_ignore': overwrite,
        'append': append,
        'preserve_order': preserve_order,
    })


@storage.command('update', help='更新存储单元信息')
@argument('name', metavar='NAME')
@option('-r', '--repository', metavar='REPOSITORY', help='所属仓库')
# Postgres；DuckDB；ODPS
@option('--catalog', metavar='CATALOG', help='目录；用于[postgres；duckdb；odps]')
# Postgres；DuckDB；ODPS；SQLite
@option('--schema', metavar='SCHEMA', help='模式；用于[postgres；duckdb；odps；sqlite]')
# Postgres；DuckDB；ODPS；SQLite；BiTable
@option('--table', metavar='TABLE', help='表；用于[postgres；duckdb；odps；sqlite；bitable]')
# BiTable
@option('--document', metavar='DOCUMENT', help='多维表格文档；用于[bitable]')
# File
@option('--file', metavar='FILE', help='文件名；用于[file]')
@option('--format', metavar='FORMAT', type=Choice(['csv', 'parquet', 'json']), help='文件格式；用于[file]')
@option('--compression', metavar='ALGORITHM', type=Choice(['gzip', 'zstd', 'snappy', 'brotli', 'lz4', 'lz4_raw']), help='压缩算法；用于[file]')
@option('--compression-level', metavar='LEVEL', type=INT, help='压缩等级；用于[file][parquet]')
@option('--parquet-version', metavar='VERSION', type=Choice(['V1', 'V2']), help='Parquet文件版本；用于[file][parquet]')
@option('--field-ids', metavar='JSON', type=JSON, help='字段的编号；用于[file][parquet]')
@option('--row-group-size', metavar='SIZE', type=INT, help='最大行数；用于[file][parquet]')
@option('--row-group-size-bytes', metavar='SIZE', type=INT, help='每行组最大字节数；用于[file][parquet]')
@option('--row-group-per-file', metavar='SIZE', type=INT, help='每个文件最大的行组数；用于[file][parquet]')
@option('--header/--no-header', is_flag=True, default=None, help='是否包含字段名；用于[file][csv]')
@option('--delimiter', metavar='DELIMITER', help='字段分隔符；用于[file][csv]')
@option('--quote-char', metavar='CHAR', help='引号字符；用于[file][csv]')
@option('--escape-char', metavar='CHAR', help='转义字符；用于[file][csv]')
@option('--null-literal', metavar='LITERAL', help='空值字面量；用于[file][csv]')
@option('--force-quote', metavar='COLUMN', multiple=True, help='给指定列强制添加引号；用于[file][csv]')
@option('--prefix', metavar='PREFIX', help='前置内容；用于[file][csv]')
@option('--suffix', metavar='SUFFIX', help='后置内容；用于[file][csv]')
@option('--date-format', metavar='FORMAT', help='日期格式；用于[file][csv；json]')
@option('--timestamp-format', metavar='FORMAT', help='时间戳格式；用于[file][csv；json]')
@option('--array/--no-array', is_flag=True, default=None, help='写入JSON数组；用于[file][json]')
@option('--per-thread-output', is_flag=True, default=None, help='是否每个线程写入独立文件；用于[file][thread]')
@option('--file-size-bytes', metavar='BYTES', type=INT, help='每个独立文件最大字节数；用于[file][thread]')
@option('--partition-by', metavar='COLUMN', multiple=True, help='分区列；用于[file][partition]')
@option('--filename-pattern', metavar='PATTERN', help='文件名模式；用于[file][partition]')
@option('--file-extension', metavar='EXTENSION', help='扩展名；用于[file][partition]')
@option('--write-partition-columns', is_flag=True, default=None, help='是否包含分区列；用于[file][partition]')
@option('--use-tmp-file', is_flag=True, default=None, help='若原始文件存在，是否先写入临时文件；用于[file][partition]')
@option('--delete-before-write', is_flag=True, default=None, help='写入之前删除整个目录；用于[file][partition]')
@option('--overwrite/--no-overwrite', is_flag=True, default=None, help='覆盖已存在的文件；用于[file][partition]')
@option('--append/--no-append', is_flag=True, default=None, help='追加到已存在的文件；用于[file][partition]')
@option('--preserve-order', is_flag=True, default=None, help='是否保留原始顺序；用于[file]')
# Others
@help_option('-h', '--help', help='展示帮助信息')
def storage_update(
        name: str,
        repository: str,
        # Postgres；DuckDB；ODPS
        catalog: str,  # 目录；用于[postgres；duckdb；odps]
        # Postgres；DuckDB；ODPS；SQLite
        schema: str,  # 模式；用于[postgres；duckdb；odps；sqlite]
        # Postgres；DuckDB；ODPS；SQLite；BiTable
        table: str,  # 表；用于[postgres；duckdb；odps；sqlite；bitable]
        # BiTable
        document: str,  # 多维表格文档；用于[bitable]
        # File
        file: str,  # 文件名；用于[file]
        format: str,  # 文件格式；用于[file]
        compression: str,  # 压缩算法；用于[file]
        compression_level: int,  # 压缩等级；用于[file][parquet]
        parquet_version: str,  # Parquet文件版本；用于[file][parquet]
        field_ids: dict[str, Any],  # 字段的编号；用于[file][parquet]
        row_group_size: int,  # 最大行数；用于[file][parquet]
        row_group_size_bytes: int,  # 每行组最大字节数；用于[file][parquet]
        row_group_per_file: int,  # 每个文件最大的行组数；用于[file][parquet]
        header: bool,  # 是否包含字段名；用于[file][csv]
        delimiter: str,  # 字段分隔符；用于[file][csv]
        quote_char: str,  # 引号字符；用于[file][csv]
        escape_char: str,  # 转义字符；用于[file][csv]
        null_literal: str,  # 空值字面量；用于[file][csv]
        force_quote: list[str],  # 给指定列强制添加引号；用于[file][csv]
        prefix: str,  # 前置内容；用于[file][csv]
        suffix: str,  # 后置内容；用于[file][csv]
        date_format: str,  # 日期格式；用于[file][csv；json]
        timestamp_format: str,  # 时间戳格式；用于[file][csv；json]
        array: bool,  # 写入JSON数组；用于[file][json]
        per_thread_output: bool,  # 是否每个线程写入独立文件；用于[file][thread]
        file_size_bytes: int,  # 每个独立文件最大字节数；用于[file][thread]
        partition_by: list[str],  # 分区列；用于[file][partition]
        filename_pattern: str,  # 文件名模式；用于[file][partition]
        file_extension: str,  # 扩展名；用于[file][partition]
        write_partition_columns: bool,  # 是否包含分区列；用于[file][partition]
        use_tmp_file: bool,  # 若原始文件存在，是否先写入临时文件；用于[file][partition]
        delete_before_write: bool,  # 写入之前删除整个目录；用于[file][partition]
        overwrite: bool,  # 覆盖已存在的文件；用于[file][partition]
        append: bool,  # 追加到已存在的文件；用于[file][partition]
        preserve_order: bool,  # 是否保留原始顺序；用于[file]
):
    logger.debug(
        'name=%s, repository=%s, catalog=%s, schema=%s, table=%s, document=%s, file=%s, format=%s, compression=%s, compression_level=%s, parquet_version=%s, field_ids=%s, row_group_size=%s, row_group_size_bytes=%s, row_group_per_file=%s, header=%s, delimiter=%s, quote_char=%s, escape_char=%s, null_literal=%s, force_quote=%s, prefix=%s, suffix=%s, date_format=%s, timestamp_format=%s, array=%s, per_thread_output=%s, file_size_bytes=%s, partition_by=%s, filename_pattern=%s, file_extension=%s, write_partition_columns=%s, use_tmp_file=%s, delete_before_write=%s, overwrite=%s, append=%s, preserve_order=%s',
        name, repository, catalog, schema, table, document,
        file, format, compression, compression_level,
        parquet_version, field_ids, row_group_size, row_group_size_bytes, row_group_per_file,
        header, delimiter, quote_char, escape_char, null_literal, force_quote, prefix, suffix,
        date_format, timestamp_format, array,
        per_thread_output, file_size_bytes,
        partition_by, filename_pattern, file_extension, write_partition_columns, use_tmp_file,
        delete_before_write, overwrite, append, preserve_order,
    )
    storage_service.storage_update(repository, name, {
        'catalog': catalog,
        'schema': schema,
        'table': table,
        'document': document,
        'file': file,
        'format': format,
        'compression': compression,
        'compression_level': compression_level,
        'parquet_version': parquet_version,
        'field_ids': field_ids,
        'row_group_size': row_group_size,
        'row_group_size_bytes': row_group_size_bytes,
        'row_group_per_file': row_group_per_file,
        'header': header,
        'delimiter': delimiter,
        'quote': quote_char,
        'escape': escape_char,
        'nullstr': null_literal,
        'force_quote': force_quote,
        'prefix': prefix,
        'suffix': suffix,
        'dateformat': date_format,
        'timestampformat': timestamp_format,
        'array': array,
        'per_thread_output': per_thread_output,
        'file_size_bytes': file_size_bytes,
        'partition_by': partition_by,
        'filename_pattern': filename_pattern,
        'file_extension': file_extension,
        'write_partition_columns': write_partition_columns,
        'use_tmp_file': use_tmp_file,
        'overwrite': delete_before_write,
        'overwrite_or_ignore': overwrite,
        'append': append,
        'preserve_order': preserve_order,
    })


@storage.command('delete', help='删除存储单元；删除迁移')
@argument('name', metavar='NAME')
@option('-r', '--repository', metavar='REPOSITORY', help='所属仓库')
@help_option('-h', '--help', help='展示帮助信息')
def storage_delete(name: str, repository: str):
    logger.debug('name=%s, repository=%s', name, repository)
    storage_service.storage_delete(repository, name)


@storage.command('list', help='列出所有存储单元')
@option('-k', '--kind', type=Choice(RepositoryKind.codes()), help='仓库类型')
@option('-r', '--repository', metavar='REPOSITORY', help='所属仓库')
@help_option('-h', '--help', help='展示帮助信息')
def storage_list(kind: str, repository: str):
    logger.debug('kind=%s, repository=%s', kind, repository)
    table = Table(title='存储单元列表')
    table.add_column('仓库类型', no_wrap=True)
    table.add_column('仓库名称', no_wrap=True)
    table.add_column('存储单元', no_wrap=True)
    table.add_column('关联迁移', justify='right')
    for row in storage_service.storage_list(kind, repository):
        table.add_row(
            row.repository_kind,
            row.repository_code,
            row.code,
            str(row.transformers),
        )

    console = Console()
    console.print(table)
