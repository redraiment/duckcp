import logging
from collections.abc import Sequence, Mapping
from numbers import Number
from typing import Optional, Any

from sqlglot import parse, Expression
from sqlglot.dialects.duckdb import DuckDB
from sqlglot.expressions import With, CTE, Table, Create, Identifier, From, Delete, Insert, Schema, Values, Tuple, Copy, Literal, CopyParameter, Var, Boolean, Struct, Array, Null, PropertyEQ

logger = logging.getLogger(__name__)


def to_expression(instance: Any) -> Expression:
    """
    将Python值转成表达式。
    """
    if instance is None:
        return Null()
    elif isinstance(instance, bool):
        return Boolean(this=instance)
    elif isinstance(instance, str):
        return Literal.string(instance)
    elif isinstance(instance, Number):
        return Literal.number(instance)
    elif isinstance(instance, Sequence):
        return Array(expressions=[to_expression(value) for value in instance])
    elif isinstance(instance, Mapping):
        return Struct(expressions=[PropertyEQ(
            this=Identifier(this=name, quoted=True),
            expression=to_expression(value),
        ) for name, value in instance.items()])
    else:
        raise ValueError(f'未知数据({instance})类型({type(instance)})')


def extract_tables(sql: str) -> set[str]:
    """
    从SQL中提取真正的表名，忽略CTE、子查询等临时的表名。
    """
    logger.debug('sql=%s', sql)
    tables = set()  # 真正的表
    for statement in parse(sql, dialect=DuckDB) or []:
        ignores = set()  # 提取CTE别名，忽略这些名字
        if expression := statement.find(With):
            for cte in expression.find_all(CTE):  # 先依次遍历CTE
                for table in cte.find_all(Table):
                    if table.name not in ignores:
                        tables.add(table.name)
                ignores.add(cte.alias)
        for expression in statement.iter_expressions():
            if not isinstance(expression, With):  # 排除With语句；前面已处理
                for table in expression.find_all(Table):
                    if table.name not in ignores:
                        tables.add(table.name)
    return tables


def create_or_replace_table(
        catalog: Optional[str],
        schema: Optional[str],
        table: str,
        source: str,
) -> Expression:
    """
    创建DuckDB方言的create or replace table语句。
    """
    logger.debug('catalog=%s, schema=%s, table=%s, source=%s', catalog, schema, table, source)
    return Create(
        this=Table(
            this=Identifier(this=table, quoted=True),
            db=Identifier(this=schema, quoted=True) if schema else None,
            catalog=Identifier(this=catalog, quoted=True) if catalog else None),
        kind='table',
        replace=True,
        expression=From(
            this=Table(
                this=Identifier(this=source, quoted=True),
                db=Identifier(this='main', quoted=False),
                catalog=Identifier(this='temp', quoted=False))))


def delete_from(
        catalog: Optional[str],
        schema: Optional[str],
        table: str,
) -> Expression:
    """
    创建通用的删除表所有数据语句。
    """
    logger.debug('catalog=%s, schema=%s, table=%s', catalog, schema, table)
    return Delete(
        this=Table(
            this=Identifier(this=table, quoted=True),
            db=Identifier(this=schema, quoted=True) if schema else None,
            catalog=Identifier(this=catalog, quoted=True) if catalog else None))


def insert_into(
        catalog: Optional[str],
        schema: Optional[str],
        table: str,
        columns: list[str],
) -> Expression:
    """
    创建通用的新增记录语句。
    """
    logger.debug('catalog=%s, schema=%s, table=%s, columns=%s', catalog, schema, table, columns)
    return Insert(
        this=Schema(
            this=Table(
                this=Identifier(this=table, quoted=True),
                db=Identifier(this=schema, quoted=True) if schema else None,
                catalog=Identifier(this=catalog, quoted=True) if catalog else None),
            expressions=[Identifier(this=column, quoted=True) for column in columns]),
        expression=Values(
            expressions=[Tuple(
                expressions=['?'] * len(columns)
            )]))


def copy_to(
        table_name: str,
        file_name: str,
        parameters: dict[str, Any],
) -> Expression:
    """
    创建DuckDB方言的COPY语句。
    """
    return Copy(
        this=Table(
            this=Identifier(this=table_name, quoted=True),
            db=Identifier(this='main', quoted=True),
            catalog=Identifier(this='temp', quoted=True)),
        files=[Literal.string(file_name)],
        params=[
            CopyParameter(this=Var(this=name), expression=to_expression(value))
            for name, value in parameters.items()
        ])
